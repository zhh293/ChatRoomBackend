package com.example.chatroom.mq.consumer;

import com.example.chatroom.cache.message.MessageCacheService;
import com.example.chatroom.cache.session.SessionCacheService;
import com.example.chatroom.common.config.RabbitMQConfig;
import com.example.chatroom.common.constant.RedisKeyConst;
import com.example.chatroom.module.message.domain.entity.ChatMessage;
import com.example.chatroom.module.message.mapper.ChatMessageMapper;
import com.example.chatroom.module.message.mapper.LocalMsgOutboxMapper;
import com.example.chatroom.module.session.mapper.SessionMapper;
import com.example.chatroom.module.websocket.manager.WebSocketSessionManager;
import com.example.chatroom.mq.dto.ChatMessageMQDTO;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 聊天消息 RabbitMQ 消费者
 * 手动 ACK + 自定义线程池异步落库
 *
 * <h3>消费流程</h3>
 * <pre>
 * ① Redis 去重（MSG_IDEM SET NX，TTL 2min）
 * ② 写 chat_message 分片表
 * ③ 更新 session.last_msg_id / last_msg_content / last_msg_at
 * ④ 更新 local_msg_outbox.status=2（已消费落库）
 * ⑤ 遍历会话成员：在线 → WebSocket 推送；离线 → 消息已写入 ZSet 缓冲区，前端上线后凭 last_read_msg_id 自行拉取
 *
 * 失败重试：最多3次，每次指数退避；3次后 NACK 进死信队列
 * </pre>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ChatMessageConsumer {

    private final ThreadPoolExecutor msgPersistExecutor;
    private final ChatMessageMapper chatMessageMapper;
    private final LocalMsgOutboxMapper outboxMapper;
    private final SessionMapper sessionMapper;
    private final SessionCacheService sessionCacheService;
    private final MessageCacheService messageCacheService;
    private final WebSocketSessionManager wsSessionManager;
    private final StringRedisTemplate stringRedisTemplate;
    private final ObjectMapper objectMapper;

    private static final int MAX_RETRY = 3;
    private static final String MACHINE_ID = System.getenv().getOrDefault("MACHINE_ID", "node-1");

    @RabbitListener(queues = RabbitMQConfig.CHAT_MESSAGE_QUEUE)
    public void consume(Message message, Channel channel) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        String body = new String(message.getBody());
        String msgNo = message.getMessageProperties().getCorrelationId();

        try {
            // 提交到自定义线程池异步处理，避免阻塞 RabbitMQ 消费线程
            msgPersistExecutor.submit(() -> processWithRetry(body, msgNo));
            // 提交任务成功即 ACK，落库失败由 outbox 补偿任务兜底
            channel.basicAck(deliveryTag, false);
        } catch (Exception e) {
            log.error("[MQ Consumer] 提交任务失败, msgNo={}", msgNo, e);
            // NACK 不重新入队，进死信队列
            channel.basicNack(deliveryTag, false, false);
        }
    }

    // =========================================================================
    // 重试包装
    // =========================================================================

    private void processWithRetry(String payload, String msgNo) {
        int attempt = 0;
        while (attempt < MAX_RETRY) {
            try {
                processMessage(payload, msgNo);
                return; // 成功，退出
            } catch (Exception e) {
                attempt++;
                log.warn("[MQ Consumer] 处理消息失败，第{}次重试, msgNo={}, error={}",
                        attempt, msgNo, e.getMessage());
                if (attempt < MAX_RETRY) {
                    // 指数退避：1s、2s、4s
                    try {
                        Thread.sleep(1000L << (attempt - 1));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
        // 3次全部失败：从缓存中删除该消息，记录错误日志（消息已进死信队列）
        log.error("[MQ Consumer] 消息处理彻底失败，已丢入死信队列, msgNo={}", msgNo);
        cleanupOnFinalFailure(payload);
    }

    // =========================================================================
    // 核心处理逻辑
    // =========================================================================

    @Transactional(rollbackFor = Exception.class)
    public void processMessage(String payload, String msgNo) throws Exception {
        ChatMessageMQDTO dto = objectMapper.readValue(payload, ChatMessageMQDTO.class);

        // ① Redis 去重：SET NX，TTL 2min（与发送端幂等 key 共用同一个 key）
        // 发送端已写过 msgNo→msgId，这里直接判断 key 是否存在
        // 若 key 不存在（TTL 过期或首次消费），则允许继续处理
        String idemKey = RedisKeyConst.MSG_IDEM + dto.getMsgNo();
        Boolean isNew = stringRedisTemplate.opsForValue()
                .setIfAbsent(idemKey, String.valueOf(dto.getMsgId()), 2, java.util.concurrent.TimeUnit.MINUTES);
        // setIfAbsent 返回 false 说明 key 已存在（发送端写过，或消费端已处理过）
        // 但发送端写的是 msgNo→msgId，消费端也写同一个 key，所以 false 时需要判断是否已落库
        if (Boolean.FALSE.equals(isNew)) {
            // Double Check：查 DB 是否已存在该消息（防止 TTL 过期后重复消费）
            if (chatMessageMapper.selectById(dto.getMsgId()) != null) {
                log.debug("[MQ Consumer] 消息已落库，跳过, msgId={}", dto.getMsgId());
                return;
            }
            // key 存在但 DB 没有 → 发送端刚写的 key，消费端首次处理，继续
        }

        // ② 写 chat_message 分片表
        ChatMessage chatMessage = buildChatMessage(dto);
        chatMessageMapper.insert(chatMessage);

        // ③ 更新 session.last_msg_id / last_msg_content / last_msg_at
        LocalDateTime msgAt = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(dto.getTimestamp()), ZoneId.systemDefault());
        String lastMsgContent = buildLastMsgContent(dto);
        sessionMapper.updateLastMsg(dto.getSessionId(), dto.getMsgId(), lastMsgContent, msgAt);

        // 同步失效 session info 缓存（下次读时重建，保证 last_msg 字段最新）
        sessionCacheService.evict(dto.getSessionId());

        // ④ 更新 local_msg_outbox.status=2（只能从 status=0/1 更新，防止重复）
        outboxMapper.updateStatusByMsgNo(dto.getMsgNo(), 2);

        // ⑤ 推送：遍历会话成员，在线推 WebSocket，离线存 Redis List
        pushToMembers(dto);
    }

    // =========================================================================
    // WebSocket 推送 / 离线暂存
    // =========================================================================

    private void pushToMembers(ChatMessageMQDTO dto) {
        List<Long> memberIds = sessionCacheService.getMemberUserIds(dto.getSessionId());
        if (memberIds == null || memberIds.isEmpty()) return;

        String messageJson;
        try {
            messageJson = objectMapper.writeValueAsString(dto);
        } catch (Exception e) {
            log.error("[MQ Consumer] 序列化推送消息失败, msgId={}", dto.getMsgId(), e);
            return;
        }

        for (Long memberId : memberIds) {
            // 跳过发送者自己（发送端已通过 sending 状态感知）
            if (memberId.equals(dto.getSenderId())) continue;

            // 先查本机是否在线
            if (wsSessionManager.isLocalOnline(memberId)) {
                wsSessionManager.pushToLocal(memberId, messageJson);
            } else {
                // 查 Redis ws:online:{userId} 是否在其他节点在线
                String onlineKey = RedisKeyConst.WS_ONLINE + memberId;
                java.util.Map<Object, Object> onlineNodes =
                        stringRedisTemplate.opsForHash().entries(onlineKey);

                if (!onlineNodes.isEmpty()) {
                    // 在其他节点在线：通过 Pub/Sub 定向推送
                    for (Object nodeId : onlineNodes.keySet()) {
                        if (MACHINE_ID.equals(nodeId.toString())) continue; // 本机已处理
                        String pushChannel = RedisKeyConst.WS_PUSH_CHANNEL_PREFIX + nodeId;
                        String pushPayload = buildPushPayload(memberId, messageJson);
                        stringRedisTemplate.convertAndSend(pushChannel, pushPayload);
                    }
                } else {
                    // 完全离线：消息已写入 ZSet 缓冲区（msg:buf:{sessionId}），
                    // 前端上线后携带 last_read_msg_id 调用 listMessages 即可拉取离线期间的消息，
                    // 无需额外暂存到 List。
                }
            }
        }
    }

    // =========================================================================
    // 失败兜底：从缓存删除消息
    // =========================================================================

    private void cleanupOnFinalFailure(String payload) {
        try {
            ChatMessageMQDTO dto = objectMapper.readValue(payload, ChatMessageMQDTO.class);
            messageCacheService.removeMessage(dto.getSessionId(), dto.getMsgId());
        } catch (Exception e) {
            log.error("[MQ Consumer] 清理缓存失败", e);
        }
    }

    // =========================================================================
    // 工具方法
    // =========================================================================

    private ChatMessage buildChatMessage(ChatMessageMQDTO dto) {
        ChatMessage msg = new ChatMessage();
        msg.setId(dto.getMsgId());
        msg.setMsgNo(dto.getMsgNo());
        msg.setSessionId(dto.getSessionId());
        msg.setSenderId(dto.getSenderId());
        msg.setMsgType(dto.getMsgType());
        msg.setContent(dto.getContent());
        msg.setExtra(dto.getExtra());
        msg.setReplyMsgId(dto.getReplyMsgId());
        msg.setStatus(1); // 1正常
        msg.setIsRead(0);
        return msg;
    }

    /**
     * 构造会话列表展示用的最后一条消息摘要
     */
    private String buildLastMsgContent(ChatMessageMQDTO dto) {
        return switch (dto.getMsgType()) {
            case 1 -> dto.getContent() != null && dto.getContent().length() > 50
                    ? dto.getContent().substring(0, 50) + "..."
                    : dto.getContent();
            case 2 -> "[图片]";
            case 3 -> "[语音]";
            case 4 -> "[视频]";
            case 5 -> "[文件]";
            default -> "[消息]";
        };
    }

    private String buildPushPayload(Long userId, String message) {
        return String.format("{\"userId\":%d,\"message\":%s}", userId, message);
    }
}
