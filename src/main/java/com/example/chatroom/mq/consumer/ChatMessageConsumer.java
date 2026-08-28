package com.example.chatroom.mq.consumer;

import com.example.chatroom.cache.message.MessageCacheService;
import com.example.chatroom.cache.session.SessionCacheService;
import com.example.chatroom.common.config.RabbitMQConfig;
import com.example.chatroom.common.constant.RedisKeyConst;
import com.example.chatroom.common.delay.DelayQueueService;
import com.example.chatroom.module.call.domain.entity.CallRecord;
import com.example.chatroom.module.call.domain.enums.CallStatus;
import com.example.chatroom.module.call.mapper.CallRecordMapper;
import com.example.chatroom.module.message.domain.entity.ChatMessage;
import com.example.chatroom.module.message.domain.entity.MsgDeadLetter;
import com.example.chatroom.module.message.mapper.ChatMessageMapper;
import com.example.chatroom.module.message.mapper.LocalMsgOutboxMapper;
import com.example.chatroom.module.message.mapper.MsgDeadLetterMapper;
import com.example.chatroom.module.session.mapper.SessionMapper;
import com.example.chatroom.module.websocket.manager.WebSocketSessionManager;
import com.example.chatroom.mq.dto.ChatMessageMQDTO;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.aop.framework.AopContext;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
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
@EnableAspectJAutoProxy(exposeProxy = true)
public class ChatMessageConsumer {

    private final ThreadPoolExecutor msgPersistExecutor;
    private final ChatMessageMapper chatMessageMapper;
    private final LocalMsgOutboxMapper outboxMapper;
    private final MsgDeadLetterMapper deadLetterMapper;
    private final DelayQueueService delayQueueService;
    private final SessionMapper sessionMapper;
    private final CallRecordMapper callRecordMapper;
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
        Exception lastException = null;
        while (attempt < MAX_RETRY) {
            try {
                processMessage(payload, msgNo);
                return; // 成功，退出
            } catch (Exception e) {
                lastException = e;
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
        // 3次全部失败：①清缓存 ②写死信表 ③更新outbox状态 ④通知发送者
        log.error("[MQ Consumer] 消息处理彻底失败, msgNo={}", msgNo, lastException);
        handleFinalFailure(payload, lastException);
    }

    // =========================================================================
    // 核心处理逻辑
    // =========================================================================

    public void processMessage(String payload, String msgNo) throws Exception {
        ChatMessageMQDTO dto = objectMapper.readValue(payload, ChatMessageMQDTO.class);

        // ① 幂等判断：Redis 快速过滤 + DB 兜底
        // 先走 Redis SET NX 快速拦截大部分重复消费
        String idemKey = RedisKeyConst.MSG_IDEM + dto.getMsgNo();
        Boolean isNew = stringRedisTemplate.opsForValue()
                .setIfAbsent(idemKey, String.valueOf(dto.getMsgId()), 2, java.util.concurrent.TimeUnit.MINUTES);
        if (Boolean.FALSE.equals(isNew)) {
            log.debug("[MQ Consumer]发现重复消息,跳过，msgNo={}",dto.getMsgNo());
            return;
        } else {
            // Redis key 不存在（可能 TTL 过期），仍需查 DB 兜底防止重复入库
            if (chatMessageMapper.existsByMsgNo(dto.getSessionId(), dto.getMsgNo()) != null) {
                log.debug("[MQ Consumer] Redis key 过期但DB已存在，跳过, msgNo={}", dto.getMsgNo());
                return;
            }
        }

        // ② 通过代理对象调用，确保 @Transactional 生效
        ChatMessageConsumer proxy = (ChatMessageConsumer) AopContext.currentProxy();
        proxy.persistInTransaction(dto);

        // ②.5 落库成功，主动移除延迟队列中的任务（避免到期空跑）
        delayQueueService.removeTask(dto.getMsgNo());

        // ③ 失效 session info 缓存（非事务操作，DB 已提交后再做）
        sessionCacheService.evict(dto.getSessionId());

        // ④ 推送：遍历会话成员，在线推 WebSocket
        pushToMembers(dto);
    }

    /**
     * 事务仅包裹纯 DB 操作：写消息 + 写通话记录 + 更新会话最后一条消息 + 更新 outbox 状态
     * 任一失败整体回滚，外层重试
     */
    @Transactional(rollbackFor = Exception.class)
    public void persistInTransaction(ChatMessageMQDTO dto) {
        // 写 chat_message 分片表
        ChatMessage chatMessage = buildChatMessage(dto);
        chatMessageMapper.insert(chatMessage);

        // 语音通话消息：额外写 call_record，状态机设为 INVITING
        if (dto.getMsgType() != null && dto.getMsgType() == 7) {
            insertCallRecord(dto);
        }

        // 更新 session.last_msg_id / last_msg_content / last_msg_at
        LocalDateTime msgAt = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(dto.getTimestamp()), ZoneId.systemDefault());
        String lastMsgContent = buildLastMsgContent(dto);
        sessionMapper.updateLastMsg(dto.getSessionId(), dto.getMsgId(), lastMsgContent, msgAt);

        // 更新 local_msg_outbox.status=2
        outboxMapper.updateStatusByMsgNo(dto.getMsgNo(), 2);
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
    // 失败兜底：① 清缓存 ② 写死信表 ③ 更新 outbox 状态 ④ 通知发送者
    // =========================================================================

    private void handleFinalFailure(String payload, Exception lastException) {
        ChatMessageMQDTO dto;
        try {
            dto = objectMapper.readValue(payload, ChatMessageMQDTO.class);
        } catch (Exception e) {
            log.error("[MQ Consumer] 最终失败处理：反序列化失败，无法兜底, payload={}", payload, e);
            return;
        }

        // ① 清除 ZSet 缓存中的消息，避免用户看到「幽灵消息」
        try {
            messageCacheService.removeMessage(dto.getSessionId(), dto.getMsgId());
        } catch (Exception e) {
            log.error("[MQ Consumer] 清理缓存失败, msgNo={}", dto.getMsgNo(), e);
        }

        // ② 写死信表，保留消息全貌供人工排查
        try {
            saveToDeadLetter(dto, payload, lastException);
        } catch (Exception e) {
            log.error("[MQ Consumer] 写死信表失败, msgNo={}", dto.getMsgNo(), e);
        }

        // ③ 更新 outbox 状态为 3（消费失败）
        try {
            outboxMapper.updateStatusByMsgNo(dto.getMsgNo(), 3);
        } catch (Exception e) {
            log.error("[MQ Consumer] 更新outbox状态失败, msgNo={}", dto.getMsgNo(), e);
        }

        // ④ WebSocket 通知发送者：消息发送失败
        try {
            notifySenderFailed(dto);
        } catch (Exception e) {
            log.error("[MQ Consumer] 通知发送者失败, msgNo={}", dto.getMsgNo(), e);
        }
    }

    /**
     * 写入死信表：完整保存消息关键信息 + 失败原因，供人工介入或自动重试
     */
    private void saveToDeadLetter(ChatMessageMQDTO dto, String rawPayload, Exception ex) {
        MsgDeadLetter deadLetter = new MsgDeadLetter();
        deadLetter.setMsgId(dto.getMsgId());
        deadLetter.setMsgNo(dto.getMsgNo());
        deadLetter.setSessionId(dto.getSessionId());
        deadLetter.setSenderId(dto.getSenderId());
        deadLetter.setMsgType(dto.getMsgType());
        deadLetter.setContent(dto.getContent());
        deadLetter.setExtra(dto.getExtra());
        deadLetter.setReplyMsgId(dto.getReplyMsgId());
        deadLetter.setRawPayload(rawPayload);
        deadLetter.setFailReason(ex != null ? ex.getClass().getSimpleName() + ": " + ex.getMessage() : "unknown");
        deadLetter.setRetryCount(MAX_RETRY);
        deadLetter.setStatus(0); // 0-待处理
        deadLetterMapper.insert(deadLetter);
        log.warn("[MQ Consumer] 消息已写入死信表, msgNo={}, failReason={}", dto.getMsgNo(), deadLetter.getFailReason());
    }

    /**
     * WebSocket 通知发送者：你的消息发送失败了
     * 前端收到后可展示红色感叹号 + 「重发」按钮
     */
    private void notifySenderFailed(ChatMessageMQDTO dto) {
        String failNotice = String.format(
                "{\"type\":\"msg_send_failed\",\"msgNo\":\"%s\",\"msgId\":%d,\"sessionId\":%d}",
                dto.getMsgNo(), dto.getMsgId(), dto.getSessionId());

        if (wsSessionManager.isLocalOnline(dto.getSenderId())) {
            wsSessionManager.pushToLocal(dto.getSenderId(), failNotice);
        } else {
            // 可能在其他节点在线，走 Pub/Sub
            String onlineKey = RedisKeyConst.WS_ONLINE + dto.getSenderId();
            java.util.Map<Object, Object> onlineNodes =
                    stringRedisTemplate.opsForHash().entries(onlineKey);
            for (Object nodeId : onlineNodes.keySet()) {
                if (MACHINE_ID.equals(nodeId.toString())) continue;
                String pushChannel = RedisKeyConst.WS_PUSH_CHANNEL_PREFIX + nodeId;
                String pushPayload = buildPushPayload(dto.getSenderId(), failNotice);
                stringRedisTemplate.convertAndSend(pushChannel, pushPayload);
            }
            // 如果完全离线，前端上线后凭 outbox.status=3 也能感知失败，无需额外处理
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
            case 7 -> "[语音通话]";
            default -> "[消息]";
        };
    }

    private String buildPushPayload(Long userId, String message) {
        return String.format("{\"userId\":%d,\"message\":%s}", userId, message);
    }

    // =========================================================================
    // 语音通话：写入 call_record
    // =========================================================================

    /**
     * 从 MQ DTO 的 extra 字段解析 callId、calleeId，写入 call_record
     * 状态机初始状态：INVITING（邀请中）
     */
    private void insertCallRecord(ChatMessageMQDTO dto) {
        try {
            JsonNode extraNode = objectMapper.readTree(dto.getExtra());
            long callId = extraNode.get("callId").asLong();
            long calleeId = extraNode.get("calleeId").asLong();

            CallRecord record = new CallRecord();
            record.setId(callId); // 使用 callId 作为主键
            record.setCallId(callId);
            record.setSessionId(dto.getSessionId());
            record.setMessageId(dto.getMsgId());
            record.setCallerId(dto.getSenderId());
            record.setCalleeId(calleeId);
            record.setStatus(CallStatus.INVITING.name());
            callRecordMapper.insert(record);

            log.info("[MQ Consumer] 写入 call_record, callId={}, status=INVITING", callId);
        } catch (Exception e) {
            log.error("[MQ Consumer] 写入 call_record 失败, msgId={}", dto.getMsgId(), e);
            throw new RuntimeException("写入 call_record 失败", e);
        }
    }
}
