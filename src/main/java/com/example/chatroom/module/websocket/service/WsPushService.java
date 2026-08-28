package com.example.chatroom.module.websocket.service;

import com.example.chatroom.cache.session.SessionCacheService;
import com.example.chatroom.common.constant.RedisKeyConst;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * WebSocket 推送服务
 *
 * <p>封装向会话成员推送消息的完整逻辑：本机推送 + Redis Pub/Sub 跨节点转发。
 * 提供同步和异步两种调用方式，供不同场景使用：
 * <ul>
 *   <li>同步推送：MQ 消费者已在自己的线程池中，无需再开异步</li>
 *   <li>异步推送：撤回通知等 HTTP 接口场景，不阻塞接口返回</li>
 * </ul>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class WsPushService {

    private final SessionCacheService sessionCacheService;
    private final WebSocketAckManager ackManager;
    private final StringRedisTemplate stringRedisTemplate;
    private final ObjectMapper objectMapper;
    private final ThreadPoolExecutor msgPersistExecutor;

    private static final String MACHINE_ID = System.getenv().getOrDefault("MACHINE_ID", "node-1");

    /**
     * 异步推送给会话内所有在线成员（排除指定用户）
     * 不阻塞调用线程，推送失败不影响业务
     *
     * @param sessionId 会话ID
     * @param excludeUserId 排除的用户ID（如撤回者本人，前端已本地处理），传null不排除任何人
     * @param json 推送的JSON内容
     */
    public void pushToSessionMembersAsync(Long sessionId, Long excludeUserId, String json) {
        msgPersistExecutor.submit(() -> {
            try {
                pushToSessionMembers(sessionId, excludeUserId, json);
            } catch (Exception e) {
                log.error("[WsPush] 异步推送失败, sessionId={}", sessionId, e);
            }
        });
    }

    /**
     * 同步推送给会话内所有在线成员（排除指定用户）
     *
     * @param sessionId 会话ID
     * @param excludeUserId 排除的用户ID，传null不排除
     * @param json 推送的JSON内容
     */
    public void pushToSessionMembers(Long sessionId, Long excludeUserId, String json) {
        List<Long> memberIds = sessionCacheService.getMemberUserIds(sessionId);
        if (memberIds == null || memberIds.isEmpty()) return;

        for (Long memberId : memberIds) {
            if (memberId.equals(excludeUserId)) continue;
            pushToUser(memberId, json);
        }
    }

    /**
     * 推送给单个用户（本机 → 跨节点 Pub/Sub）
     */
    public void pushToUser(Long userId, String json) {
        // 先查本机
        if (ackManager.isLocalOnline(userId)) {
            ackManager.pushBestEffortToLocal(userId, json);
            return;
        }

        // 查 Redis 是否在其他节点在线
        String onlineKey = RedisKeyConst.WS_ONLINE + userId;
        Map<Object, Object> onlineNodes = stringRedisTemplate.opsForHash().entries(onlineKey);

        if (!onlineNodes.isEmpty()) {
            for (Object nodeId : onlineNodes.keySet()) {
                if (MACHINE_ID.equals(nodeId.toString())) continue;
                String pushChannel = RedisKeyConst.WS_PUSH_CHANNEL_PREFIX + nodeId;
                String pushPayload = buildPushPayload(userId, json);
                stringRedisTemplate.convertAndSend(pushChannel, pushPayload);
            }
        }
        // 完全离线：不做额外处理，前端下次拉取时自然从DB获取最新状态
    }

    /**
     * 推送需要客户端 ACK 的聊天消息。
     * 待确认记录由持有客户端连接的目标节点创建，避免跨节点 ACK 路由。
     */
    public void pushMessageToUser(Long userId, Long msgId, Long sessionId, String json) {
        if (ackManager.isLocalOnline(userId)) {
            ackManager.pushWithAck(userId, msgId, sessionId, json);
            return;
        }

        String onlineKey = RedisKeyConst.WS_ONLINE + userId;
        Map<Object, Object> onlineNodes = stringRedisTemplate.opsForHash().entries(onlineKey);
        for (Object nodeId : onlineNodes.keySet()) {
            if (MACHINE_ID.equals(nodeId.toString())) continue;
            String pushChannel = RedisKeyConst.WS_PUSH_CHANNEL_PREFIX + nodeId;
            String pushPayload = buildAckPushPayload(userId, msgId, sessionId, json);
            stringRedisTemplate.convertAndSend(pushChannel, pushPayload);
        }
    }

    private String buildPushPayload(Long userId, String message) {
        try {
            ObjectNode root = objectMapper.createObjectNode();
            root.put("userId", userId);
            root.set("message", objectMapper.readTree(message));
            return objectMapper.writeValueAsString(root);
        } catch (Exception e) {
            throw new IllegalArgumentException("无法构造跨节点推送消息", e);
        }
    }

    private String buildAckPushPayload(Long userId, Long msgId, Long sessionId, String message) {
        try {
            ObjectNode root = objectMapper.createObjectNode();
            root.put("userId", userId);
            root.put("requireAck", true);
            root.put("msgId", msgId);
            root.put("sessionId", sessionId);
            root.set("message", objectMapper.readTree(message));
            return objectMapper.writeValueAsString(root);
        } catch (Exception e) {
            throw new IllegalArgumentException("无法构造跨节点 ACK 推送消息", e);
        }
    }
}
