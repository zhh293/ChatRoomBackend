package com.example.chatroom.module.websocket.handler;

import com.example.chatroom.common.util.JwtUtil;
import com.example.chatroom.module.websocket.manager.WebSocketSessionManager;
import com.example.chatroom.module.websocket.service.WebSocketAckManager;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.jsonwebtoken.Claims;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.*;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * WebSocket 消息处理器
 * 连接地址：ws://host/ws/chat?token={accessToken}
 *
 * 连接建立：
 * 1. 验证 Token
 * 2. 注册到本机 ConcurrentHashMap
 * 3. 写 Redis ws:online:{userId}
 * 4. 发布上线事件到 ws:sync 频道
 * 5. 前端上线后凭 last_read_msg_id 调用 listMessages 拉取离线期间消息，无需服务端主动推送
 *
 * 连接断开：
 * 1. 从本机 Map 移除
 * 2. 删除 Redis ws:online:{userId}
 * 3. 发布下线事件
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ChatWebSocketHandler extends TextWebSocketHandler {

    private final JwtUtil jwtUtil;
    private final WebSocketSessionManager sessionManager;
    private final RedisTemplate<String, Object> redisTemplate;
    private final ObjectMapper objectMapper;
    private final WebSocketAckManager ackManager;

    private static final String WS_ONLINE_KEY_PREFIX = "ws:online:";
    private static final String WS_SYNC_CHANNEL = "ws:sync";
    // 机器ID，生产环境通过环境变量注入
    private static final String MACHINE_ID = System.getenv().getOrDefault("MACHINE_ID", "node-1");

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        Long userId = extractUserId(session);
        if (userId == null) {
            closeQuietly(session);
            return;
        }

        // 将 userId 存入 session 属性，供后续使用
        session.getAttributes().put("userId", userId);

        // 注册到本机 Map
        sessionManager.register(userId, session);

        // 写 Redis 在线状态
        redisTemplate.opsForHash().put(WS_ONLINE_KEY_PREFIX + userId, MACHINE_ID,
                String.valueOf(System.currentTimeMillis()));

        // 发布上线事件
        String event = String.format("{\"event\":\"online\",\"userId\":%d,\"machineId\":\"%s\"}", userId, MACHINE_ID);
        redisTemplate.convertAndSend(WS_SYNC_CHANNEL, event);

        log.info("[WS] 连接建立, userId={}", userId);
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        String payload = message.getPayload();
        // 处理心跳
        if ("{\"type\":\"PING\"}".equals(payload)) {
            try {
                session.sendMessage(new TextMessage("{\"type\":\"PONG\"}"));
            } catch (Exception e) {
                log.error("[WS] 发送 PONG 失败", e);
            }
            return;
        }
        try {
            JsonNode root = objectMapper.readTree(payload);
            String type = root.path("type").asText();
            if ("MESSAGE_ACK".equals(type)) {
                Long userId = (Long) session.getAttributes().get("userId");
                long msgId = root.path("msgId").asLong(0L);
                long sessionId = root.path("sessionId").asLong(0L);
                if (userId == null || msgId <= 0 || sessionId <= 0) {
                    log.warn("[WS] 非法 MESSAGE_ACK, userId={}, payload={}", userId, payload);
                    return;
                }
                ackManager.acknowledge(userId, msgId, sessionId);
                return;
            }
            if ("MESSAGE_ACK_BATCH".equals(type)) {
                Long userId = (Long) session.getAttributes().get("userId");
                JsonNode items = root.path("items");
                if (userId == null || !items.isArray()) {
                    log.warn("[WS] 非法 MESSAGE_ACK_BATCH, userId={}, payload={}", userId, payload);
                    return;
                }
                List<WebSocketAckManager.AckItem> ackItems = new ArrayList<>();
                for (JsonNode item : items) {
                    ackItems.add(new WebSocketAckManager.AckItem(
                            item.path("msgId").asLong(0L),
                            item.path("sessionId").asLong(0L)));
                }
                ackManager.acknowledgeBatch(userId, ackItems);
                return;
            }
        } catch (Exception e) {
            log.warn("[WS] 无法解析上行消息, payload={}", payload, e);
            return;
        }
        log.debug("[WS] 收到消息: {}", payload);
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        Long userId = (Long) session.getAttributes().get("userId");
        if (userId == null) return;

        // 从本机 Map 移除
        if (!sessionManager.remove(userId, session)) {
            // 旧连接的关闭回调不能清理刚建立的新连接及其在线状态。
            return;
        }

        // 删除 Redis 在线状态
        redisTemplate.opsForHash().delete(WS_ONLINE_KEY_PREFIX + userId, MACHINE_ID);

        // 发布下线事件
        String event = String.format("{\"event\":\"offline\",\"userId\":%d,\"machineId\":\"%s\"}", userId, MACHINE_ID);
        redisTemplate.convertAndSend(WS_SYNC_CHANNEL, event);

        log.info("[WS] 连接断开, userId={}, status={}", userId, status);
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) {
        log.error("[WS] 传输错误, sessionId={}", session.getId(), exception);
        closeQuietly(session);
    }

    private Long extractUserId(WebSocketSession session) {
        try {
            // 从 URL 参数中提取 token
            String query = session.getUri() != null ? session.getUri().getQuery() : null;
            if (query == null) return null;
            Map<String, String> params = parseQuery(query);
            String token = params.get("token");
            if (token == null) return null;
            Claims claims = jwtUtil.parseToken(token);
            return jwtUtil.getUserId(claims);
        } catch (Exception e) {
            log.warn("[WS] Token 验证失败: {}", e.getMessage());
            return null;
        }
    }

    private Map<String, String> parseQuery(String query) {
        Map<String, String> map = new java.util.HashMap<>();
        for (String pair : query.split("&")) {
            String[] kv = pair.split("=", 2);
            if (kv.length == 2) map.put(kv[0], kv[1]);
        }
        return map;
    }

    private void closeQuietly(WebSocketSession session) {
        try {
            session.close(CloseStatus.NOT_ACCEPTABLE);
        } catch (Exception ignored) {}
    }
}
