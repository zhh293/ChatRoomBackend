package com.example.chatroom.module.websocket.event;

import com.example.chatroom.module.websocket.manager.WebSocketSessionManager;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.stereotype.Component;

/**
 * Redis Pub/Sub 监听器
 * 订阅 ws:push:{machineId} 频道，接收其他节点转发过来的推送请求
 *
 * 频道设计：
 * - ws:sync          广播上下线事件，各机器同步连接状态
 * - ws:push:{nodeId} 定向推送，目标机器收到后找到本地 Session 推送
 *
 * 推送消息格式：{"userId":10001,"message":{...消息JSON...}}
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class WebSocketPubSubListener implements MessageListener {

    private final WebSocketSessionManager sessionManager;
    private final ObjectMapper objectMapper;

    private static final String MACHINE_ID = System.getenv().getOrDefault("MACHINE_ID", "node-1");

    @Override
    public void onMessage(Message message, byte[] pattern) {
        String channel = new String(message.getChannel());
        String body = new String(message.getBody());

        if (("ws:push:" + MACHINE_ID).equals(channel)) {
            handlePushMessage(body);
        } else if ("ws:sync".equals(channel)) {
            log.debug("[WS PubSub] 收到同步事件: {}", body);
        }
    }

    /**
     * 处理定向推送消息
     * 格式：{"userId":10001,"message":{...消息JSON...}}
     */
    private void handlePushMessage(String body) {
        try {
            JsonNode root = objectMapper.readTree(body);
            Long userId = root.get("userId").asLong();
            String messageJson = root.get("message").toString();

            boolean pushed = sessionManager.pushToLocal(userId, messageJson);
            if (!pushed) {
                // 用户已在推送过程中断开，忽略即可（离线消息已在消费端写入 Redis List）
                log.debug("[WS PubSub] 用户不在本机或连接已断开, userId={}", userId);
            }
        } catch (Exception e) {
            log.error("[WS PubSub] 处理推送消息失败, body={}", body, e);
        }
    }
}
