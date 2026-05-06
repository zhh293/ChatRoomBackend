package com.example.chatroom.module.netty.listener;

import com.example.chatroom.module.netty.manager.NettyChannelManager;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.stereotype.Component;

/**
 * Netty 层 Redis Pub/Sub 监听器
 *
 * <p>订阅 {@code ws:push:{machineId}} 频道，接收其他节点转发过来的推送请求，
 * 找到本机对应的 Channel 并推送消息。
 *
 * <p>频道设计（与原 Spring WebSocket 层保持一致）：
 * <ul>
 *   <li>{@code ws:push:{nodeId}} — 定向推送，目标机器收到后找到本地 Channel 推送</li>
 * </ul>
 *
 * <p>推送消息格式：{@code {"userId":10001,"message":{...消息JSON...}}}
 *
 * <p>此 Listener 需要在 Redis 配置中注册订阅，见 {@code RedisClusterConfig} 或
 * {@code application-dev.yml} 中的 Pub/Sub 配置。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class NettyPubSubListener implements MessageListener {

    private final NettyChannelManager channelManager;
    private final ObjectMapper objectMapper;

    private static final String MACHINE_ID =
            System.getenv().getOrDefault("MACHINE_ID", "node-1");

    @Override
    public void onMessage(Message message, byte[] pattern) {
        String channel = new String(message.getChannel());
        String body = new String(message.getBody());

        if (("ws:push:" + MACHINE_ID).equals(channel)) {
            handlePushMessage(body);
        }
        // ws:sync 广播事件：上下线不需要跨节点同步（Redis Hash 已是共享状态），忽略即可
    }

    /**
     * 处理定向推送消息
     * 格式：{@code {"userId":10001,"message":{...消息JSON...}}}
     */
    private void handlePushMessage(String body) {
        try {
            JsonNode root = objectMapper.readTree(body);
            Long userId = root.get("userId").asLong();
            String messageJson = root.get("message").toString();

            int pushed = channelManager.pushToUser(userId, messageJson);
            if (pushed == 0) {
                // 用户已在推送过程中断开，忽略即可（离线消息已在消费端写入 Redis List）
                log.debug("[Netty PubSub] 用户不在本机或连接已断开, userId={}", userId);
            } else {
                log.debug("[Netty PubSub] 推送成功, userId={}, 推送端数={}", userId, pushed);
            }
        } catch (Exception e) {
            log.error("[Netty PubSub] 处理推送消息失败, body={}", body, e);
        }
    }
}
