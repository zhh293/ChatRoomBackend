package com.example.chatroom.common.config;

import com.example.chatroom.module.websocket.event.WebSocketPubSubListener;
import com.example.chatroom.module.websocket.handler.ChatWebSocketHandler;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.Topic;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

/**
 * WebSocket + Redis Pub/Sub 配置
 */
@Configuration
@EnableWebSocket
@RequiredArgsConstructor
public class WebSocketConfig implements WebSocketConfigurer {

    private final ChatWebSocketHandler chatWebSocketHandler;
    private final WebSocketPubSubListener pubSubListener;

    private static final String MACHINE_ID = System.getenv().getOrDefault("MACHINE_ID", "node-1");

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(chatWebSocketHandler, "/ws/chat")
                .setAllowedOriginPatterns("*");
    }

    /**
     * Redis Pub/Sub 监听容器
     * 订阅：ws:sync（广播）+ ws:push:{machineId}（定向推送）
     */
    @Bean
    public RedisMessageListenerContainer redisMessageListenerContainer(
            RedisConnectionFactory connectionFactory) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);

        // 订阅广播频道
        container.addMessageListener(pubSubListener, new PatternTopic("ws:sync"));
        // 订阅本机定向推送频道
        container.addMessageListener(pubSubListener,
                new PatternTopic("ws:push:" + MACHINE_ID));

        return container;
    }
}
