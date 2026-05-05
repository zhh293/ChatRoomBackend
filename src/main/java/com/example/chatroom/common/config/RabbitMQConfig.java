package com.example.chatroom.common.config;

import org.springframework.amqp.core.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * RabbitMQ Exchange / Queue / Binding 配置
 *
 * 拓扑结构：
 *   chat.exchange (Direct)
 *     ├── routing-key: chat.message  →  chat.message.queue（主队列）
 *     └── routing-key: chat.message.dlx  →  chat.message.dlq（死信队列）
 */
@Configuration
public class RabbitMQConfig {

    public static final String CHAT_EXCHANGE = "chat.exchange";
    public static final String CHAT_MESSAGE_QUEUE = "chat.message.queue";
    public static final String CHAT_MESSAGE_ROUTING_KEY = "chat.message";

    public static final String CHAT_DLX_EXCHANGE = "chat.dlx.exchange";
    public static final String CHAT_DLQ = "chat.message.dlq";
    public static final String CHAT_DLX_ROUTING_KEY = "chat.message.dlx";

    // ===== 主 Exchange =====
    @Bean
    public DirectExchange chatExchange() {
        return ExchangeBuilder.directExchange(CHAT_EXCHANGE).durable(true).build();
    }

    // ===== 死信 Exchange =====
    @Bean
    public DirectExchange chatDlxExchange() {
        return ExchangeBuilder.directExchange(CHAT_DLX_EXCHANGE).durable(true).build();
    }

    // ===== 主队列（绑定死信） =====
    @Bean
    public Queue chatMessageQueue() {
        return QueueBuilder.durable(CHAT_MESSAGE_QUEUE)
                .withArgument("x-dead-letter-exchange", CHAT_DLX_EXCHANGE)
                .withArgument("x-dead-letter-routing-key", CHAT_DLX_ROUTING_KEY)
                .build();
    }

    // ===== 死信队列 =====
    @Bean
    public Queue chatDeadLetterQueue() {
        return QueueBuilder.durable(CHAT_DLQ).build();
    }

    // ===== Binding =====
    @Bean
    public Binding chatMessageBinding() {
        return BindingBuilder.bind(chatMessageQueue())
                .to(chatExchange())
                .with(CHAT_MESSAGE_ROUTING_KEY);
    }

    @Bean
    public Binding chatDlqBinding() {
        return BindingBuilder.bind(chatDeadLetterQueue())
                .to(chatDlxExchange())
                .with(CHAT_DLX_ROUTING_KEY);
    }
}
