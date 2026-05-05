package com.example.chatroom.mq.producer;

import com.example.chatroom.common.config.RabbitMQConfig;
import com.example.chatroom.module.message.mapper.LocalMsgOutboxMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

/**
 * 聊天消息 RabbitMQ 生产者
 * 使用 Publisher Confirm 模式，ACK 后更新 outbox.status=1
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ChatMessageProducer {

    private final RabbitTemplate rabbitTemplate;
    private final LocalMsgOutboxMapper outboxMapper;

    /**
     * 发送消息到 RabbitMQ（Publisher Confirm）
     * @param payload MQ 消息体 JSON
     * @param msgNo   消息业务编号（作为 CorrelationData ID，用于 Confirm 回调定位）
     */
    public void sendWithConfirm(String payload, String msgNo) {
        CorrelationData correlationData = new CorrelationData(msgNo);

        // 注册 Confirm 回调
        correlationData.getFuture().whenComplete((confirm, ex) -> {
            if (ex != null) {
                log.error("[MQ Producer] 发送异常, msgNo={}", msgNo, ex);
                return;
            }
            if (confirm != null && confirm.isAck()) {
                // Confirm ACK：更新 outbox.status=1（已发送MQ）
                outboxMapper.updateStatusByMsgNo(msgNo, 1);
                log.debug("[MQ Producer] Confirm ACK, msgNo={}", msgNo);
            } else {
                // Confirm NACK：不更新，等定时任务重试
                String reason = confirm != null ? confirm.getReason() : "unknown";
                log.error("[MQ Producer] Confirm NACK, msgNo={}, reason={}", msgNo, reason);
            }
        });

        rabbitTemplate.convertAndSend(
                RabbitMQConfig.CHAT_EXCHANGE,
                RabbitMQConfig.CHAT_MESSAGE_ROUTING_KEY,
                payload,
                correlationData
        );
    }
}
