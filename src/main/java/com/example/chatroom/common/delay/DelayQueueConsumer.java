package com.example.chatroom.common.delay;

import com.example.chatroom.module.message.domain.entity.LocalMsgOutbox;
import com.example.chatroom.module.message.mapper.LocalMsgOutboxMapper;
import com.example.chatroom.mq.producer.ChatMessageProducer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 延迟队列消费者
 *
 * <h3>线程模型</h3>
 * <pre>
 * 1. Transfer 线程（定时，每秒）：将 Delay ZSet 中到期的任务转移到 Ready List
 * 2. Consumer 线程（常驻，阻塞）：BRPOP Ready List，处理到期的 outbox 任务
 * 3. Recover 线程（定时，每 10 秒）：回收 Processing 中超时未 ACK 的任务
 * </pre>
 *
 * <h3>消费逻辑</h3>
 * <pre>
 * 取到 msgNo 后查 outbox 表：
 *   - status=2 → 消费端已落库，任务完成，ack
 *   - status=0 → 未发 MQ（发送端崩了），重新投递
 *   - status=1 → 已发 MQ 但消费端未落库（MQ 丢了/消费失败），重新投递
 *   - status=3 → 已标记消费失败，不再处理，ack
 *   - 找不到记录 → 异常数据，ack 丢弃并告警
 *
 * 重新投递后 outbox.retry_count++，超过最大重试次数标记 status=3
 * </pre>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DelayQueueConsumer {

    private final DelayQueueService delayQueueService;
    private final LocalMsgOutboxMapper outboxMapper;
    private final ChatMessageProducer mqProducer;

    /** 最大重试次数，超过后标记死信 */
    private static final int MAX_RETRY = 5;
    /** Transfer 每次最大转移数量 */
    private static final int TRANSFER_BATCH = 100;
    /** Recover 每次最大回迁数量 */
    private static final int RECOVER_BATCH = 50;
    /** BRPOP 阻塞超时（秒），超时后循环检查 running 标志 */
    private static final int POLL_TIMEOUT_SECONDS = 3;

    private final AtomicBoolean running = new AtomicBoolean(true);
    private ScheduledExecutorService scheduler;
    private ExecutorService consumerExecutor;

    @PostConstruct
    public void start() {
        // 定时线程池：Transfer + Recover
        scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "delay-queue-scheduler");
            t.setDaemon(true);
            return t;
        });

        // Transfer 线程：每秒扫描一次 Delay ZSet，将到期任务转移到 Ready List
        scheduler.scheduleWithFixedDelay(this::doTransfer, 1, 1, TimeUnit.SECONDS);

        // Recover 线程：每 10 秒扫描一次 Processing ZSet，回收超时任务
        scheduler.scheduleWithFixedDelay(this::doRecover, 10, 10, TimeUnit.SECONDS);

        // Consumer 线程：常驻阻塞消费
        consumerExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "delay-queue-consumer");
            t.setDaemon(true);
            return t;
        });
        consumerExecutor.submit(this::consumeLoop);

        log.info("[DelayQueueConsumer] 启动完成：Transfer(1s) + Consumer(BRPOP) + Recover(10s)");
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        if (scheduler != null) scheduler.shutdownNow();
        if (consumerExecutor != null) consumerExecutor.shutdownNow();
        log.info("[DelayQueueConsumer] 已停止");
    }

    // =========================================================================
    // Transfer：Delay → Ready
    // =========================================================================

    private void doTransfer() {
        try {
            long transferred = delayQueueService.transferReady(TRANSFER_BATCH);
            // 如果满批，可能还有更多到期任务，继续转移
            while (transferred == TRANSFER_BATCH) {
                transferred = delayQueueService.transferReady(TRANSFER_BATCH);
            }
        } catch (Exception e) {
            log.error("[DelayQueueConsumer] Transfer 异常", e);
        }
    }

    // =========================================================================
    // Consumer：BRPOP Ready → 处理 → ACK
    // =========================================================================

    private void consumeLoop() {
        while (running.get()) {
            try {
                String msgNo = delayQueueService.pollReady(POLL_TIMEOUT_SECONDS);
                if (msgNo == null) {
                    continue; // 超时无任务，继续循环
                }
                processTask(msgNo);
            } catch (Exception e) {
                if (running.get()) {
                    log.error("[DelayQueueConsumer] Consumer 处理异常", e);
                    // 短暂休眠防止异常风暴
                    try { Thread.sleep(1000); } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
        log.info("[DelayQueueConsumer] Consumer 线程退出");
    }

    /**
     * 处理单个延迟任务
     */
    private void processTask(String msgNo) {
        LocalMsgOutbox outbox = outboxMapper.selectByMsgNo(msgNo);

        if (outbox == null) {
            // 找不到记录：异常数据，可能是 outbox 已被归档清理
            log.warn("[DelayQueueConsumer] outbox 记录不存在, msgNo={}, 直接 ACK 丢弃", msgNo);
            delayQueueService.ack(msgNo);
            return;
        }

        switch (outbox.getStatus()) {
            case 2 -> {
                // 消费端已落库成功，任务完成
                delayQueueService.ack(msgNo);
                log.debug("[DelayQueueConsumer] 任务已完成(status=2), msgNo={}", msgNo);
            }
            case 3 -> {
                // 已标记消费失败/死信，不再处理
                delayQueueService.ack(msgNo);
                log.debug("[DelayQueueConsumer] 任务已标记失败(status=3), msgNo={}", msgNo);
            }
            case 0, 1 -> {
                // 未完成：需要重新投递 MQ
                retryDelivery(outbox);
                delayQueueService.ack(msgNo);
            }
            default -> {
                log.warn("[DelayQueueConsumer] 未知 status={}, msgNo={}", outbox.getStatus(), msgNo);
                delayQueueService.ack(msgNo);
            }
        }
    }

    /**
     * 重新投递到 MQ
     * 成功：不更新 status（等消费端处理完更新为 2）
     * 失败：retry_count++，超限标 status=3
     */
    private void retryDelivery(LocalMsgOutbox outbox) {
        try {
            mqProducer.sendWithConfirm(outbox.getPayload(), outbox.getMsgNo());
            log.info("[DelayQueueConsumer] 补偿投递成功, msgNo={}, retryCount={}",
                    outbox.getMsgNo(), outbox.getRetryCount());

            // 投递成功后，再投一个延迟任务进去：如果 30s 后 status 还不是 2，说明消费端又出问题了
            delayQueueService.addTask(outbox.getMsgNo(), 30);

        } catch (Exception e) {
            int nextRetryCount = outbox.getRetryCount() + 1;
            if (nextRetryCount >= MAX_RETRY) {
                // 超过最大重试次数，标记死信
                outboxMapper.updateStatusById(outbox.getId(), 3);
                log.error("[DelayQueueConsumer] 消息达最大重试次数，标记死信, msgNo={}, sessionId={}, senderId={}",
                        outbox.getMsgNo(), outbox.getSessionId(), outbox.getSenderId());
            } else {
                // 更新重试次数，指数退避：重新加入延迟队列，延迟时间递增
                long delaySeconds = (1L << nextRetryCount) * 30; // 60s, 120s, 240s, 480s
                outboxMapper.updateRetry(outbox.getId(), nextRetryCount,
                        LocalDateTime.now().plusSeconds(delaySeconds));
                delayQueueService.addTask(outbox.getMsgNo(), (int) delaySeconds);
                log.warn("[DelayQueueConsumer] 投递失败，安排退避重试, msgNo={}, nextRetry={}, delaySeconds={}",
                        outbox.getMsgNo(), nextRetryCount, delaySeconds);
            }
        }
    }

    // =========================================================================
    // Recover：回收超时任务
    // =========================================================================

    private void doRecover() {
        try {
            long recovered = delayQueueService.recoverTimeout(RECOVER_BATCH);
            while (recovered == RECOVER_BATCH) {
                recovered = delayQueueService.recoverTimeout(RECOVER_BATCH);
            }
        } catch (Exception e) {
            log.error("[DelayQueueConsumer] Recover 异常", e);
        }
    }
}
