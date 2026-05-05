package com.example.chatroom.task;

import com.example.chatroom.module.message.domain.entity.LocalMsgOutbox;
import com.example.chatroom.module.message.mapper.LocalMsgOutboxMapper;
import com.example.chatroom.mq.producer.ChatMessageProducer;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 本地消息发件箱补偿任务（Outbox Pattern 核心保障）
 *
 * <h3>XXL-Job 配置</h3>
 * <pre>
 * JobHandler : outboxCompensateJobHandler
 * Cron       : 0/30 * * * * ?（每30秒）
 * 路由策略   : 分片广播（SHARDING_BROADCAST）
 * 超时       : 25s
 * 失败重试   : 0（任务本身不重试，下一个30s周期自然触发）
 * </pre>
 *
 * <h3>核心设计</h3>
 * <pre>
 * ① 时间窗口：只捞 created_at < NOW()-30s 的记录，避免误捞正常飞行中的消息
 * ② 分片广播：id % shardTotal == shardIndex，各节点处理不重叠的数据集
 * ③ CAS 抢占：UPDATE status=0→1 WHERE id=? AND status=0，防止任务重跑重复投递
 * ④ 游标翻页：id > lastId ORDER BY id ASC，防止深度分页，单次最多处理 BATCH_SIZE 条
 *    若本批满载（=BATCH_SIZE），继续翻页直到捞完或达到单次最大处理量 MAX_PROCESS
 * ⑤ 指数退避：投递失败后 next_retry_at = NOW() + 2^retryCount 分钟（1/2/4/8/16min）
 * ⑥ 死信标记：retry_count >= MAX_RETRY 时 status=3，停止重投，触发告警
 * </pre>
 *
 * <h3>幂等保障</h3>
 * 消费端已有 Redis SET NX + DB 查重双重幂等，重复投递完全安全。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxCompensateJobHandler {

    /** 每批查询条数 */
    private static final int BATCH_SIZE = 100;
    /** 单次任务最大处理条数（防止单次跑太久超时） */
    private static final int MAX_PROCESS = 500;
    /** 最大重试次数，超过后标记死信 */
    private static final int MAX_RETRY = 5;
    /**
     * 飞行超时阈值（秒）：只捞超过此时间仍未落库的消息
     * 正常链路：发送端写 Outbox → 发 MQ → 消费端落库更新 status=2，全程 < 5s
     * 30s 足够覆盖 MQ 抖动，同时不会误捞正常消息
     */
    private static final int FLIGHT_TIMEOUT_SECONDS = 30;

    private final LocalMsgOutboxMapper outboxMapper;
    private final ChatMessageProducer mqProducer;

    @XxlJob("outboxCompensateJobHandler")
    public void execute() {
        int shardIndex = XxlJobHelper.getShardIndex();
        int shardTotal = XxlJobHelper.getShardTotal();

        XxlJobHelper.log("[OutboxCompensate] 开始执行，shardIndex={}, shardTotal={}",
                shardIndex, shardTotal);

        // 时间窗口：只处理超过 FLIGHT_TIMEOUT_SECONDS 秒仍未落库的消息
        LocalDateTime deadline = LocalDateTime.now().minusSeconds(FLIGHT_TIMEOUT_SECONDS);

        long lastId = 0L;
        int totalSuccess = 0;
        int totalFail = 0;
        int totalSkip = 0;
        int totalProcessed = 0;

        // 游标翻页，直到捞完或达到单次最大处理量
        while (totalProcessed < MAX_PROCESS) {
            List<LocalMsgOutbox> batch = outboxMapper.selectPendingCursor(
                    lastId, deadline, MAX_RETRY, shardIndex, shardTotal, BATCH_SIZE);

            if (batch.isEmpty()) break;

            for (LocalMsgOutbox outbox : batch) {
                lastId = outbox.getId();
                totalProcessed++;

                // ③ CAS 抢占：只有 status=0 时才能抢到，防止并发重复投递
                // status=1 的记录（已发送MQ但超时未落库）直接重投，不需要 CAS
                if (outbox.getStatus() == 0) {
                    int claimed = outboxMapper.casClaimForRetry(outbox.getId());
                    if (claimed == 0) {
                        // 被其他节点/线程抢走，跳过
                        totalSkip++;
                        continue;
                    }
                }

                // ④ 重新投递到 MQ
                try {
                    mqProducer.sendWithConfirm(outbox.getPayload(), outbox.getMsgNo());
                    totalSuccess++;
                    XxlJobHelper.log("[OutboxCompensate] 补偿投递成功，msgNo={}, retryCount={}",
                            outbox.getMsgNo(), outbox.getRetryCount());
                } catch (Exception e) {
                    totalFail++;
                    int nextRetryCount = outbox.getRetryCount() + 1;

                    if (nextRetryCount >= MAX_RETRY) {
                        // 超过最大重试次数，标记死信，停止重投
                        outboxMapper.updateStatusById(outbox.getId(), 3);
                        XxlJobHelper.log(
                                "[OutboxCompensate] 消息已达最大重试次数，标记死信，msgNo={}, retryCount={}",
                                outbox.getMsgNo(), outbox.getRetryCount());
                        log.error("[OutboxCompensate] 消息死信告警！msgNo={}, sessionId={}, senderId={}",
                                outbox.getMsgNo(), outbox.getSessionId(), outbox.getSenderId());
                    } else {
                        // 指数退避：1min, 2min, 4min, 8min, 16min
                        long delayMinutes = 1L << outbox.getRetryCount(); // 2^retryCount
                        LocalDateTime nextRetryAt = LocalDateTime.now().plusMinutes(delayMinutes);
                        outboxMapper.updateRetry(outbox.getId(), nextRetryCount, nextRetryAt);
                        XxlJobHelper.log(
                                "[OutboxCompensate] 投递失败，已安排退避重试，msgNo={}, nextRetry={}, nextRetryAt={}",
                                outbox.getMsgNo(), nextRetryCount, nextRetryAt);
                    }
                }
            }

            // 本批不满，说明已捞完
            if (batch.size() < BATCH_SIZE) break;
        }

        XxlJobHelper.log(
                "[OutboxCompensate] 执行完毕，成功={}, 失败={}, 跳过(CAS)={}, 合计处理={}",
                totalSuccess, totalFail, totalSkip, totalProcessed);

        // 有死信时让 XXL-Job 标记任务失败，触发告警
        if (totalFail > 0) {
            XxlJobHelper.handleFail(
                    String.format("存在 %d 条消息投递失败（其中部分可能已标记死信），请检查 MQ 连通性", totalFail));
        }
    }
}
