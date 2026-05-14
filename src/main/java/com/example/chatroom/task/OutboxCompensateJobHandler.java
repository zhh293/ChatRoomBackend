package com.example.chatroom.task;

import com.example.chatroom.common.delay.DelayQueueService;
import com.example.chatroom.module.message.domain.entity.LocalMsgOutbox;
import com.example.chatroom.module.message.mapper.LocalMsgOutboxMapper;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 本地消息发件箱补偿任务（最终兜底，低频执行）
 *
 * <h3>定位</h3>
 * <pre>
 * 主力补偿：由 Redis 延迟队列（DelayQueueConsumer）精准驱动，无需扫表。
 * 本任务仅作为延迟队列的「最终保险」：
 *   - 延迟队列 Redis 故障时兜底
 *   - 延迟队列任务丢失时兜底（Redis 重启/主从切换丢数据）
 *   - 极端情况下的漏网之鱼
 * </pre>
 *
 * <h3>XXL-Job 配置</h3>
 * <pre>
 * JobHandler : outboxCompensateJobHandler
 * Cron       : 0 0 0/12 * * ?（每 12 小时）
 * 路由策略   : 分片广播（SHARDING_BROADCAST）
 * 超时       : 30s
 * 失败重试   : 0
 * </pre>
 *
 * <h3>核心逻辑</h3>
 * <pre>
 * 只捞 created_at < NOW() - 30min 且 status IN (0, 1) 的记录。
 * 正常消息通过延迟队列在 30s 内就处理了，30 分钟还没完成的说明延迟队列也没兜住。
 * 扫到后不直接投 MQ，而是重新加入延迟队列（addTask），让延迟队列统一处理。
 * 如果延迟队列本身故障，才直接投 MQ。
 * </pre>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxCompensateJobHandler {

    /** 每批查询条数 */
    private static final int BATCH_SIZE = 100;
    /** 单次任务最大处理条数 */
    private static final int MAX_PROCESS = 300;
    /** 兜底超时阈值（秒）：30 分钟前仍未落库的才捞 */
    private static final int FALLBACK_TIMEOUT_SECONDS = 1800;

    private final LocalMsgOutboxMapper outboxMapper;
    private final DelayQueueService delayQueueService;

    @XxlJob("outboxCompensateJobHandler")
    public void execute() {
        int shardIndex = XxlJobHelper.getShardIndex();
        int shardTotal = XxlJobHelper.getShardTotal();

        XxlJobHelper.log("[OutboxCompensate] 兜底扫描开始, shardIndex={}, shardTotal={}",
                shardIndex, shardTotal);

        // 只捞 5 分钟前仍未落库的记录
        LocalDateTime deadline = LocalDateTime.now().minusSeconds(FALLBACK_TIMEOUT_SECONDS);

        long lastId = 0L;
        int totalRecovered = 0;
        int totalProcessed = 0;

        while (totalProcessed < MAX_PROCESS) {
            List<LocalMsgOutbox> batch = outboxMapper.selectPendingCursor(
                    lastId, deadline, Integer.MAX_VALUE, shardIndex, shardTotal, BATCH_SIZE);

            if (batch.isEmpty()) break;

            for (LocalMsgOutbox outbox : batch) {
                lastId = outbox.getId();
                totalProcessed++;

                // 只处理 status=0 或 status=1 的记录
                if (outbox.getStatus() != 0 && outbox.getStatus() != 1) {
                    continue;
                }

                // 重新投入延迟队列，5s 后触发检查
                try {
                    delayQueueService.addTask(outbox.getMsgNo(), 5);
                    totalRecovered++;
                } catch (Exception e) {
                    // 延迟队列也故障了，记日志告警
                    log.error("[OutboxCompensate] 延迟队列投递失败, msgNo={}", outbox.getMsgNo(), e);
                }
            }

            if (batch.size() < BATCH_SIZE) break;
        }

        XxlJobHelper.log("[OutboxCompensate] 兜底扫描完毕, 恢复={}, 合计扫描={}", totalRecovered, totalProcessed);

        if (totalRecovered > 0) {
            log.warn("[OutboxCompensate] 兜底发现 {} 条超时未完成的消息，已重新加入延迟队列", totalRecovered);
        }
    }
}
