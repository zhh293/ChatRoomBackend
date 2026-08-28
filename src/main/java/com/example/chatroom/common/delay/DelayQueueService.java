package com.example.chatroom.common.delay;

import com.example.chatroom.common.constant.RedisKeyConst;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Redis 延迟队列服务（三队列模型）
 *
 * <h3>队列模型</h3>
 * <pre>
 * ┌──────────────────┐       ┌──────────────────┐       ┌──────────────────┐
 * │  Delay Queue     │ ───→  │  Ready Queue     │ ───→  │ Processing Queue │
 * │  (ZSet)          │       │  (List)          │       │  (ZSet)          │
 * │  score=到期时间   │       │  BRPOP 消费      │       │  score=超时时间   │
 * └──────────────────┘       └──────────────────┘       └──────────────────┘
 *         ↑                                                      │
 *         └──────────── 超时未 ACK，回迁 ────────────────────────┘
 * </pre>
 *
 * <h3>核心流程</h3>
 * <pre>
 * ① 发送端写完 outbox 后调 addTask(msgNo, delaySeconds)
 * ② 转移线程每秒执行 transferReady()：原子迁移到期任务到 Ready List
 * ③ 消费线程 BRPOP Ready List → 放入 Processing ZSet
 * ④ 查 outbox status 决定处理方式 → 完成后 ack
 * ⑤ 超时兜底线程扫描 Processing 中超时任务 → 回迁 Delay ZSet
 * </pre>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DelayQueueService {

    private final StringRedisTemplate stringRedisTemplate;

    /** Processing 超时时间（毫秒）：60s 内必须 ack */
    private static final long PROCESSING_TIMEOUT_MS = 60_000L;

    /** 超时回迁后重新延迟时间（毫秒）：5s 后重试 */
    private static final long RECOVER_DELAY_MS = 5_000L;

    // =========================================================================
    // Lua 脚本：原子转移 Delay → Ready
    // =========================================================================

    /**
     * KEYS[1] = Delay ZSet, KEYS[2] = Ready List
     * ARGV[1] = 当前时间戳(ms), ARGV[2] = 批量上限
     * 返回：转移数量
     */
    private static final RedisScript<Long> TRANSFER_SCRIPT = new DefaultRedisScript<>(
            "local tasks = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2])) " +
            "if #tasks == 0 then return 0 end " +
            "for i, task in ipairs(tasks) do " +
            "  redis.call('ZREM', KEYS[1], task) " +
            "  redis.call('LPUSH', KEYS[2], task) " +
            "end " +
            "return #tasks",
            Long.class
    );

    // =========================================================================
    // Lua 脚本：原子回迁 Processing → Delay
    // =========================================================================

    /**
     * KEYS[1] = Processing ZSet, KEYS[2] = Delay ZSet
     * ARGV[1] = 当前时间戳(ms), ARGV[2] = 回迁后新 score, ARGV[3] = 批量上限
     * 返回：回迁数量
     */
    private static final RedisScript<Long> RECOVER_SCRIPT = new DefaultRedisScript<>(
            "local tasks = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[3])) " +
            "if #tasks == 0 then return 0 end " +
            "for i, task in ipairs(tasks) do " +
            "  redis.call('ZREM', KEYS[1], task) " +
            "  redis.call('ZADD', KEYS[2], ARGV[2], task) " +
            "end " +
            "return #tasks",
            Long.class
    );

    // =========================================================================
    // 公开方法
    // =========================================================================

    /**
     * 添加延迟任务到 Delay ZSet
     *
     * @param taskId       任务标识（msgNo，ZSet 天然去重）
     * @param delaySeconds 延迟秒数
     */
    public void addTask(String taskId, int delaySeconds) {
        long executeAt = System.currentTimeMillis() + delaySeconds * 1000L;
        stringRedisTemplate.opsForZSet().add(RedisKeyConst.DELAY_QUEUE, taskId, executeAt);
        log.debug("[DelayQueue] 添加任务, taskId={}, delaySeconds={}", taskId, delaySeconds);
    }

    /**
     * 转移到期任务：Delay ZSet → Ready List（Lua 原子操作）
     *
     * @param batchSize 单次最大转移数
     * @return 实际转移数量
     */
    public long transferReady(int batchSize) {
        long now = System.currentTimeMillis();
        Long count = stringRedisTemplate.execute(
                TRANSFER_SCRIPT,
                List.of(RedisKeyConst.DELAY_QUEUE, RedisKeyConst.DELAY_READY),
                String.valueOf(now), String.valueOf(batchSize));
        if (count != null && count > 0) {
            log.debug("[DelayQueue] 转移到期任务 {} 条", count);
        }
        return count != null ? count : 0;
    }

    /**
     * 从 Ready List 阻塞弹出一个任务，并放入 Processing ZSet
     *
     * @param timeoutSeconds BRPOP 阻塞超时秒数
     * @return 任务 ID（msgNo），超时返回 null
     */
    public String pollReady(int timeoutSeconds) {
        String taskId = stringRedisTemplate.opsForList()
                .rightPop(RedisKeyConst.DELAY_READY, timeoutSeconds, TimeUnit.SECONDS);
        if (taskId == null) {
            return null;
        }
        // 放入 Processing ZSet，score = 超时时间点
        long timeoutAt = System.currentTimeMillis() + PROCESSING_TIMEOUT_MS;
        stringRedisTemplate.opsForZSet().add(RedisKeyConst.DELAY_PROCESSING, taskId, timeoutAt);
        log.debug("[DelayQueue] 取到任务, taskId={}", taskId);
        return taskId;
    }

    /**
     * 确认任务完成：从 Processing ZSet 移除
     */
    public void ack(String taskId) {
        stringRedisTemplate.opsForZSet().remove(RedisKeyConst.DELAY_PROCESSING, taskId);
        log.debug("[DelayQueue] ACK, taskId={}", taskId);
    }

    /**
     * 回收超时任务：Processing ZSet → 回迁 Delay ZSet（Lua 原子操作）
     *
     * @param batchSize 单次最大回迁数
     * @return 实际回迁数量
     */
    public long recoverTimeout(int batchSize) {
        long now = System.currentTimeMillis();
        long newScore = now + RECOVER_DELAY_MS;
        Long count = stringRedisTemplate.execute(
                RECOVER_SCRIPT,
                List.of(RedisKeyConst.DELAY_PROCESSING, RedisKeyConst.DELAY_QUEUE),
                String.valueOf(now), String.valueOf(newScore), String.valueOf(batchSize));
        if (count != null && count > 0) {
            log.warn("[DelayQueue] 回收超时任务 {} 条", count);
        }
        return count != null ? count : 0;
    }

    /**
     * 主动移除任务（消费端落库成功后调用，避免到期空跑）
     * 三个队列都尝试删，无论任务当前在哪个阶段
     */
    public void removeTask(String taskId) {
        stringRedisTemplate.opsForZSet().remove(RedisKeyConst.DELAY_QUEUE, taskId);
        stringRedisTemplate.opsForList().remove(RedisKeyConst.DELAY_READY, 1, taskId);
        stringRedisTemplate.opsForZSet().remove(RedisKeyConst.DELAY_PROCESSING, taskId);
        log.debug("[DelayQueue] 移除任务, taskId={}", taskId);
    }
}
