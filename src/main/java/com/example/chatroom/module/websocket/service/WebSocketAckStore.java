package com.example.chatroom.module.websocket.service;

import com.example.chatroom.common.constant.RedisKeyConst;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * 待 ACK 消息的 Redis 恢复影子。
 *
 * <p>本地内存仍是 ACK 快路径；Redis 操作在独立单线程中异步执行，
 * 节点重启后只恢复有限批次，避免 Redis 故障或恢复洪峰拖慢 WebSocket IO。</p>
 */
@Slf4j
@Component
public class WebSocketAckStore {

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final Executor persistenceExecutor;

    @Value("${chat.websocket.ack.shadow-ttl-seconds:86400}")
    private long shadowTtlSeconds;

    public WebSocketAckStore(
            StringRedisTemplate redisTemplate,
            ObjectMapper objectMapper,
            @Qualifier("wsAckPersistenceExecutor") Executor persistenceExecutor) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
        this.persistenceExecutor = persistenceExecutor;
    }

    public void save(String nodeId, PendingSnapshot snapshot) {
        executeAsync(() -> {
            String pendingKey = RedisKeyConst.WS_ACK_PENDING + nodeId;
            String deadlineKey = RedisKeyConst.WS_ACK_DEADLINE + nodeId;
            String field = field(snapshot.userId(), snapshot.msgId());
            String json = objectMapper.writeValueAsString(snapshot);

            redisTemplate.opsForHash().put(pendingKey, field, json);
            redisTemplate.opsForZSet().add(deadlineKey, field, snapshot.expireAtMillis());
            redisTemplate.expire(pendingKey, shadowTtlSeconds, TimeUnit.SECONDS);
            redisTemplate.expire(deadlineKey, shadowTtlSeconds, TimeUnit.SECONDS);
        }, "save");
    }

    public void remove(String nodeId, Long userId, Long msgId) {
        removeBatch(nodeId, userId, List.of(msgId));
    }

    /** 同一用户批量 ACK 时合并成一次 HDEL 和一次 ZREM。 */
    public void removeBatch(String nodeId, Long userId, Collection<Long> msgIds) {
        if (msgIds == null || msgIds.isEmpty()) return;
        Object[] fields = msgIds.stream()
                .distinct()
                .map(msgId -> field(userId, msgId))
                .toArray();
        executeAsync(() -> {
            redisTemplate.opsForHash().delete(RedisKeyConst.WS_ACK_PENDING + nodeId, fields);
            redisTemplate.opsForZSet().remove(RedisKeyConst.WS_ACK_DEADLINE + nodeId, fields);
        }, "remove");
    }

    /**
     * 按最早到期顺序读取本节点恢复影子。此方法仅在启动时同步执行；
     * Redis 不可用时返回空列表，客户端游标补拉仍是最终兜底。
     */
    public List<PendingSnapshot> loadForNode(String nodeId, int limit) {
        if (limit <= 0) return Collections.emptyList();
        try {
            String pendingKey = RedisKeyConst.WS_ACK_PENDING + nodeId;
            Set<String> fields = redisTemplate.opsForZSet().range(
                    RedisKeyConst.WS_ACK_DEADLINE + nodeId, 0, limit - 1L);
            if (fields == null || fields.isEmpty()) return Collections.emptyList();

            List<Object> hashFields = new ArrayList<>(fields);
            List<Object> values = redisTemplate.opsForHash().multiGet(pendingKey, hashFields);
            if (values == null || values.isEmpty()) return Collections.emptyList();

            List<PendingSnapshot> snapshots = new ArrayList<>(values.size());
            for (Object value : values) {
                if (value == null) continue;
                try {
                    snapshots.add(objectMapper.readValue(value.toString(), PendingSnapshot.class));
                } catch (Exception e) {
                    log.warn("[WS ACK Store] 忽略损坏的恢复记录, nodeId={}, value={}", nodeId, value, e);
                }
            }
            return snapshots;
        } catch (Exception e) {
            log.warn("[WS ACK Store] 加载恢复记录失败，降级为客户端游标补拉, nodeId={}", nodeId, e);
            return Collections.emptyList();
        }
    }

    private void executeAsync(ThrowingRunnable task, String operation) {
        try {
            persistenceExecutor.execute(() -> {
                try {
                    task.run();
                } catch (Exception e) {
                    log.warn("[WS ACK Store] Redis {} 失败，保留本地 ACK 快路径", operation, e);
                }
            });
        } catch (RejectedExecutionException e) {
            log.warn("[WS ACK Store] 异步队列已满，丢弃恢复影子操作 operation={}", operation);
        }
    }

    private String field(Long userId, Long msgId) {
        return userId + ":" + msgId;
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    public record PendingSnapshot(
            Long userId,
            Long msgId,
            Long sessionId,
            String payload,
            int retryCount,
            long expireAtMillis) {
    }
}
