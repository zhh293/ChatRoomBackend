package com.example.chatroom.module.websocket.service;

import com.example.chatroom.module.netty.manager.NettyChannelManager;
import com.example.chatroom.module.websocket.manager.WebSocketSessionManager;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * WebSocket 客户端 ACK 管理器。
 *
 * <p>本地内存是 ACK 快路径，Redis 保存异步恢复影子。未收到 ACK 时使用
 * 指数退避 + Equal Jitter 有限重发；应用重启时恢复本节点记录，无法恢复或
 * 重试耗尽时由客户端按 lastReceivedMsgId 从消息接口补拉。</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class WebSocketAckManager {

    private final WebSocketSessionManager springSessionManager;
    private final NettyChannelManager nettyChannelManager;
    private final WebSocketAckStore ackStore;

    private final ConcurrentMap<PendingKey, PendingMessage> pendingMessages = new ConcurrentHashMap<>();
    private final ConcurrentMap<Long, AtomicInteger> pendingPerUser = new ConcurrentHashMap<>();
    private final DelayQueue<PendingMessage> retryQueue = new DelayQueue<>();
    private final AtomicInteger totalPending = new AtomicInteger();
    private final AtomicBoolean running = new AtomicBoolean(false);

    @Value("${chat.websocket.ack.max-retries:3}")
    private int maxRetries;

    @Value("${chat.websocket.ack.base-delay-ms:2000}")
    private long baseDelayMillis;

    @Value("${chat.websocket.ack.max-delay-ms:10000}")
    private long maxDelayMillis;

    @Value("${chat.websocket.ack.max-pending-per-user:200}")
    private int maxPendingPerUser;

    @Value("${chat.websocket.ack.max-pending-total:10000}")
    private int maxPendingTotal;

    @Value("${chat.websocket.ack.max-batch-size:50}")
    private int maxBatchSize;

    @Value("${chat.websocket.ack.recovery-batch-size:10000}")
    private int recoveryBatchSize;

    @Value("${chat.websocket.ack.recovery-delay-ms:5000}")
    private long recoveryDelayMillis;

    private static final String MACHINE_ID =
            System.getenv().getOrDefault("MACHINE_ID", "node-1");

    private Thread retryWorker;

    @PostConstruct
    public void start() {
        if (!running.compareAndSet(false, true)) return;
        recoverPendingMessages();
        retryWorker = new Thread(this::retryLoop, "ws-ack-retry");
        retryWorker.setDaemon(true);
        retryWorker.start();
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        if (retryWorker != null) retryWorker.interrupt();
        pendingMessages.clear();
        pendingPerUser.clear();
        retryQueue.clear();
        totalPending.set(0);
    }

    /**
     * 登记待确认消息后推送到本机连接。登记必须先于发送，防止快速 ACK 产生竞态。
     */
    public boolean pushWithAck(Long userId, Long msgId, Long sessionId, String payload) {
        PendingKey key = new PendingKey(userId, msgId);
        PendingMessage existing = pendingMessages.get(key);
        if (existing != null) {
            return pushBestEffortToLocal(userId, payload);
        }

        if (!reserveCapacity(userId)) {
            log.warn("[WS ACK] 待确认队列已满，降级为单次推送, userId={}, msgId={}", userId, msgId);
            return pushBestEffortToLocal(userId, payload);
        }

        PendingMessage pending = PendingMessage.schedule(
                key, sessionId, payload, 0, nextDelayMillis(0));
        PendingMessage raced = pendingMessages.putIfAbsent(key, pending);
        if (raced != null) {
            releaseCapacity(userId);
            return pushBestEffortToLocal(userId, payload);
        }

        retryQueue.offer(pending);
        persistPending(pending);
        boolean pushed = pushBestEffortToLocal(userId, payload);
        if (!pushed) {
            removePending(key, pending, true);
        }
        return pushed;
    }

    /** 客户端确认消息已持久化；重复 ACK 按成功处理。 */
    public boolean acknowledge(Long userId, Long msgId, Long sessionId) {
        PendingKey key = new PendingKey(userId, msgId);
        PendingMessage pending = pendingMessages.get(key);
        if (pending == null) {
            // 本地已确认、重启后未恢复或晚到 ACK：仍清理可能残留的 Redis 影子。
            ackStore.remove(MACHINE_ID, userId, msgId);
            return true;
        }
        if (!Objects.equals(pending.sessionId(), sessionId)) {
            log.warn("[WS ACK] sessionId 不匹配, userId={}, msgId={}, expected={}, actual={}",
                    userId, msgId, pending.sessionId(), sessionId);
            return false;
        }
        removePending(key, pending, true);
        log.debug("[WS ACK] 收到客户端确认, userId={}, msgId={}", userId, msgId);
        return true;
    }

    /**
     * 批量确认客户端已持久化的消息。内存删除保持 O(n)，Redis 清理合并成一批异步操作。
     * 超过配置上限的批次整体拒绝，避免恶意大包占用 IO 线程。
     *
     * @return 成功从本地 pending 中移除的数量；重复 ACK 仍按幂等成功处理
     */
    public int acknowledgeBatch(Long userId, List<AckItem> items) {
        if (userId == null || items == null || items.isEmpty()) return 0;
        if (items.size() > maxBatchSize) {
            log.warn("[WS ACK] 批量 ACK 超过上限, userId={}, size={}, max={}",
                    userId, items.size(), maxBatchSize);
            return 0;
        }

        Set<Long> cleanupMsgIds = new LinkedHashSet<>();
        int removedCount = 0;
        for (AckItem item : items) {
            if (item == null || item.msgId() == null || item.msgId() <= 0
                    || item.sessionId() == null || item.sessionId() <= 0) {
                continue;
            }
            PendingKey key = new PendingKey(userId, item.msgId());
            PendingMessage pending = pendingMessages.get(key);
            if (pending == null) {
                cleanupMsgIds.add(item.msgId()); // 清理可能残留的 Redis 影子
                continue;
            }
            if (!Objects.equals(pending.sessionId(), item.sessionId())) {
                log.warn("[WS ACK] 批量 ACK sessionId 不匹配, userId={}, msgId={}, expected={}, actual={}",
                        userId, item.msgId(), pending.sessionId(), item.sessionId());
                continue;
            }
            if (removeLocalPending(key, pending)) {
                removedCount++;
                cleanupMsgIds.add(item.msgId());
            }
        }
        if (!cleanupMsgIds.isEmpty()) {
            ackStore.removeBatch(MACHINE_ID, userId, cleanupMsgIds);
        }
        log.debug("[WS ACK] 批量确认完成, userId={}, requested={}, removed={}",
                userId, items.size(), removedCount);
        return removedCount;
    }

    public boolean isLocalOnline(Long userId) {
        return springSessionManager.isLocalOnline(userId)
                || nettyChannelManager.isLocalOnline(userId);
    }

    /** 向本机所有类型的 WebSocket 连接推送，不登记 ACK。 */
    public boolean pushBestEffortToLocal(Long userId, String payload) {
        boolean springPushed = springSessionManager.pushToLocal(userId, payload);
        int nettyPushed = nettyChannelManager.pushToUser(userId, payload);
        return springPushed || nettyPushed > 0;
    }

    public int getPendingCount() {
        return totalPending.get();
    }

    private void retryLoop() {
        while (running.get()) {
            try {
                PendingMessage expired = retryQueue.take();
                retry(expired);
            } catch (InterruptedException e) {
                if (!running.get()) break;
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("[WS ACK] 重试线程处理异常", e);
            }
        }
    }

    private void retry(PendingMessage expired) {
        PendingMessage current = pendingMessages.get(expired.key());
        if (current != expired) return;

        if (expired.retryCount() >= maxRetries) {
            removePending(expired.key(), expired, true);
            Long userId = expired.key().userId();
            boolean springClosed = springSessionManager.closeForResync(userId);
            int nettyClosed = nettyChannelManager.closeUserConnectionsForResync(userId);
            log.warn("[WS ACK] 达到最大重试次数，已关闭连接等待客户端重连补拉, "
                            + "userId={}, msgId={}, springClosed={}, nettyClosed={}",
                    userId, expired.key().msgId(), springClosed, nettyClosed);
            return;
        }

        if (!isLocalOnline(expired.key().userId())) {
            removePending(expired.key(), expired, true);
            return;
        }

        if (!pushBestEffortToLocal(expired.key().userId(), expired.payload())) {
            removePending(expired.key(), expired, true);
            return;
        }

        int nextRetryCount = expired.retryCount() + 1;
        PendingMessage next = PendingMessage.schedule(
                expired.key(), expired.sessionId(), expired.payload(), nextRetryCount,
                nextDelayMillis(nextRetryCount));
        if (pendingMessages.replace(expired.key(), expired, next)) {
            retryQueue.offer(next);
            persistPending(next);
            log.debug("[WS ACK] 未收到确认，已重发, userId={}, msgId={}, retry={}",
                    expired.key().userId(), expired.key().msgId(), nextRetryCount);
        }
    }

    /** Equal Jitter：delay = cap/2 + random(0, cap/2)，避免重试风暴且保留最小等待时间。 */
    long nextDelayMillis(int retryCount) {
        int shift = Math.min(Math.max(retryCount, 0), 30);
        long exponential;
        try {
            exponential = Math.multiplyExact(baseDelayMillis, 1L << shift);
        } catch (ArithmeticException e) {
            exponential = Long.MAX_VALUE;
        }
        long cap = Math.max(1L, Math.min(maxDelayMillis, exponential));
        long half = Math.max(1L, cap / 2);
        return half + ThreadLocalRandom.current().nextLong(cap - half + 1);
    }

    private boolean reserveCapacity(Long userId) {
        if (totalPending.incrementAndGet() > maxPendingTotal) {
            totalPending.decrementAndGet();
            return false;
        }
        AtomicInteger userPending = pendingPerUser.computeIfAbsent(userId, ignored -> new AtomicInteger());
        if (userPending.incrementAndGet() > maxPendingPerUser) {
            if (userPending.decrementAndGet() == 0) pendingPerUser.remove(userId, userPending);
            totalPending.decrementAndGet();
            return false;
        }
        return true;
    }

    private void releaseCapacity(Long userId) {
        totalPending.decrementAndGet();
        AtomicInteger userPending = pendingPerUser.get(userId);
        if (userPending != null && userPending.decrementAndGet() == 0) {
            pendingPerUser.remove(userId, userPending);
        }
    }

    private void removePending(PendingKey key, PendingMessage expected, boolean removeShadow) {
        if (removeLocalPending(key, expected) && removeShadow) {
            ackStore.remove(MACHINE_ID, key.userId(), key.msgId());
        }
    }

    private boolean removeLocalPending(PendingKey key, PendingMessage expected) {
        if (!pendingMessages.remove(key, expected)) return false;
        releaseCapacity(key.userId());
        return true;
    }

    private void persistPending(PendingMessage pending) {
        ackStore.save(MACHINE_ID, new WebSocketAckStore.PendingSnapshot(
                pending.key().userId(),
                pending.key().msgId(),
                pending.sessionId(),
                pending.payload(),
                pending.retryCount(),
                pending.expireAtMillis()));
    }

    private void recoverPendingMessages() {
        List<WebSocketAckStore.PendingSnapshot> snapshots =
                ackStore.loadForNode(MACHINE_ID, recoveryBatchSize);
        if (snapshots == null || snapshots.isEmpty()) return;

        int recovered = 0;
        List<Long> discardedMsgIds = new ArrayList<>();
        for (WebSocketAckStore.PendingSnapshot snapshot : snapshots) {
            if (!isValid(snapshot)) {
                if (snapshot != null && snapshot.userId() != null && snapshot.msgId() != null) {
                    ackStore.remove(MACHINE_ID, snapshot.userId(), snapshot.msgId());
                }
                continue;
            }
            PendingKey key = new PendingKey(snapshot.userId(), snapshot.msgId());
            if (pendingMessages.containsKey(key)) continue;
            if (!reserveCapacity(snapshot.userId())) {
                discardedMsgIds.add(snapshot.msgId());
                continue;
            }

            PendingMessage pending = PendingMessage.schedule(
                    key,
                    snapshot.sessionId(),
                    snapshot.payload(),
                    Math.max(0, snapshot.retryCount()),
                    Math.max(1L, recoveryDelayMillis));
            if (pendingMessages.putIfAbsent(key, pending) == null) {
                retryQueue.offer(pending);
                persistPending(pending);
                recovered++;
            } else {
                releaseCapacity(snapshot.userId());
            }
        }
        if (!discardedMsgIds.isEmpty()) {
            log.warn("[WS ACK] 恢复容量不足，剩余记录交由客户端游标补拉, count={}",
                    discardedMsgIds.size());
        }
        log.info("[WS ACK] 节点待确认记录恢复完成, nodeId={}, recovered={}", MACHINE_ID, recovered);
    }

    private boolean isValid(WebSocketAckStore.PendingSnapshot snapshot) {
        return snapshot != null
                && snapshot.userId() != null && snapshot.userId() > 0
                && snapshot.msgId() != null && snapshot.msgId() > 0
                && snapshot.sessionId() != null && snapshot.sessionId() > 0
                && snapshot.payload() != null;
    }

    private record PendingKey(Long userId, Long msgId) {}

    public record AckItem(Long msgId, Long sessionId) {}

    private record PendingMessage(
            PendingKey key,
            Long sessionId,
            String payload,
            int retryCount,
            long deadlineNanos,
            long expireAtMillis) implements Delayed {

        private static PendingMessage schedule(
                PendingKey key, Long sessionId, String payload, int retryCount, long delayMillis) {
            return new PendingMessage(
                    key, sessionId, payload, retryCount,
                    System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(delayMillis),
                    System.currentTimeMillis() + delayMillis);
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(deadlineNanos - System.nanoTime(), TimeUnit.NANOSECONDS);
        }

        @Override
        public int compareTo(Delayed other) {
            return Long.compare(deadlineNanos, ((PendingMessage) other).deadlineNanos);
        }
    }
}
