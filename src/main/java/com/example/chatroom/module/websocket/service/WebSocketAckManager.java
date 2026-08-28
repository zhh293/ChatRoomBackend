package com.example.chatroom.module.websocket.service;

import com.example.chatroom.module.netty.manager.NettyChannelManager;
import com.example.chatroom.module.websocket.manager.WebSocketSessionManager;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Objects;
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
 * <p>待确认消息只保存在拥有客户端连接的节点内存中。未收到 ACK 时使用
 * 指数退避 + Equal Jitter 有限重发；应用重启或重试耗尽后由客户端按
 * lastReceivedMsgId 从消息接口补拉。</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class WebSocketAckManager {

    private final WebSocketSessionManager springSessionManager;
    private final NettyChannelManager nettyChannelManager;

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

    private Thread retryWorker;

    @PostConstruct
    public void start() {
        if (!running.compareAndSet(false, true)) return;
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
        boolean pushed = pushBestEffortToLocal(userId, payload);
        if (!pushed) {
            removePending(key, pending);
        }
        return pushed;
    }

    /** 客户端确认消息已持久化；重复 ACK 按成功处理。 */
    public boolean acknowledge(Long userId, Long msgId, Long sessionId) {
        PendingKey key = new PendingKey(userId, msgId);
        PendingMessage pending = pendingMessages.get(key);
        if (pending == null) return true;
        if (!Objects.equals(pending.sessionId(), sessionId)) {
            log.warn("[WS ACK] sessionId 不匹配, userId={}, msgId={}, expected={}, actual={}",
                    userId, msgId, pending.sessionId(), sessionId);
            return false;
        }
        removePending(key, pending);
        log.debug("[WS ACK] 收到客户端确认, userId={}, msgId={}", userId, msgId);
        return true;
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
            removePending(expired.key(), expired);
            Long userId = expired.key().userId();
            boolean springClosed = springSessionManager.closeForResync(userId);
            int nettyClosed = nettyChannelManager.closeUserConnectionsForResync(userId);
            log.warn("[WS ACK] 达到最大重试次数，已关闭连接等待客户端重连补拉, "
                            + "userId={}, msgId={}, springClosed={}, nettyClosed={}",
                    userId, expired.key().msgId(), springClosed, nettyClosed);
            return;
        }

        if (!isLocalOnline(expired.key().userId())) {
            removePending(expired.key(), expired);
            return;
        }

        if (!pushBestEffortToLocal(expired.key().userId(), expired.payload())) {
            removePending(expired.key(), expired);
            return;
        }

        int nextRetryCount = expired.retryCount() + 1;
        PendingMessage next = PendingMessage.schedule(
                expired.key(), expired.sessionId(), expired.payload(), nextRetryCount,
                nextDelayMillis(nextRetryCount));
        if (pendingMessages.replace(expired.key(), expired, next)) {
            retryQueue.offer(next);
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

    private void removePending(PendingKey key, PendingMessage expected) {
        if (pendingMessages.remove(key, expected)) {
            releaseCapacity(key.userId());
        }
    }

    private record PendingKey(Long userId, Long msgId) {}

    private record PendingMessage(
            PendingKey key,
            Long sessionId,
            String payload,
            int retryCount,
            long deadlineNanos) implements Delayed {

        private static PendingMessage schedule(
                PendingKey key, Long sessionId, String payload, int retryCount, long delayMillis) {
            return new PendingMessage(
                    key, sessionId, payload, retryCount,
                    System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(delayMillis));
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
