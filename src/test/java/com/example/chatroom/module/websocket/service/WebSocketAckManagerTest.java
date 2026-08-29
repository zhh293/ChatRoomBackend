package com.example.chatroom.module.websocket.service;

import com.example.chatroom.module.netty.manager.NettyChannelManager;
import com.example.chatroom.module.websocket.manager.WebSocketSessionManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class WebSocketAckManagerTest {

    private WebSocketAckManager ackManager;
    private WebSocketSessionManager springSessionManager;
    private NettyChannelManager nettyChannelManager;
    private WebSocketAckStore ackStore;

    @BeforeEach
    void setUp() {
        springSessionManager = mock(WebSocketSessionManager.class);
        nettyChannelManager = mock(NettyChannelManager.class);
        ackStore = mock(WebSocketAckStore.class);
        when(ackStore.loadForNode(anyString(), anyInt())).thenReturn(List.of());
        ackManager = new WebSocketAckManager(springSessionManager, nettyChannelManager, ackStore);

        ReflectionTestUtils.setField(ackManager, "baseDelayMillis", 2_000L);
        ReflectionTestUtils.setField(ackManager, "maxDelayMillis", 10_000L);
        ReflectionTestUtils.setField(ackManager, "maxPendingPerUser", 200);
        ReflectionTestUtils.setField(ackManager, "maxPendingTotal", 10_000);
        ReflectionTestUtils.setField(ackManager, "maxBatchSize", 50);
        ReflectionTestUtils.setField(ackManager, "recoveryBatchSize", 10_000);
        ReflectionTestUtils.setField(ackManager, "recoveryDelayMillis", 5_000L);
    }

    @AfterEach
    void tearDown() {
        ackManager.stop();
    }

    @Test
    void equalJitterShouldStayWithinExponentialBackoffWindow() {
        assertDelayRange(0, 1_000L, 2_000L);
        assertDelayRange(1, 2_000L, 4_000L);
        assertDelayRange(2, 4_000L, 8_000L);
        assertDelayRange(3, 5_000L, 10_000L);
    }

    @Test
    void ackShouldRemovePendingMessageAndRemainIdempotent() {
        long userId = 1L;
        long msgId = 101L;
        long sessionId = 10L;
        when(springSessionManager.pushToLocal(userId, "payload")).thenReturn(true);

        assertThat(ackManager.pushWithAck(userId, msgId, sessionId, "payload")).isTrue();
        assertThat(ackManager.getPendingCount()).isEqualTo(1);

        assertThat(ackManager.acknowledge(userId, msgId, sessionId)).isTrue();
        assertThat(ackManager.getPendingCount()).isZero();
        assertThat(ackManager.acknowledge(userId, msgId, sessionId)).isTrue();
        verify(ackStore, times(2)).remove(anyString(), eq(userId), eq(msgId));
    }

    @Test
    void ackShouldRejectMismatchedSession() {
        long userId = 1L;
        long msgId = 101L;
        when(springSessionManager.pushToLocal(userId, "payload")).thenReturn(true);

        ackManager.pushWithAck(userId, msgId, 10L, "payload");

        assertThat(ackManager.acknowledge(userId, msgId, 11L)).isFalse();
        assertThat(ackManager.getPendingCount()).isEqualTo(1);
    }

    @Test
    void batchAckShouldRemovePendingMessagesAndBatchRedisCleanup() {
        long userId = 1L;
        when(springSessionManager.pushToLocal(userId, "payload-1")).thenReturn(true);
        when(springSessionManager.pushToLocal(userId, "payload-2")).thenReturn(true);

        ackManager.pushWithAck(userId, 101L, 10L, "payload-1");
        ackManager.pushWithAck(userId, 102L, 10L, "payload-2");

        int removed = ackManager.acknowledgeBatch(userId, List.of(
                new WebSocketAckManager.AckItem(101L, 10L),
                new WebSocketAckManager.AckItem(102L, 10L)));

        assertThat(removed).isEqualTo(2);
        assertThat(ackManager.getPendingCount()).isZero();
        verify(ackStore).removeBatch(anyString(), eq(userId),
                argThat(ids -> ids.containsAll(List.of(101L, 102L))));
    }

    @Test
    void startShouldRecoverPendingMessagesFromRedisShadow() {
        when(ackStore.loadForNode(anyString(), anyInt())).thenReturn(List.of(
                new WebSocketAckStore.PendingSnapshot(
                        1L, 101L, 10L, "payload", 1, System.currentTimeMillis())));

        ackManager.start();

        assertThat(ackManager.getPendingCount()).isEqualTo(1);
        verify(ackStore).save(anyString(),
                argThat(snapshot -> snapshot.msgId().equals(101L) && snapshot.retryCount() == 1));
    }

    @Test
    void retryExhaustionShouldCloseConnectionsForResync() {
        long userId = 1L;
        when(springSessionManager.pushToLocal(userId, "payload")).thenReturn(true);
        ReflectionTestUtils.setField(ackManager, "maxRetries", 0);
        ReflectionTestUtils.setField(ackManager, "baseDelayMillis", 1L);
        ReflectionTestUtils.setField(ackManager, "maxDelayMillis", 1L);

        ackManager.start();
        ackManager.pushWithAck(userId, 101L, 10L, "payload");

        verify(springSessionManager, timeout(1_000)).closeForResync(userId);
        verify(nettyChannelManager, timeout(1_000)).closeUserConnectionsForResync(userId);
        assertThat(ackManager.getPendingCount()).isZero();
    }

    private void assertDelayRange(int retryCount, long lowerBound, long upperBound) {
        for (int i = 0; i < 100; i++) {
            assertThat(ackManager.nextDelayMillis(retryCount))
                    .isBetween(lowerBound, upperBound);
        }
    }
}
