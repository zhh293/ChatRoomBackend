package com.example.chatroom.module.websocket.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class WebSocketAckStoreTest {

    private StringRedisTemplate redisTemplate;
    private HashOperations<String, Object, Object> hashOperations;
    private ZSetOperations<String, String> zSetOperations;
    private WebSocketAckStore store;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        redisTemplate = mock(StringRedisTemplate.class);
        hashOperations = mock(HashOperations.class);
        zSetOperations = mock(ZSetOperations.class);
        when(redisTemplate.opsForHash()).thenReturn(hashOperations);
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        store = new WebSocketAckStore(redisTemplate, new ObjectMapper(), Runnable::run);
        ReflectionTestUtils.setField(store, "shadowTtlSeconds", 86_400L);
    }

    @Test
    void saveShouldWritePendingHashAndDeadlineIndex() {
        WebSocketAckStore.PendingSnapshot snapshot = new WebSocketAckStore.PendingSnapshot(
                1L, 101L, 10L, "payload", 0, 1_000L);

        store.save("node-1", snapshot);

        verify(hashOperations).put(eq("ws:ack:pending:node-1"), eq("1:101"), anyString());
        verify(zSetOperations).add("ws:ack:deadline:node-1", "1:101", 1_000D);
    }

    @Test
    void loadShouldReadSnapshotsByDeadlineOrder() throws Exception {
        WebSocketAckStore.PendingSnapshot snapshot = new WebSocketAckStore.PendingSnapshot(
                1L, 101L, 10L, "payload", 2, 1_000L);
        String json = new ObjectMapper().writeValueAsString(snapshot);
        when(zSetOperations.range("ws:ack:deadline:node-1", 0, 9))
                .thenReturn(Set.of("1:101"));
        when(hashOperations.multiGet(eq("ws:ack:pending:node-1"), any()))
                .thenReturn(List.of(json));

        List<WebSocketAckStore.PendingSnapshot> result = store.loadForNode("node-1", 10);

        assertThat(result).containsExactly(snapshot);
    }
}
