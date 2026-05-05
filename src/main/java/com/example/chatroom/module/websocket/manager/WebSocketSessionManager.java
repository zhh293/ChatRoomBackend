package com.example.chatroom.module.websocket.manager;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 本机 WebSocket 连接管理器
 * 维护 userId → WebSocketSession 的映射
 * 多机同步通过 Redis Pub/Sub 实现（见 WebSocketPubSubListener）
 */
@Slf4j
@Component
public class WebSocketSessionManager {

    /** 本机在线连接：userId → WebSocketSession */
    private final ConcurrentHashMap<Long, WebSocketSession> localSessions = new ConcurrentHashMap<>();

    /**
     * 注册连接
     */
    public void register(Long userId, WebSocketSession session) {
        localSessions.put(userId, session);
        log.info("[WS Manager] 用户上线, userId={}, sessionId={}", userId, session.getId());
    }

    /**
     * 移除连接
     */
    public void remove(Long userId) {
        localSessions.remove(userId);
        log.info("[WS Manager] 用户下线, userId={}", userId);
    }

    /**
     * 判断用户是否在本机在线
     */
    public boolean isLocalOnline(Long userId) {
        WebSocketSession session = localSessions.get(userId);
        return session != null && session.isOpen();
    }

    /**
     * 向本机在线用户推送消息
     */
    public boolean pushToLocal(Long userId, String message) {
        WebSocketSession session = localSessions.get(userId);
        if (session == null || !session.isOpen()) {
            return false;
        }
        try {
            synchronized (session) {
                session.sendMessage(new TextMessage(message));
            }
            return true;
        } catch (IOException e) {
            log.error("[WS Manager] 推送消息失败, userId={}", userId, e);
            return false;
        }
    }

    /**
     * 获取本机在线用户数
     */
    public int getLocalOnlineCount() {
        return (int) localSessions.values().stream()
                .filter(WebSocketSession::isOpen)
                .count();
    }
}
