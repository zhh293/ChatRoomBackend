package com.example.chatroom.module.netty.manager;

import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.GlobalEventExecutor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Netty Channel 连接管理器
 *
 * <p>设计要点：
 * <ul>
 *   <li>支持同一用户多端登录：userId → Set&lt;ChannelId&gt;</li>
 *   <li>Channel 上通过 AttributeKey 绑定 userId，断连时可反查</li>
 *   <li>ChannelGroup 统一管理所有活跃连接，JVM 退出时可批量关闭</li>
 *   <li>所有操作均为 ConcurrentHashMap / CopyOnWriteArraySet，线程安全</li>
 * </ul>
 */
@Slf4j
@Component
public class NettyChannelManager {

    /** Channel 上绑定 userId 的 AttributeKey */
    public static final AttributeKey<Long> USER_ID_KEY = AttributeKey.valueOf("userId");

    /** 全局 ChannelGroup，用于广播和优雅关闭 */
    private final ChannelGroup allChannels =
            new DefaultChannelGroup("all-ws-channels", GlobalEventExecutor.INSTANCE);

    /**
     * userId → 该用户在本机的所有 Channel ID 集合（多端登录）
     * 使用 CopyOnWriteArraySet 保证并发安全，且遍历时不需要加锁
     */
    private final ConcurrentHashMap<Long, CopyOnWriteArraySet<ChannelId>> userChannels =
            new ConcurrentHashMap<>();

    /**
     * ChannelId → Channel 的快速反查表
     * Netty 的 ChannelGroup 内部也维护了这个映射，但对外 API 不够方便，这里单独维护
     */
    private final ConcurrentHashMap<ChannelId, Channel> channelMap = new ConcurrentHashMap<>();

    // ------------------------------------------------------------------ //
    //  注册 / 注销
    // ------------------------------------------------------------------ //

    /**
     * 注册连接
     * 在 TokenAuthHandler 鉴权成功后调用
     */
    public void register(Long userId, Channel channel) {
        channel.attr(USER_ID_KEY).set(userId);
        allChannels.add(channel);
        channelMap.put(channel.id(), channel);
        userChannels.computeIfAbsent(userId, k -> new CopyOnWriteArraySet<>())
                    .add(channel.id());
        log.info("[Netty Manager] 用户上线, userId={}, channelId={}, 当前端数={}",
                userId, channel.id().asShortText(),
                userChannels.get(userId).size());
    }

    /**
     * 注销连接
     * 在 channelInactive 时调用
     */
    public void unregister(Channel channel) {
        Long userId = channel.attr(USER_ID_KEY).get();
        allChannels.remove(channel);
        channelMap.remove(channel.id());
        if (userId != null) {
            CopyOnWriteArraySet<ChannelId> ids = userChannels.get(userId);
            if (ids != null) {
                ids.remove(channel.id());
                if (ids.isEmpty()) {
                    userChannels.remove(userId);
                    log.info("[Netty Manager] 用户全部下线, userId={}", userId);
                } else {
                    log.info("[Netty Manager] 用户断开一个端, userId={}, 剩余端数={}", userId, ids.size());
                }
            }
        }
    }

    // ------------------------------------------------------------------ //
    //  查询
    // ------------------------------------------------------------------ //

    /**
     * 判断用户是否在本机有活跃连接
     */
    public boolean isLocalOnline(Long userId) {
        CopyOnWriteArraySet<ChannelId> ids = userChannels.get(userId);
        if (ids == null || ids.isEmpty()) return false;
        // 只要有一个 Channel 是 active 的就算在线
        return ids.stream()
                  .map(channelMap::get)
                  .anyMatch(ch -> ch != null && ch.isActive());
    }

    /**
     * 获取用户在本机的所有活跃 Channel
     */
    public Set<ChannelId> getChannelIds(Long userId) {
        CopyOnWriteArraySet<ChannelId> ids = userChannels.get(userId);
        return ids != null ? ids : Set.of();
    }

    /**
     * 本机在线用户数（去重）
     */
    public int getLocalOnlineUserCount() {
        return userChannels.size();
    }

    /**
     * 本机活跃连接总数（含多端）
     */
    public int getLocalOnlineChannelCount() {
        return allChannels.size();
    }

    // ------------------------------------------------------------------ //
    //  推送
    // ------------------------------------------------------------------ //

    /**
     * 向用户的所有在线端推送消息（多端同步）
     *
     * @return 成功推送的端数
     */
    public int pushToUser(Long userId, String json) {
        CopyOnWriteArraySet<ChannelId> ids = userChannels.get(userId);
        if (ids == null || ids.isEmpty()) return 0;

        int count = 0;
        for (ChannelId id : ids) {
            Channel ch = channelMap.get(id);
            if (ch != null && ch.isActive()) {
                // writeAndFlush 是线程安全的，Netty 内部会将任务提交到 Channel 绑定的 EventLoop
                ch.writeAndFlush(new TextWebSocketFrame(json));
                count++;
            }
        }
        return count;
    }

    /**
     * 向全部在线连接广播（如系统公告）
     */
    public void broadcast(String json) {
        allChannels.writeAndFlush(new TextWebSocketFrame(json));
    }

    /**
     * 优雅关闭所有连接（应用停止时调用）
     */
    public void closeAll() {
        allChannels.close().syncUninterruptibly();
        log.info("[Netty Manager] 所有连接已关闭");
    }
}
