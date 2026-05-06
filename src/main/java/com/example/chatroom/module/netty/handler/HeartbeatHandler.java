package com.example.chatroom.module.netty.handler;

import com.example.chatroom.module.netty.manager.NettyChannelManager;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.concurrent.TimeUnit;

/**
 * 心跳 + 在线状态维护 Handler
 *
 * <p>Pipeline 中 {@link io.netty.handler.timeout.IdleStateHandler} 负责计时，
 * 本 Handler 响应其触发的 {@link IdleStateEvent}：
 *
 * <ul>
 *   <li>{@code READER_IDLE}：客户端长时间没有发送数据，服务端主动发送 WebSocket PING 帧</li>
 *   <li>{@code ALL_IDLE}：读写都空闲（客户端未回应 PING），强制关闭连接</li>
 * </ul>
 *
 * <p>同时处理客户端发来的 JSON 心跳（{@code {"type":"PING"}}）和 WebSocket 原生 PONG 帧，
 * 并在每次收到任意消息时刷新 Redis 在线状态 TTL。
 *
 * <p>此 Handler 为 {@code @Sharable}，可在多个 Channel 间共享（无实例状态）。
 */
@Slf4j
@RequiredArgsConstructor
@io.netty.channel.ChannelHandler.Sharable
public class HeartbeatHandler extends ChannelInboundHandlerAdapter {

    private static final String PING_JSON = "{\"type\":\"PING\"}";
    private static final String PONG_JSON = "{\"type\":\"PONG\"}";

    private static final String WS_ONLINE_KEY_PREFIX = "ws:online:";
    private static final String MACHINE_ID =
            System.getenv().getOrDefault("MACHINE_ID", "node-1");

    /** Redis 在线状态 TTL（秒），略大于 allIdleSeconds */
    private static final long ONLINE_TTL_SECONDS = 90;

    private final NettyChannelManager channelManager;
    private final RedisTemplate<String, Object> redisTemplate;

    // ------------------------------------------------------------------ //
    //  IdleStateEvent 处理
    // ------------------------------------------------------------------ //

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent idle) {
            if (idle.state() == IdleState.READER_IDLE) {
                // 客户端没有发数据，发一个 WebSocket PING 帧探活
                log.debug("[Netty Heartbeat] 读空闲，发送 PING, channelId={}",
                        ctx.channel().id().asShortText());
                ctx.writeAndFlush(new PingWebSocketFrame());
            } else if (idle.state() == IdleState.ALL_IDLE) {
                // 读写都空闲，客户端已失联，关闭连接
                log.info("[Netty Heartbeat] 全空闲超时，关闭连接, channelId={}",
                        ctx.channel().id().asShortText());
                ctx.close();
            }
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }

    // ------------------------------------------------------------------ //
    //  消息处理
    // ------------------------------------------------------------------ //

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof PongWebSocketFrame) {
            // 收到 WebSocket 原生 PONG，刷新 TTL，不向后传递
            refreshOnlineTtl(ctx);
            ((PongWebSocketFrame) msg).release();
            return;
        }

        if (msg instanceof TextWebSocketFrame frame) {
            String text = frame.text();
            if (PING_JSON.equals(text)) {
                // 收到 JSON 心跳，回复 PONG，刷新 TTL，不向后传递
                ctx.writeAndFlush(new TextWebSocketFrame(PONG_JSON));
                refreshOnlineTtl(ctx);
                frame.release();
                return;
            }
        }

        // 其他消息向后传递，同时刷新 TTL（有业务消息说明连接活跃）
        refreshOnlineTtl(ctx);
        ctx.fireChannelRead(msg);
    }

    // ------------------------------------------------------------------ //
    //  连接断开
    // ------------------------------------------------------------------ //

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        Long userId = ctx.channel().attr(NettyChannelManager.USER_ID_KEY).get();
        channelManager.unregister(ctx.channel());

        if (userId != null) {
            // 删除 Redis 在线状态中本机的记录
            redisTemplate.opsForHash().delete(WS_ONLINE_KEY_PREFIX + userId, MACHINE_ID);
            log.info("[Netty Heartbeat] 连接断开，清理在线状态, userId={}", userId);
        }
        ctx.fireChannelInactive();
    }

    // ------------------------------------------------------------------ //
    //  异常处理
    // ------------------------------------------------------------------ //

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("[Netty Heartbeat] 异常, channelId={}", ctx.channel().id().asShortText(), cause);
        ctx.close();
    }

    // ------------------------------------------------------------------ //
    //  私有方法
    // ------------------------------------------------------------------ //

    /**
     * 刷新 Redis 在线状态 TTL
     * 每次收到任意消息时调用，保证 Redis 中的在线记录不过期
     */
    private void refreshOnlineTtl(ChannelHandlerContext ctx) {
        Long userId = ctx.channel().attr(NettyChannelManager.USER_ID_KEY).get();
        if (userId == null) return;
        try {
            String key = WS_ONLINE_KEY_PREFIX + userId;
            redisTemplate.opsForHash().put(key, MACHINE_ID,
                    String.valueOf(System.currentTimeMillis()));
            redisTemplate.expire(key, ONLINE_TTL_SECONDS, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("[Netty Heartbeat] 刷新在线状态失败, userId={}", userId, e);
        }
    }
}
