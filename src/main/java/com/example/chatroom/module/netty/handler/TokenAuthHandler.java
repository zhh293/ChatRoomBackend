package com.example.chatroom.module.netty.handler;

import com.example.chatroom.common.util.JwtUtil;
import com.example.chatroom.module.netty.manager.NettyChannelManager;
import io.jsonwebtoken.Claims;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.websocketx.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * WebSocket 握手鉴权 Handler
 *
 * <p>职责：
 * <ol>
 *   <li>拦截 HTTP Upgrade 请求，从 URL 参数中提取 {@code token}</li>
 *   <li>调用 {@link JwtUtil} 验证 Token，解析出 userId</li>
 *   <li>鉴权成功：完成 WebSocket 握手，将 userId 绑定到 Channel，注册到 {@link NettyChannelManager}</li>
 *   <li>鉴权失败：返回 401 并关闭连接</li>
 *   <li>握手完成后将自身从 Pipeline 移除，后续帧不再经过此 Handler</li>
 * </ol>
 *
 * <p>注意：此 Handler 标注了 {@code @Component} 但 <b>不能</b> 直接注入到 Pipeline，
 * 因为 Netty 要求每个连接使用独立的 Handler 实例（非 {@code @Sharable} 的 Handler 不可共享）。
 * 实际使用时通过 {@code new TokenAuthHandler(jwtUtil, channelManager, path)} 创建实例。
 */
@Slf4j
@RequiredArgsConstructor
public class TokenAuthHandler extends ChannelInboundHandlerAdapter {

    private final JwtUtil jwtUtil;
    private final NettyChannelManager channelManager;
    private final String wsPath;

    /** WebSocket 握手处理器，握手完成后保存，用于后续帧解码 */
    private WebSocketServerHandshaker handshaker;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof FullHttpRequest request) {
            handleHttpUpgrade(ctx, request);
        } else if (msg instanceof WebSocketFrame frame) {
            // 握手完成后的帧直接向后传递
            ctx.fireChannelRead(frame);
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    private void handleHttpUpgrade(ChannelHandlerContext ctx, FullHttpRequest request) {
        // 1. 解析 token
        String token = extractToken(request.uri());
        if (token == null) {
            log.warn("[Netty Auth] 缺少 token, remoteAddr={}", ctx.channel().remoteAddress());
            rejectAndClose(ctx, WebSocketCloseStatus.POLICY_VIOLATION);
            return;
        }

        // 2. 验证 token
        Long userId;
        try {
            Claims claims = jwtUtil.parseToken(token);
            userId = jwtUtil.getUserId(claims);
        } catch (Exception e) {
            log.warn("[Netty Auth] Token 验证失败: {}", e.getMessage());
            rejectAndClose(ctx, WebSocketCloseStatus.POLICY_VIOLATION);
            return;
        }

        // 3. 完成 WebSocket 握手
        WebSocketServerHandshakerFactory factory = new WebSocketServerHandshakerFactory(
                "ws://" + request.headers().get("Host") + wsPath,
                null,
                true
        );
        handshaker = factory.newHandshaker(request);
        if (handshaker == null) {
            WebSocketServerHandshakerFactory.sendUnsupportedVersionResponse(ctx.channel());
            return;
        }
        handshaker.handshake(ctx.channel(), request).addListener(future -> {
            if (future.isSuccess()) {
                // 4. 注册到 ChannelManager
                channelManager.register(userId, ctx.channel());
                log.info("[Netty Auth] 握手成功, userId={}, channelId={}",
                        userId, ctx.channel().id().asShortText());
                // 5. 握手完成，将自身从 Pipeline 移除
                ctx.pipeline().remove(this);
            } else {
                log.error("[Netty Auth] 握手失败", future.cause());
                ctx.close();
            }
        });
    }

    private void rejectAndClose(ChannelHandlerContext ctx, WebSocketCloseStatus status) {
        ctx.channel().close();
    }

    /**
     * 从 URI 的 query string 中提取 token 参数
     */
    private String extractToken(String rawUri) {
        try {
            URI uri = new URI(rawUri);
            String query = uri.getRawQuery();
            if (query == null) return null;
            Map<String, String> params = parseQuery(query);
            return params.get("token");
        } catch (Exception e) {
            return null;
        }
    }

    private Map<String, String> parseQuery(String query) {
        Map<String, String> map = new HashMap<>();
        for (String pair : query.split("&")) {
            String[] kv = pair.split("=", 2);
            if (kv.length == 2) {
                map.put(
                    URLDecoder.decode(kv[0], StandardCharsets.UTF_8),
                    URLDecoder.decode(kv[1], StandardCharsets.UTF_8)
                );
            }
        }
        return map;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("[Netty Auth] 异常, channelId={}", ctx.channel().id().asShortText(), cause);
        ctx.close();
    }
}
