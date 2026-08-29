package com.example.chatroom.module.netty.handler;

import com.example.chatroom.module.netty.manager.NettyChannelManager;
import com.example.chatroom.module.websocket.service.WebSocketAckManager;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * WebSocket 业务帧处理 Handler
 *
 * <p>职责：
 * <ul>
 *   <li>处理 {@link TextWebSocketFrame}：解析上行业务消息（预留扩展点）</li>
 *   <li>处理 {@link CloseWebSocketFrame}：优雅关闭握手</li>
 *   <li>处理 {@link BinaryWebSocketFrame}：暂不支持，直接关闭</li>
 * </ul>
 *
 * <p>心跳帧（PING/PONG）已在 {@link HeartbeatHandler} 中消费，不会到达此 Handler。
 *
 * <p>标注 {@code @Sharable}，可在多个 Channel 间共享（无实例状态）。
 */
@Slf4j
@ChannelHandler.Sharable
@RequiredArgsConstructor
public class WebSocketFrameHandler extends SimpleChannelInboundHandler<WebSocketFrame> {

    private final NettyChannelManager channelManager;
    private final ObjectMapper objectMapper;
    private final WebSocketAckManager ackManager;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) {
        if (frame instanceof TextWebSocketFrame textFrame) {
            handleTextFrame(ctx, textFrame);
        } else if (frame instanceof CloseWebSocketFrame closeFrame) {
            handleCloseFrame(ctx, closeFrame);
        } else if (frame instanceof BinaryWebSocketFrame) {
            log.warn("[Netty Frame] 不支持 Binary 帧, channelId={}", ctx.channel().id().asShortText());
            ctx.close();
        }
        // PingWebSocketFrame / PongWebSocketFrame 已被 HeartbeatHandler 消费，不会到达这里
    }

    // ------------------------------------------------------------------ //
    //  Text 帧处理
    // ------------------------------------------------------------------ //

    private void handleTextFrame(ChannelHandlerContext ctx, TextWebSocketFrame frame) {
        String payload = frame.text();
        Long userId = ctx.channel().attr(NettyChannelManager.USER_ID_KEY).get();

        log.debug("[Netty Frame] 收到上行消息, userId={}, payload={}", userId, payload);

        try {
            JsonNode root = objectMapper.readTree(payload);
            String type = root.path("type").asText();
            if ("MESSAGE_ACK".equals(type)) {
                long msgId = root.path("msgId").asLong(0L);
                long sessionId = root.path("sessionId").asLong(0L);
                if (userId == null || msgId <= 0 || sessionId <= 0) {
                    log.warn("[Netty Frame] 非法 MESSAGE_ACK, userId={}, payload={}", userId, payload);
                    return;
                }
                ackManager.acknowledge(userId, msgId, sessionId);
                return;
            }
            if ("MESSAGE_ACK_BATCH".equals(type)) {
                JsonNode items = root.path("items");
                if (userId == null || !items.isArray()) {
                    log.warn("[Netty Frame] 非法 MESSAGE_ACK_BATCH, userId={}, payload={}", userId, payload);
                    return;
                }
                List<WebSocketAckManager.AckItem> ackItems = new ArrayList<>();
                for (JsonNode item : items) {
                    ackItems.add(new WebSocketAckManager.AckItem(
                            item.path("msgId").asLong(0L),
                            item.path("sessionId").asLong(0L)));
                }
                ackManager.acknowledgeBatch(userId, ackItems);
                return;
            }
        } catch (Exception e) {
            log.warn("[Netty Frame] 无法解析上行消息, userId={}, payload={}", userId, payload, e);
            return;
        }

        /*
         * TODO: 根据 type 字段路由到具体业务处理器
         *
         * 当前 IM 架构中，客户端发送消息走 HTTP POST /api/messages/send，
         * WebSocket 仅作为服务端推送通道，上行消息目前只有心跳。
         *
         * 如果后续需要支持纯 WebSocket 发消息，在此处解析 JSON 并调用 MessageService。
         *
         * 示例：
         *   JsonNode node = objectMapper.readTree(payload);
         *   String type = node.get("type").asText();
         *   switch (type) {
         *       case "SEND_MSG" -> messageService.sendViaWs(userId, node);
         *       case "READ_ACK" -> sessionService.ackRead(userId, node);
         *       default -> log.warn("未知消息类型: {}", type);
         *   }
         */
    }

    // ------------------------------------------------------------------ //
    //  Close 帧处理
    // ------------------------------------------------------------------ //

    private void handleCloseFrame(ChannelHandlerContext ctx, CloseWebSocketFrame frame) {
        Long userId = ctx.channel().attr(NettyChannelManager.USER_ID_KEY).get();
        log.info("[Netty Frame] 收到 Close 帧, userId={}, status={}, reason={}",
                userId, frame.statusCode(), frame.reasonText());
        // 回送 Close 帧完成握手，然后关闭 Channel
        // channelInactive 会在 close 后触发，HeartbeatHandler 负责清理状态
        ctx.writeAndFlush(frame.retain()).addListener(f -> ctx.close());
    }

    // ------------------------------------------------------------------ //
    //  异常处理
    // ------------------------------------------------------------------ //

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        Long userId = ctx.channel().attr(NettyChannelManager.USER_ID_KEY).get();
        log.error("[Netty Frame] 异常, userId={}, channelId={}",
                userId, ctx.channel().id().asShortText(), cause);
        ctx.close();
    }
}
