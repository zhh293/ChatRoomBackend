package com.example.chatroom.module.netty.config;

import com.example.chatroom.common.util.JwtUtil;
import com.example.chatroom.module.netty.handler.HeartbeatHandler;
import com.example.chatroom.module.netty.handler.TokenAuthHandler;
import com.example.chatroom.module.netty.handler.WebSocketFrameHandler;
import com.example.chatroom.module.netty.manager.NettyChannelManager;
import com.example.chatroom.module.websocket.service.WebSocketAckManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolConfig;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.handler.timeout.IdleStateHandler;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.concurrent.TimeUnit;

/**
 * Netty WebSocket 服务器
 *
 * <p>Pipeline 结构（从头到尾）：
 * <pre>
 *  ┌─────────────────────────────────────────────────────────────────┐
 *  │  LoggingHandler          （DEBUG 级别，生产可关闭）              │
 *  │  HttpServerCodec         （HTTP 编解码）                         │
 *  │  HttpObjectAggregator    （聚合 HTTP 分片，最大 64KB）            │
 *  │  ChunkedWriteHandler     （支持大文件分块写出，WebSocket 握手需要）│
 *  │  IdleStateHandler        （读空闲 30s / 全空闲 60s）             │
 *  │  TokenAuthHandler        （JWT 鉴权 + WebSocket 握手，一次性）    │
 *  │  HeartbeatHandler        （心跳响应 + 在线状态维护 + 断连清理）   │
 *  │  WebSocketFrameHandler   （业务帧处理）                          │
 *  └─────────────────────────────────────────────────────────────────┘
 * </pre>
 *
 * <p>启动流程：
 * <ol>
 *   <li>Spring 容器初始化完成后，{@link #start()} 在独立线程中启动 Netty</li>
 *   <li>Spring 容器关闭时，{@link #stop()} 优雅关闭所有连接和线程组</li>
 * </ol>
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class NettyWebSocketServer {

    private final NettyWebSocketProperties props;
    private final JwtUtil jwtUtil;
    private final NettyChannelManager channelManager;
    private final RedisTemplate<String, Object> redisTemplate;
    private final ObjectMapper objectMapper;
    private final WebSocketAckManager ackManager;

    private NioEventLoopGroup bossGroup;
    private NioEventLoopGroup workerGroup;
    private Channel serverChannel;

    /** 共享的 Sharable Handler（无状态，可复用） */
    private HeartbeatHandler heartbeatHandler;
    private WebSocketFrameHandler frameHandler;

    @PostConstruct
    public void start() {
        heartbeatHandler = new HeartbeatHandler(channelManager, redisTemplate);
        frameHandler = new WebSocketFrameHandler(channelManager, objectMapper, ackManager);

        bossGroup = new NioEventLoopGroup(props.getBossThreads());
        workerGroup = new NioEventLoopGroup(props.getWorkerThreads());

        // 在独立线程中启动，避免阻塞 Spring 主线程
        Thread serverThread = new Thread(() -> {
            try {
                ServerBootstrap bootstrap = new ServerBootstrap();
                bootstrap.group(bossGroup, workerGroup)
                         .channel(NioServerSocketChannel.class)
                         .handler(new LoggingHandler(LogLevel.INFO))
                         .childOption(ChannelOption.TCP_NODELAY, true)
                         .childOption(ChannelOption.SO_KEEPALIVE, true)
                         .childHandler(new ChannelInitializer<SocketChannel>() {
                             @Override
                             protected void initChannel(SocketChannel ch) {
                                 buildPipeline(ch.pipeline());
                             }
                         });

                ChannelFuture future = bootstrap.bind(props.getPort()).sync();
                serverChannel = future.channel();
                log.info("[Netty] WebSocket 服务器启动成功，监听端口: {}", props.getPort());
                serverChannel.closeFuture().sync();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("[Netty] 服务器线程被中断");
            } catch (Exception e) {
                log.error("[Netty] 服务器启动失败", e);
            }
        }, "netty-ws-server");
        serverThread.setDaemon(true);
        serverThread.start();
    }

    @PreDestroy
    public void stop() {
        log.info("[Netty] 开始优雅关闭...");
        // 1. 关闭所有 WebSocket 连接
        channelManager.closeAll();
        // 2. 关闭服务端 Channel
        if (serverChannel != null) {
            serverChannel.close().syncUninterruptibly();
        }
        // 3. 关闭线程组（quietPeriod=2s，timeout=5s）
        if (bossGroup != null) {
            bossGroup.shutdownGracefully(2, 5, TimeUnit.SECONDS).syncUninterruptibly();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully(2, 5, TimeUnit.SECONDS).syncUninterruptibly();
        }
        log.info("[Netty] 优雅关闭完成");
    }

    // ------------------------------------------------------------------ //
    //  Pipeline 构建
    // ------------------------------------------------------------------ //

    private void buildPipeline(ChannelPipeline pipeline) {
        // HTTP 编解码
        pipeline.addLast("http-codec", new HttpServerCodec());

        // 聚合 HTTP 分片（WebSocket 握手请求需要完整的 FullHttpRequest）
        pipeline.addLast("http-aggregator",
                new HttpObjectAggregator(props.getMaxFrameSize()));

        // 支持大文件分块写出（WebSocket 握手响应可能较大）
        pipeline.addLast("chunked-writer", new ChunkedWriteHandler());

        // 空闲检测：读空闲 readerIdleSeconds，写空闲 0（不检测），全空闲 allIdleSeconds
        pipeline.addLast("idle-state",
                new IdleStateHandler(
                        props.getReaderIdleSeconds(),
                        0,
                        props.getAllIdleSeconds(),
                        TimeUnit.SECONDS
                ));

        // JWT 鉴权 + WebSocket 握手（每个连接独立实例，握手完成后自动移除）
        pipeline.addLast("token-auth",
                new TokenAuthHandler(jwtUtil, channelManager, props.getPath()));

        // 心跳响应 + 在线状态维护（Sharable）
        pipeline.addLast("heartbeat", heartbeatHandler);

        // 业务帧处理（Sharable）
        pipeline.addLast("ws-frame", frameHandler);
    }
}
