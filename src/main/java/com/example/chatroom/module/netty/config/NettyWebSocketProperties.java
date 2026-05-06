package com.example.chatroom.module.netty.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Netty WebSocket 配置属性
 * 对应 application.yml 中 netty.websocket.* 配置项
 */
@Data
@Component
@ConfigurationProperties(prefix = "netty.websocket")
public class NettyWebSocketProperties {

    /** Netty 监听端口，与 Spring HTTP 端口分开 */
    private int port = 9090;

    /** Boss 线程数（接受连接），通常 1 即可 */
    private int bossThreads = 1;

    /** Worker 线程数（I/O 读写），默认 CPU*2 */
    private int workerThreads = Runtime.getRuntime().availableProcessors() * 2;

    /** WebSocket 升级路径 */
    private String path = "/ws/chat";

    /** 单帧最大字节数（默认 64KB） */
    private int maxFrameSize = 65536;

    /**
     * 读空闲超时（秒）
     * 超时后 HeartbeatHandler 向客户端发送 PING；
     * 若再次触发 allIdleSeconds 仍无响应则关闭连接
     */
    private int readerIdleSeconds = 30;

    /**
     * 全空闲超时（秒）
     * 读写都空闲超过此值，强制关闭连接
     */
    private int allIdleSeconds = 60;
}
