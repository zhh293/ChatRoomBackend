package com.example.chatroom.common.config;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 线程池配置
 * 各业务线程池统一在此注册为 Spring Bean，便于监控和管理
 */
@Configuration
public class ThreadPoolConfig {

    @Value("${thread-pool.msg-persist.core-size:10}")
    private int msgPersistCoreSize;

    @Value("${thread-pool.msg-persist.max-size:50}")
    private int msgPersistMaxSize;

    @Value("${thread-pool.msg-persist.queue-capacity:1000}")
    private int msgPersistQueueCapacity;

    @Value("${thread-pool.cache-rebuild.core-size:2}")
    private int cacheRebuildCoreSize;

    @Value("${thread-pool.cache-rebuild.max-size:10}")
    private int cacheRebuildMaxSize;

    @Value("${thread-pool.cache-rebuild.queue-capacity:200}")
    private int cacheRebuildQueueCapacity;

    @Value("${thread-pool.ws-ack-store.queue-capacity:10000}")
    private int wsAckStoreQueueCapacity;

    /**
     * 消息持久化线程池（RabbitMQ 消费者内部异步落库）
     * 拒绝策略：CallerRunsPolicy，队列满时由调用线程执行，保证消息不丢
     */
    @Bean("msgPersistExecutor")
    public ThreadPoolExecutor msgPersistExecutor() {
        return new ThreadPoolExecutor(
                msgPersistCoreSize,
                msgPersistMaxSize,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(msgPersistQueueCapacity),
                new ThreadFactoryBuilder().setNameFormat("msg-persist-%d").build(),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    /**
     * 缓存重建线程池（单源异步回溯）
     * 拒绝策略：DiscardPolicy，已有线程在重建时直接丢弃，不影响正确性
     */
    @Bean("cacheRebuildExecutor")
    public ThreadPoolExecutor cacheRebuildExecutor() {
        return new ThreadPoolExecutor(
                cacheRebuildCoreSize,
                cacheRebuildMaxSize,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(cacheRebuildQueueCapacity),
                new ThreadFactoryBuilder().setNameFormat("cache-rebuild-%d").build(),
                new ThreadPoolExecutor.DiscardPolicy()
        );
    }

    /**
     * WebSocket ACK 恢复影子写入线程。
     *
     * <p>单线程保证同一 pending 的 save/remove 顺序；队列满时拒绝影子更新，
     * 不阻塞 WebSocket IO/ACK 快路径，消息仍可由数据库游标补拉恢复。</p>
     */
    @Bean(name = "wsAckPersistenceExecutor", destroyMethod = "shutdown")
    public ThreadPoolExecutor wsAckPersistenceExecutor() {
        return new ThreadPoolExecutor(
                1,
                1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(wsAckStoreQueueCapacity),
                new ThreadFactoryBuilder().setNameFormat("ws-ack-store-%d").build(),
                new ThreadPoolExecutor.AbortPolicy()
        );
    }
}
