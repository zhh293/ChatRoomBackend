package com.example.chatroom.cache.bloom;

import com.example.chatroom.module.session.mapper.SessionMapper;
import com.example.chatroom.module.user.mapper.UserMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RedissonClient;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 布隆过滤器初始化器（防缓存穿透）
 *
 * 策略：
 * - 应用启动完成后（ApplicationReadyEvent）触发，不阻塞启动流程
 * - 后台异步线程分批游标加载历史 ID（避免 OOM）
 * - AtomicBoolean 标志位控制就绪状态，未就绪时降级走缓存/DB
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class BloomFilterInitializer implements ApplicationListener<ApplicationReadyEvent> {

    private static final int BATCH_SIZE = 1000;

    private final RedissonClient redissonClient;
    private final UserMapper userMapper;
    private final SessionMapper sessionMapper;

    /** 布隆过滤器就绪标志，未就绪时所有查询降级走 DB */
    public static final AtomicBoolean userBloomReady = new AtomicBoolean(false);
    public static final AtomicBoolean sessionBloomReady = new AtomicBoolean(false);

    /** 专用初始化线程，与业务线程隔离 */
    private final ExecutorService initExecutor = Executors.newFixedThreadPool(2,
            new ThreadFactoryBuilder().setNameFormat("bloom-init-%d").build());

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        initExecutor.submit(this::initUserBloom);
        initExecutor.submit(this::initSessionBloom);
    }

    private void initUserBloom() {
        try {
            RBloomFilter<Long> bloomFilter = redissonClient.getBloomFilter("bloom:user:ids");
            if (!bloomFilter.isExists()) {
                // 预期容量100万，误判率0.01%
                bloomFilter.tryInit(1_000_000L, 0.0001);
            }

            Long maxId = userMapper.selectMaxId();
            if (maxId == null) {
                userBloomReady.set(true);
                return;
            }

            // 游标分批加载，避免大偏移量分页性能退化
            long lastId = 0L;
            long totalLoaded = 0;
            while (lastId <= maxId) {
                List<Long> batch = userMapper.selectIdsBatch(lastId, BATCH_SIZE);
                if (batch.isEmpty()) break;
                batch.forEach(bloomFilter::add);
                lastId = batch.get(batch.size() - 1);
                totalLoaded += batch.size();
                log.info("[BloomInit] 用户布隆过滤器加载进度: {}/{}", totalLoaded, maxId);
            }

            userBloomReady.set(true);
            log.info("[BloomInit] 用户布隆过滤器初始化完成，共加载 {} 条", totalLoaded);

        } catch (Exception e) {
            log.error("[BloomInit] 用户布隆过滤器初始化失败，降级走DB", e);
        }
    }

    private void initSessionBloom() {
        try {
            RBloomFilter<Long> bloomFilter = redissonClient.getBloomFilter("bloom:session:ids");
            if (!bloomFilter.isExists()) {
                bloomFilter.tryInit(500_000L, 0.0001);
            }

            Long maxId = sessionMapper.selectMaxId();
            if (maxId == null) {
                sessionBloomReady.set(true);
                return;
            }

            long lastId = 0L;
            long totalLoaded = 0;
            while (lastId <= maxId) {
                List<Long> batch = sessionMapper.selectIdsBatch(lastId, BATCH_SIZE);
                if (batch.isEmpty()) break;
                batch.forEach(bloomFilter::add);
                lastId = batch.get(batch.size() - 1);
                totalLoaded += batch.size();
            }

            sessionBloomReady.set(true);
            log.info("[BloomInit] 会话布隆过滤器初始化完成，共加载 {} 条", totalLoaded);

        } catch (Exception e) {
            log.error("[BloomInit] 会话布隆过滤器初始化失败，降级走DB", e);
        }
    }
}
