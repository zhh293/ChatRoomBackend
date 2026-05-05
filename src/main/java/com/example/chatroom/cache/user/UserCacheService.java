package com.example.chatroom.cache.user;

import com.example.chatroom.cache.bloom.BloomFilterInitializer;
import com.example.chatroom.cache.helper.CacheTtlHelper;
import com.example.chatroom.common.constant.RedisKeyConst;
import com.example.chatroom.module.user.domain.entity.User;
import com.example.chatroom.module.user.mapper.UserMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 用户信息缓存服务
 *
 * 三大防护：
 * 1. 缓存穿透：布隆过滤器前置拦截 + 空值缓存兜底
 * 2. 缓存雪崩：随机 TTL（CacheTtlHelper）
 * 3. 缓存击穿：外围 SET NX + 单源异步回溯 + 普通锁+看门狗 + Double Check
 *
 * 主动写入：
 * - 登录/注册成功后调用 put() 直接写缓存，避免首次查询穿透
 * - 同步调用 addToBloom() 将新用户 ID 加入布隆过滤器
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class UserCacheService {

    private static final String NULL_VALUE = RedisKeyConst.NULL_VALUE;
    private static final int SPIN_TIMES = 3;
    private static final long SPIN_INTERVAL_MS = 50;

    private final RedissonClient redissonClient;
    private final RedisTemplate<String, Object> redisTemplate;
    private final UserMapper userMapper;
    private final CacheTtlHelper ttlHelper;

    /** 专用缓存重建线程池，与业务线程池隔离 */
    private final ThreadPoolExecutor rebuildExecutor = new ThreadPoolExecutor(
            2, 10, 60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(200),
            new ThreadFactoryBuilder().setNameFormat("cache-rebuild-%d").build(),
            new ThreadPoolExecutor.DiscardPolicy()
    );

    // ─────────────────────────────────────────────────────────────────────────
    // 主动写入（登录/注册时调用）
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * 主动写入用户缓存（登录/注册成功后调用）
     * 同时将 userId 加入布隆过滤器，保证新用户不被误拦截
     *
     * @param user 完整用户实体
     */
    public void put(User user) {
        if (user == null || user.getId() == null) return;

        String cacheKey = RedisKeyConst.USER_INFO + user.getId();
        redisTemplate.opsForValue().set(cacheKey, user, ttlHelper.userInfoTtl(), TimeUnit.SECONDS);

        // 同步加入布隆过滤器（布隆过滤器就绪才操作，未就绪时启动初始化会补全）
        addToBloom(user.getId());

        log.debug("[UserCache] 主动写入用户缓存: userId={}", user.getId());
    }

    /**
     * 将 userId 加入布隆过滤器
     * 幂等操作，重复添加无副作用
     */
    public void addToBloom(Long userId) {
        try {
            RBloomFilter<Long> bloomFilter = redissonClient.getBloomFilter(RedisKeyConst.BLOOM_USER_IDS);
            // 布隆过滤器未初始化时先 tryInit（容量/误判率与 BloomFilterInitializer 保持一致）
            if (!bloomFilter.isExists()) {
                bloomFilter.tryInit(1_000_000L, 0.0001);
            }
            bloomFilter.add(userId);
        } catch (Exception e) {
            // 布隆过滤器写入失败不影响主流程，降级走 DB 即可
            log.warn("[UserCache] 布隆过滤器写入失败, userId={}: {}", userId, e.getMessage());
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 读取（含三大防护）
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * 获取用户信息（含三大缓存防护）
     */
    public User getUserById(Long userId) {
        // ① 布隆过滤器前置拦截（就绪才拦截，未就绪降级）
        if (BloomFilterInitializer.userBloomReady.get()) {
            RBloomFilter<Long> bloomFilter = redissonClient.getBloomFilter(RedisKeyConst.BLOOM_USER_IDS);
            if (!bloomFilter.contains(userId)) {
                return null; // 100% 不存在
            }
        }

        String cacheKey = RedisKeyConst.USER_INFO + userId;
        Object cached = redisTemplate.opsForValue().get(cacheKey);

        // ② 空值缓存兜底
        if (NULL_VALUE.equals(cached)) return null;
        if (cached != null) return (User) cached;

        // ③ 缓存未命中，进入击穿保护
        return handleCacheMiss(userId, cacheKey);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 失效
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * 删除用户缓存（更新用户信息后调用）
     * 注意：布隆过滤器不支持删除，无需处理
     */
    public void evict(Long userId) {
        redisTemplate.delete(RedisKeyConst.USER_INFO + userId);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 批量读取
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * 批量获取用户信息（Cache-Aside）
     *
     * <pre>
     * 1. Pipeline MGET 一次网络往返拿回所有缓存结果
     * 2. 命中 → 直接收集；未命中 → 记录 userId
     * 3. 未命中的 userId 一次 IN 查询回源 DB
     * 4. 回源结果异步写回缓存（复用 put 方法）
     * 5. 合并返回，过滤掉空值标记
     * </pre>
     *
     * @param userIds 用户 ID 列表
     * @return 用户列表，顺序与入参一致，不存在的用户自动跳过
     */
    public List<User> batchGetUserByIds(List<Long> userIds) {
        if (userIds == null || userIds.isEmpty()) return Collections.emptyList();

        // ① 构造所有 cacheKey
        List<String> keys = userIds.stream()
                .map(id -> RedisKeyConst.USER_INFO + id)
                .collect(java.util.stream.Collectors.toList());

        // ② Pipeline MGET，一次网络往返
        List<Object> cachedValues = redisTemplate.executePipelined(
                (org.springframework.data.redis.core.RedisCallback<Object>) conn -> {
                    for (String key : keys) {
                        conn.stringCommands().get(key.getBytes());
                    }
                    return null;
                });

        // ③ 分拣：命中 vs 未命中
        List<User> result = new ArrayList<>(userIds.size());
        List<Long> missIds = new ArrayList<>();

        for (int i = 0; i < userIds.size(); i++) {
            Object val = cachedValues.get(i);
            if (val == null) {
                // 缓存不存在，需要回源
                missIds.add(userIds.get(i));
            } else if (!NULL_VALUE.equals(val)) {
                // 正常命中
                result.add((User) val);
            }
            // NULL_VALUE 说明该用户不存在，直接跳过
        }

        // ④ 未命中的批量回源 DB（一次 IN 查询）
        if (!missIds.isEmpty()) {
            List<User> dbUsers = userMapper.selectBatchIds(missIds);

            // 用 Map 方便后续写回缓存时判断哪些 id 真的不存在
            Map<Long, User> dbMap = new LinkedHashMap<>();
            for (User u : dbUsers) {
                dbMap.put(u.getId(), u);
            }

            // 收集回源结果
            result.addAll(dbUsers);

            // ⑤ 异步写回缓存（命中的写用户对象，不存在的写空值标记）
            rebuildExecutor.execute(() -> {
                for (Long missId : missIds) {
                    User u = dbMap.get(missId);
                    if (u != null) {
                        put(u);
                    } else {
                        // 写空值标记防穿透，TTL=5min
                        String cacheKey = RedisKeyConst.USER_INFO + missId;
                        redisTemplate.opsForValue().set(cacheKey, NULL_VALUE, 5, TimeUnit.MINUTES);
                    }
                }
            });
        }

        return result;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 缓存击穿保护（内部）
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * 缓存击穿保护入口
     * 核心：抢到锁 → 异步重建 → 当前线程立即返回 null，保证响应速度
     */
    private User handleCacheMiss(Long userId, String cacheKey) {
        String lockKey = RedisKeyConst.CACHE_REBUILD_LOCK + cacheKey;
        RLock outerLock = redissonClient.getLock(lockKey + ":outer");

        boolean outerAcquired = false;
        try {
            outerAcquired = outerLock.tryLock(0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (outerAcquired) {
            // 抢到锁：立即提交异步重建任务，当前线程直接返回 null
            rebuildExecutor.submit(() -> doAsyncRebuild(userId, cacheKey, lockKey, outerLock));
            return null;
        }

        // 抢锁失败：自旋等待异步重建完成
        for (int i = 0; i < SPIN_TIMES; i++) {
            try {
                Thread.sleep(SPIN_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            Object cached = redisTemplate.opsForValue().get(cacheKey);
            if (cached != null) {
                return NULL_VALUE.equals(cached) ? null : (User) cached;
            }
        }
        return null;
    }

    /**
     * 异步重建缓存（在 rebuildExecutor 线程池中执行）
     */
    private void doAsyncRebuild(Long userId, String cacheKey, String lockKey, RLock outerLock) {
        try {
            // Double Check
            Object cached = redisTemplate.opsForValue().get(cacheKey);
            if (cached != null) return;

            // 普通 Redisson 锁 + 看门狗（leaseTime=-1 启用看门狗）
            RLock rebuildLock = redissonClient.getLock(lockKey + ":rebuild");
            boolean rebuildAcquired = false;
            try {
                rebuildAcquired = rebuildLock.tryLock(0, -1, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            if (!rebuildAcquired) return;

            try {
                User user = userMapper.selectById(userId);
                if (user == null) {
                    redisTemplate.opsForValue().set(cacheKey, NULL_VALUE, 5, TimeUnit.MINUTES);
                } else {
                    redisTemplate.opsForValue().set(cacheKey, user,
                            ttlHelper.userInfoTtl(), TimeUnit.SECONDS);
                }
            } finally {
                if (rebuildLock.isHeldByCurrentThread()) rebuildLock.unlock();
            }

        } catch (Exception e) {
            log.error("[CacheRebuild] 异步重建用户缓存失败, userId={}", userId, e);
        } finally {
            if (outerLock.isHeldByCurrentThread()) outerLock.unlock();
        }
    }
}
