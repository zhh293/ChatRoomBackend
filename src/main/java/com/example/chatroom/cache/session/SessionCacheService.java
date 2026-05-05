package com.example.chatroom.cache.session;

import com.example.chatroom.cache.bloom.BloomFilterInitializer;
import com.example.chatroom.cache.helper.CacheTtlHelper;
import com.example.chatroom.common.constant.RedisKeyConst;
import com.example.chatroom.module.session.domain.entity.Session;
import com.example.chatroom.module.session.mapper.SessionMapper;
import com.example.chatroom.module.session.mapper.SessionMemberMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 会话信息缓存服务
 *
 * <h3>会话基本信息缓存（session:info:{sessionId}）</h3>
 * 与 UserCacheService 完全相同的单源异步回溯方案，防击穿/穿透/雪崩。
 *
 * <h3>群成员列表缓存（session:members:{sessionId}）</h3>
 * <pre>
 * 数据结构：Redis Set，value = userId 字符串
 * TTL：7天 + 随机抖动（CacheTtlHelper.sessionMembersTtl）
 *
 * 防穿透：
 *   - 布隆过滤器前置拦截非法 sessionId
 *   - 空值标记（NULL_VALUE）缓存不存在的 session，TTL=5min
 *
 * 防雪崩：
 *   - TTL 加随机抖动，避免大量 key 同时过期
 *
 * 防击穿（单源异步回溯）：
 *   - 抢到锁 → 异步重建 Set → 当前线程直接查 DB 返回（不等待重建）
 *   - 没抢到锁 → 直接查 DB 返回（不等待，不自旋）
 *   - 群成员列表数据量可能较大，不适合自旋等待
 *
 * 主动失效：
 *   - 成员加入/退出/被踢时调用 evictMembers(sessionId)
 *   - 下次请求触发重建
 * </pre>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SessionCacheService {

    private static final String NULL_VALUE = RedisKeyConst.NULL_VALUE;
    private static final int SPIN_TIMES = 3;
    private static final long SPIN_INTERVAL_MS = 50;

    /**
     * Lua：key 存在时才 SADD，原子操作，省掉 EXISTS + SADD 两次往返
     * KEYS[1] = setKey, ARGV[1] = member
     * 返回 1 表示 SADD 成功，0 表示 key 不存在跳过
     */
    private static final RedisScript<Long> SADD_IF_EXISTS_SCRIPT = new DefaultRedisScript<>(
            "if redis.call('EXISTS', KEYS[1]) == 1 then " +
            "  return redis.call('SADD', KEYS[1], ARGV[1]) " +
            "else " +
            "  return 0 " +
            "end",
            Long.class
    );

    /**
     * Lua：key 存在时才 SREM，原子操作，省掉 EXISTS + SREM 两次往返
     * KEYS[1] = setKey, ARGV[1] = member
     * 返回 1 表示 SREM 成功，0 表示 key 不存在跳过
     */
    private static final RedisScript<Long> SREM_IF_EXISTS_SCRIPT = new DefaultRedisScript<>(
            "if redis.call('EXISTS', KEYS[1]) == 1 then " +
            "  return redis.call('SREM', KEYS[1], ARGV[1]) " +
            "else " +
            "  return 0 " +
            "end",
            Long.class
    );

    private final RedissonClient       redissonClient;
    private final RedisTemplate<String, Object> redisTemplate;
    private final StringRedisTemplate  stringRedisTemplate;
    private final SessionMapper        sessionMapper;
    private final SessionMemberMapper  sessionMemberMapper;
    private final CacheTtlHelper       ttlHelper;
    private final ThreadPoolExecutor   cacheRebuildExecutor;

    // =========================================================================
    // 会话基本信息缓存
    // =========================================================================

    /**
     * 获取会话信息（含三大缓存防护）
     */
    public Session getSessionById(Long sessionId) {
        // ① 布隆过滤器前置拦截
        if (BloomFilterInitializer.sessionBloomReady.get()) {
            RBloomFilter<Long> bloomFilter =
                    redissonClient.getBloomFilter(RedisKeyConst.BLOOM_SESSION_IDS);
            if (!bloomFilter.contains(sessionId)) {
                return null;
            }
        }

        String cacheKey = RedisKeyConst.SESSION_INFO + sessionId;
        Object cached = redisTemplate.opsForValue().get(cacheKey);

        if (NULL_VALUE.equals(cached)) return null;
        if (cached != null) return (Session) cached;

        return handleSessionCacheMiss(sessionId, cacheKey);
    }

    public void evict(Long sessionId) {
        redisTemplate.delete(RedisKeyConst.SESSION_INFO + sessionId);
    }

    // =========================================================================
    // 群成员列表缓存
    // =========================================================================

    /**
     * 获取会话成员 userId 列表
     *
     * <p>流程：
     * <ol>
     *   <li>布隆过滤器拦截非法 sessionId</li>
     *   <li>查 Redis Set（session:members:{sessionId}）</li>
     *   <li>命中且非空值标记 → 返回 userId 集合</li>
     *   <li>未命中 → 查 DB，异步重建 Set，当前线程直接返回 DB 结果</li>
     * </ol>
     *
     * @param sessionId 会话数据库 ID
     * @return 成员 userId 列表，session 不存在时返回 null，成员为空时返回空列表
     */
    public List<Long> getMemberUserIds(Long sessionId) {
        // ① 布隆过滤器前置拦截
        if (BloomFilterInitializer.sessionBloomReady.get()) {
            RBloomFilter<Long> bloomFilter =
                    redissonClient.getBloomFilter(RedisKeyConst.BLOOM_SESSION_IDS);
            if (!bloomFilter.contains(sessionId)) {
                return null; // sessionId 100% 不存在
            }
        }

        String setKey = RedisKeyConst.SESSION_MEMBERS + sessionId;

        // ② 查 Redis Set
        Set<String> cached = stringRedisTemplate.opsForSet().members(setKey);

        if (cached != null && !cached.isEmpty()) {
            // 命中空值标记（session 不存在）
            if (cached.contains(NULL_VALUE)) return null;
            // 正常命中，字符串转 Long
            return cached.stream()
                    .map(Long::parseLong)
                    .collect(Collectors.toList());
        }

        // ③ 缓存未命中，查 DB
        return handleMembersCacheMiss(sessionId, setKey);
    }

    /**
     * 主动失效群成员缓存（会话解散时调用）
     * 使用 UNLINK 异步删除，不阻塞主线程
     */
    public void evictMembers(Long sessionId) {
        stringRedisTemplate.unlink(RedisKeyConst.SESSION_MEMBERS + sessionId);
    }

    /**
     * 成员加入时，直接往 Set 里 SADD，无需重建整个缓存
     * Set 不存在时跳过（下次读请求触发重建）
     */
    public void addMember(Long sessionId, Long userId) {
        String setKey = RedisKeyConst.SESSION_MEMBERS + sessionId;
        // Lua 原子判断 key 存在再 SADD，一次网络往返，消除 TOCTOU 竞态
        stringRedisTemplate.execute(
                SADD_IF_EXISTS_SCRIPT,
                Collections.singletonList(setKey),
                String.valueOf(userId)
        );
    }

    /**
     * 成员退出/被踢时，直接从 Set 里 SREM，无需重建整个缓存
     */
    public void removeMember(Long sessionId, Long userId) {
        String setKey = RedisKeyConst.SESSION_MEMBERS + sessionId;
        // Lua 原子判断 key 存在再 SREM，一次网络往返，消除 TOCTOU 竞态
        stringRedisTemplate.execute(
                SREM_IF_EXISTS_SCRIPT,
                Collections.singletonList(setKey),
                String.valueOf(userId)
        );
    }

    /**
     * 主动写入群成员缓存（创建会话 / 重建时调用）
     */
    public void putMembers(Long sessionId, List<Long> userIds) {
        if (userIds == null || userIds.isEmpty()) return;
        String setKey = RedisKeyConst.SESSION_MEMBERS + sessionId;
        long ttlSeconds = ttlHelper.sessionMembersTtl();
        byte[][] memberBytes = userIds.stream()
                .map(id -> String.valueOf(id).getBytes())
                .toArray(byte[][]::new);
        // Pipeline：SADD + EXPIRE 一次网络往返
        stringRedisTemplate.executePipelined((org.springframework.data.redis.core.RedisCallback<Object>) conn -> {
            byte[] key = setKey.getBytes();
            conn.sAdd(key, memberBytes);
            conn.expire(key, ttlSeconds);
            return null;
        });
    }

    // =========================================================================
    // 内部：群成员缓存未命中处理（单源异步回溯）
    // =========================================================================

    private List<Long> handleMembersCacheMiss(Long sessionId, String setKey) {
        // 先查 DB，保证当前请求能正常返回
        List<Long> userIds = sessionMemberMapper.selectMemberUserIds(sessionId);

        // 抢锁，抢到则异步重建缓存，抢不到则跳过（其他线程会重建）
        String lockKey = RedisKeyConst.SESSION_MEMBERS_REBUILD_LOCK + sessionId;
        RLock lock = redissonClient.getLock(lockKey);
        boolean locked = false;
        try {
            locked = lock.tryLock(0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (locked) {
            final List<Long> toCache = userIds;
            final RLock finalLock = lock;
            cacheRebuildExecutor.execute(() -> {
                try {
                    // Double Check：防止重复重建
                    // 注意：SMEMBERS 在 key 不存在时返回空 Set 而非 null，不能用来判断 key 是否存在
                    if (Boolean.TRUE.equals(stringRedisTemplate.hasKey(setKey))) return;

                    if (toCache == null || toCache.isEmpty()) {
                        // session 不存在或无成员，写空值标记防穿透，TTL=5min
                        stringRedisTemplate.executePipelined((org.springframework.data.redis.core.RedisCallback<Object>) conn -> {
                            byte[] key = setKey.getBytes();
                            conn.sAdd(key, NULL_VALUE.getBytes());
                            conn.expire(key, 5 * 60);
                            return null;
                        });
                    } else {
                        // 正常成员列表，Pipeline 批量 SADD + EXPIRE，一次网络往返
                        long ttlSeconds = ttlHelper.sessionMembersTtl();
                        byte[][] memberBytes = toCache.stream()
                                .map(id -> String.valueOf(id).getBytes())
                                .toArray(byte[][]::new);
                        stringRedisTemplate.executePipelined((org.springframework.data.redis.core.RedisCallback<Object>) conn -> {
                            byte[] key = setKey.getBytes();
                            conn.sAdd(key, memberBytes);
                            conn.expire(key, ttlSeconds);
                            return null;
                        });
                    }
                } catch (Exception e) {
                    log.error("[CacheRebuild] 异步重建群成员缓存失败, sessionId={}", sessionId, e);
                } finally {
                    if (finalLock.isHeldByCurrentThread()) finalLock.unlock();
                }
            });
        }

        return userIds == null ? Collections.emptyList() : userIds;
    }

    // =========================================================================
    // 内部：会话基本信息缓存未命中处理
    // =========================================================================

    private Session handleSessionCacheMiss(Long sessionId, String cacheKey) {
        String lockKey = "cache:rebuild:lock:session:info:" + sessionId;
        RLock outerLock = redissonClient.getLock(lockKey + ":outer");

        boolean outerAcquired = false;
        try {
            outerAcquired = outerLock.tryLock(0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (outerAcquired) {
            cacheRebuildExecutor.submit(() -> doAsyncSessionRebuild(sessionId, cacheKey, lockKey, outerLock));
            return null;
        }

        for (int i = 0; i < SPIN_TIMES; i++) {
            try {
                Thread.sleep(SPIN_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            Object cached = redisTemplate.opsForValue().get(cacheKey);
            if (cached != null) {
                return NULL_VALUE.equals(cached) ? null : (Session) cached;
            }
        }
        return null;
    }

    private void doAsyncSessionRebuild(Long sessionId, String cacheKey, String lockKey, RLock outerLock) {
        try {
            Object cached = redisTemplate.opsForValue().get(cacheKey);
            if (cached != null) return;

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
                Session session = sessionMapper.selectById(sessionId);
                if (session == null) {
                    redisTemplate.opsForValue().set(cacheKey, NULL_VALUE, 5, TimeUnit.MINUTES);
                } else {
                    redisTemplate.opsForValue().set(cacheKey, session,
                            ttlHelper.sessionInfoTtl(), TimeUnit.SECONDS);
                }
            } finally {
                if (rebuildLock.isHeldByCurrentThread()) rebuildLock.unlock();
            }

        } catch (Exception e) {
            log.error("[CacheRebuild] 异步重建会话缓存失败, sessionId={}", sessionId, e);
        } finally {
            if (outerLock.isHeldByCurrentThread()) outerLock.unlock();
        }
    }
}
