package com.example.chatroom.module.auth.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.example.chatroom.cache.user.UserCacheService;
import com.example.chatroom.common.constant.RedisKeyConst;
import com.example.chatroom.common.exception.BizException;
import com.example.chatroom.common.response.ResultCode;
import com.example.chatroom.common.util.JwtUtil;
import com.example.chatroom.module.auth.domain.dto.LoginDTO;
import com.example.chatroom.module.auth.domain.vo.TokenVO;
import com.example.chatroom.module.auth.domain.vo.UserInfoVO;
import com.example.chatroom.module.auth.service.AuthService;
import com.example.chatroom.module.user.domain.entity.User;
import com.example.chatroom.module.user.domain.entity.UserRefreshToken;
import com.example.chatroom.module.user.mapper.UserMapper;
import com.example.chatroom.module.user.mapper.UserRefreshTokenMapper;
import io.jsonwebtoken.Claims;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HexFormat;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 认证服务实现
 *
 * Redis Key 说明：
 *   user:info:{userId}            → User 实体缓存（TTL 随机，防雪崩）
 *   user:login:current:{userId}   → 当前有效的 refreshToken SHA-256 哈希（TTL = 7d）
 *                                   用于登录前置校验：key 存在 = 已登录
 *   token:refresh:{sha256Hash}    → userId（TTL = 7d，供 refresh 接口快速查 userId）
 *   token:blacklist:{jti}         → "1"（TTL = Access Token 剩余有效期，登出时写入）
 *
 * 事务设计：
 *   login() 不加方法级 @Transactional，BCrypt 验证（慢操作）和 Redis 操作均在事务外。
 *   只有 insert refreshToken + update lastLoginAt 这两步 DB 写操作包在小事务里。
 *   并发安全：Redisson 分布式锁 lock:login:{userId} 保证同一用户同时只有一个线程
 *   走完整创建流程，锁内 double check 防止重复创建。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AuthServiceImpl implements AuthService {

    private final UserMapper userMapper;
    private final UserRefreshTokenMapper refreshTokenMapper;
    private final JwtUtil jwtUtil;
    private final BCryptPasswordEncoder passwordEncoder;
    private final RedisTemplate<String, Object> redisTemplate;
    private final UserCacheService userCacheService;
    private final RedissonClient redissonClient;

    @Value("${jwt.refresh-token-expire:604800}")
    private long refreshTokenExpire; // 秒，默认 7 天

    private static final DateTimeFormatter ISO_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    // ─────────────────────────────────────────────────────────────────────────
    // 登录
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    // 注意：不加方法级 @Transactional，BCrypt 慢操作不占连接，小事务在 doCreateToken() 内
    public TokenVO login(LoginDTO dto, String clientIp) {

        // 1. 查用户
        User user = userMapper.selectOne(
                new LambdaQueryWrapper<User>().eq(User::getUsername, dto.getUsername()));
        if (user == null) {
            throw new BizException(ResultCode.USER_NOT_FOUND);
        }

        // 2. BCrypt 密码校验（慢操作，约 200-400ms，不持有 DB 连接）
        if (!passwordEncoder.matches(dto.getPassword(), user.getPasswordHash())) {
            throw new BizException(ResultCode.PASSWORD_ERROR);
        }

        // 3. 账号状态校验
        if (user.getStatus() != 1) {
            throw new BizException(ResultCode.USER_DISABLED);
        }

        // 4. 前置校验：检查用户是否已登录，防止重复创建 token
        //    快路径：Redis user:login:current:{userId} 存在 → 直接复用
        //    兜底路径：Redis 无值（重启/key 丢失）→ 查 DB 确认是否有未过期的有效 token
        String existingTokenHash = getCurrentLoginHash(user.getId());
        if (existingTokenHash == null) {
            existingTokenHash = getValidTokenHashFromDb(user.getId());
            if (existingTokenHash != null) {
                // DB 有有效记录，Redis 丢了，pipeline 回写补齐
                final String hashToRestore = existingTokenHash;
                final Long userIdToRestore = user.getId();
                redisTemplate.executePipelined((org.springframework.data.redis.core.RedisCallback<Object>) conn -> {
                    byte[] currentKey = redisTemplate.getStringSerializer()
                            .serialize(RedisKeyConst.USER_LOGIN_CURRENT + userIdToRestore);
                    byte[] refreshKey = redisTemplate.getStringSerializer()
                            .serialize(RedisKeyConst.TOKEN_REFRESH + hashToRestore);
                    byte[] hashVal = redisTemplate.getStringSerializer().serialize(hashToRestore);
                    byte[] userIdVal = redisTemplate.getStringSerializer()
                            .serialize(String.valueOf(userIdToRestore));
                    conn.set(currentKey, hashVal);
                    conn.expire(currentKey, refreshTokenExpire);
                    conn.set(refreshKey, userIdVal);
                    conn.expire(refreshKey, refreshTokenExpire);
                    return null;
                });
                log.debug("[Auth] Redis 登录态丢失，从 DB 恢复: userId={}", user.getId());
            }
        }
        if (existingTokenHash != null) {
            log.debug("[Auth] 用户已登录，重签 accessToken 返回: userId={}", user.getId());
            return reuseExistingToken(existingTokenHash, user);
        }

        // 5. 未登录 → 加分布式锁，防止并发重复创建 token
        return doCreateToken(user, clientIp, dto.getDeviceInfo());
    }

    /**
     * 加 Redisson 分布式锁，锁内 double check，通过后执行小事务创建 token。
     * 锁粒度：单个 userId，不同用户互不影响。
     */
    private TokenVO doCreateToken(User user, String clientIp, String deviceInfo) {
        RLock lock = redissonClient.getLock("lock:login:" + user.getId());
        try {
            // 最多等待 3s，持锁 10s（看门狗不启用，登录操作有明确上限）
            if (!lock.tryLock(3, 10, TimeUnit.SECONDS)) {
                throw new BizException(ResultCode.TOO_MANY_REQUESTS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BizException(ResultCode.TOO_MANY_REQUESTS);
        }

        try {
            // double check：拿到锁后再查一次，防止排队的请求重复创建
            String existingTokenHash = getCurrentLoginHash(user.getId());
            if (existingTokenHash == null) {
                existingTokenHash = getValidTokenHashFromDb(user.getId());
            }
            if (existingTokenHash != null) {
                log.debug("[Auth] double check 命中，用户已登录: userId={}", user.getId());
                return reuseExistingToken(existingTokenHash, user);
            }

            // 真正未登录，执行小事务：insert refreshToken + update lastLoginAt
            String rawRefreshToken = UUID.randomUUID().toString().replace("-", "");
            String tokenHash = sha256Hex(rawRefreshToken);
            // 通过 AopContext.currentProxy() 拿到代理对象调用，保证 @Transactional 生效
            ((AuthServiceImpl) AopContext.currentProxy()).persistNewToken(user, tokenHash, clientIp, deviceInfo);

            // 事务提交后写 Redis（pipeline 一次往返写三个 key）
            writeLoginCachePipeline(user, tokenHash);

            String accessToken = jwtUtil.generateAccessToken(user.getId(), user.getUsername());
            return buildTokenVO(accessToken, rawRefreshToken, user);

        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    /**
     * 小事务：insert refreshToken + update lastLoginAt
     * 通过代理调用保证事务生效，不可改为 private
     */
    @Transactional(rollbackFor = Exception.class)
    public void persistNewToken(User user, String tokenHash, String clientIp, String deviceInfo) {
        UserRefreshToken entity = new UserRefreshToken();
        entity.setUserId(user.getId());
        entity.setTokenHash(tokenHash);
        entity.setDeviceInfo(deviceInfo);
        entity.setIp(clientIp);
        entity.setExpiresAt(LocalDateTime.now().plusSeconds(refreshTokenExpire));
        entity.setRevoked(0);
        refreshTokenMapper.insert(entity);

        User update = new User();
        update.setId(user.getId());
        update.setLastLoginAt(LocalDateTime.now());
        update.setLastLoginIp(clientIp);
        userMapper.updateById(update);
    }

    /**
     * Pipeline 一次往返写三个登录态 Redis key：
     *   user:info:{userId}          → User 实体（含布隆过滤器 add，单独调用）
     *   token:refresh:{tokenHash}   → userId
     *   user:login:current:{userId} → tokenHash
     *
     * 注意：userCacheService.put() 内部有布隆过滤器操作，不适合放进 pipeline，单独调用。
     */
    private void writeLoginCachePipeline(User user, String tokenHash) {
        // 布隆过滤器 + 用户信息缓存（内部有 Redisson 操作，不能进 pipeline）
        userCacheService.put(user);

        // token:refresh 和 user:login:current 两个 key 用 pipeline 一次写入
        final String refreshKey = RedisKeyConst.TOKEN_REFRESH + tokenHash;
        final String currentKey = RedisKeyConst.USER_LOGIN_CURRENT + user.getId();
        final String userIdStr = String.valueOf(user.getId());
        final String tokenHashStr = tokenHash;

        redisTemplate.executePipelined((org.springframework.data.redis.core.RedisCallback<Object>) conn -> {
            byte[] rKey = redisTemplate.getStringSerializer().serialize(refreshKey);
            byte[] cKey = redisTemplate.getStringSerializer().serialize(currentKey);
            byte[] rVal = redisTemplate.getStringSerializer().serialize(userIdStr);
            byte[] cVal = redisTemplate.getStringSerializer().serialize(tokenHashStr);
            conn.set(rKey, rVal);
            conn.expire(rKey, refreshTokenExpire);
            conn.set(cKey, cVal);
            conn.expire(cKey, refreshTokenExpire);
            return null;
        });
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 登出
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public void logout(String accessToken, String refreshToken) {

        // Access Token 加入黑名单（TTL = 剩余有效期，过期自动清除）
        if (accessToken != null) {
            try {
                Claims claims = jwtUtil.parseToken(accessToken);
                String jti = jwtUtil.getJti(claims);
                long remaining = jwtUtil.getRemainingExpire(claims);
                if (remaining > 0) {
                    redisTemplate.opsForValue().set(
                            RedisKeyConst.TOKEN_BLACKLIST + jti, "1", remaining, TimeUnit.SECONDS);
                }
            } catch (Exception e) {
                log.warn("[Auth] 登出时解析 Access Token 失败，忽略: {}", e.getMessage());
            }
        }

        if (refreshToken != null) {
            String tokenHash = sha256Hex(refreshToken);

            // 先查 userId（DB 标记 revoked 之前查，保证能查到）
            Long logoutUserId = getUserIdByTokenHash(tokenHash);

            // DB 标记撤销
            refreshTokenMapper.update(null,
                    new LambdaUpdateWrapper<UserRefreshToken>()
                            .eq(UserRefreshToken::getTokenHash, tokenHash)
                            .set(UserRefreshToken::getRevoked, 1));

            // pipeline 删除两个 Redis key
            if (logoutUserId != null) {
                final String refreshKey = RedisKeyConst.TOKEN_REFRESH + tokenHash;
                final String currentKey = RedisKeyConst.USER_LOGIN_CURRENT + logoutUserId;
                redisTemplate.executePipelined((org.springframework.data.redis.core.RedisCallback<Object>) conn -> {
                    conn.del(redisTemplate.getStringSerializer().serialize(refreshKey));
                    conn.del(redisTemplate.getStringSerializer().serialize(currentKey));
                    return null;
                });
            } else {
                redisTemplate.delete(RedisKeyConst.TOKEN_REFRESH + tokenHash);
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 刷新 Token
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public TokenVO refresh(String refreshToken) {

        String tokenHash = sha256Hex(refreshToken);

        // 优先从 Redis 读 userId（命中则跳过 DB 查询）
        Long userId = getRefreshTokenCache(tokenHash);

        UserRefreshToken entity = null;
        User user;

        if (userId != null) {
            user = userCacheService.getUserById(userId);
            if (user == null) {
                user = userMapper.selectById(userId);
            }
            if (user == null || user.getStatus() != 1) {
                // pipeline 清理两个 key
                final String rKey = RedisKeyConst.TOKEN_REFRESH + tokenHash;
                final String cKey = RedisKeyConst.USER_LOGIN_CURRENT + userId;
                redisTemplate.executePipelined((org.springframework.data.redis.core.RedisCallback<Object>) conn -> {
                    conn.del(redisTemplate.getStringSerializer().serialize(rKey));
                    conn.del(redisTemplate.getStringSerializer().serialize(cKey));
                    return null;
                });
                throw new BizException(ResultCode.USER_DISABLED);
            }
        } else {
            // Redis 未命中：降级走 DB
            entity = refreshTokenMapper.selectOne(
                    new LambdaQueryWrapper<UserRefreshToken>()
                            .eq(UserRefreshToken::getTokenHash, tokenHash)
                            .eq(UserRefreshToken::getRevoked, 0));

            if (entity == null || entity.getExpiresAt().isBefore(LocalDateTime.now())) {
                throw new BizException(ResultCode.UNAUTHORIZED);
            }

            user = userMapper.selectById(entity.getUserId());
            if (user == null || user.getStatus() != 1) {
                throw new BizException(ResultCode.USER_DISABLED);
            }
            userId = user.getId();
        }

        // 滑动续期：生成新 Refresh Token，旧的撤销
        String newRawRefreshToken = UUID.randomUUID().toString().replace("-", "");
        String newTokenHash = sha256Hex(newRawRefreshToken);

        // DB：撤销旧 token，插入新 token
        refreshTokenMapper.update(null,
                new LambdaUpdateWrapper<UserRefreshToken>()
                        .eq(UserRefreshToken::getTokenHash, tokenHash)
                        .set(UserRefreshToken::getRevoked, 1));

        UserRefreshToken newEntity = new UserRefreshToken();
        newEntity.setUserId(userId);
        newEntity.setTokenHash(newTokenHash);
        newEntity.setDeviceInfo(entity != null ? entity.getDeviceInfo() : null);
        newEntity.setIp(entity != null ? entity.getIp() : null);
        newEntity.setExpiresAt(LocalDateTime.now().plusSeconds(refreshTokenExpire));
        newEntity.setRevoked(0);
        refreshTokenMapper.insert(newEntity);

        // pipeline：删旧两个 key，写新两个 key（4 条命令一次往返）
        final String oldRefreshKey = RedisKeyConst.TOKEN_REFRESH + tokenHash;
        final String currentKey = RedisKeyConst.USER_LOGIN_CURRENT + userId;
        final String newRefreshKey = RedisKeyConst.TOKEN_REFRESH + newTokenHash;
        final String userIdStr = String.valueOf(userId);
        final String newTokenHashStr = newTokenHash;
        final long ttl = refreshTokenExpire;

        redisTemplate.executePipelined((org.springframework.data.redis.core.RedisCallback<Object>) conn -> {
            // 删旧
            conn.del(redisTemplate.getStringSerializer().serialize(oldRefreshKey));
            // 写新 token:refresh
            byte[] nrKey = redisTemplate.getStringSerializer().serialize(newRefreshKey);
            byte[] nrVal = redisTemplate.getStringSerializer().serialize(userIdStr);
            conn.set(nrKey, nrVal);
            conn.expire(nrKey, ttl);
            // 更新 user:login:current
            byte[] cKey = redisTemplate.getStringSerializer().serialize(currentKey);
            byte[] cVal = redisTemplate.getStringSerializer().serialize(newTokenHashStr);
            conn.set(cKey, cVal);
            conn.expire(cKey, ttl);
            return null;
        });

        // 用户信息缓存刷新（含布隆过滤器，不进 pipeline）
        userCacheService.put(user);

        String newAccessToken = jwtUtil.generateAccessToken(user.getId(), user.getUsername());
        return buildTokenVO(newAccessToken, newRawRefreshToken, user);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 私有工具方法
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * 已登录时复用现有 refreshToken，只重签一个新 accessToken 返回。
     * refreshToken 原文不存 DB/Redis（安全设计），前端本地必然已有，
     * 响应里 refreshToken 返回空字符串，前端收到后保持本地原有值不变。
     */
    private TokenVO reuseExistingToken(String existingTokenHash, User user) {
        String newAccessToken = jwtUtil.generateAccessToken(user.getId(), user.getUsername());
        userCacheService.put(user);
        return buildTokenVO(newAccessToken, "", user);
    }

    /** 读 Redis：userId → 当前登录态 tokenHash，null 表示未登录 */
    private String getCurrentLoginHash(Long userId) {
        Object val = redisTemplate.opsForValue().get(RedisKeyConst.USER_LOGIN_CURRENT + userId);
        return val instanceof String s ? s : null;
    }

    /** 读 Redis：tokenHash → userId，null 表示未命中 */
    private Long getRefreshTokenCache(String tokenHash) {
        Object val = redisTemplate.opsForValue().get(RedisKeyConst.TOKEN_REFRESH + tokenHash);
        if (val == null) return null;
        if (val instanceof Long l) return l;
        if (val instanceof Integer i) return i.longValue();
        if (val instanceof Number n) return n.longValue();
        // pipeline 回写时 userId 以字符串存入，兼容解析
        if (val instanceof String s) {
            try { return Long.parseLong(s); } catch (NumberFormatException ignored) {}
        }
        return null;
    }

    /**
     * 查 DB：该用户是否有未过期的有效 refreshToken（Redis 兜底路径）
     * 按过期时间倒序取最新一条，返回其 tokenHash
     */
    private String getValidTokenHashFromDb(Long userId) {
        UserRefreshToken entity = refreshTokenMapper.selectOne(
                new LambdaQueryWrapper<UserRefreshToken>()
                        .eq(UserRefreshToken::getUserId, userId)
                        .eq(UserRefreshToken::getRevoked, 0)
                        .gt(UserRefreshToken::getExpiresAt, LocalDateTime.now())
                        .orderByDesc(UserRefreshToken::getExpiresAt)
                        .last("LIMIT 1"));
        return entity != null ? entity.getTokenHash() : null;
    }

    /**
     * 从 DB 查 userId（revoked 不过滤，登出时 revoke 之前调用）
     * 只查 userId 字段，减少数据传输
     */
    private Long getUserIdByTokenHash(String tokenHash) {
        UserRefreshToken entity = refreshTokenMapper.selectOne(
                new LambdaQueryWrapper<UserRefreshToken>()
                        .eq(UserRefreshToken::getTokenHash, tokenHash)
                        .select(UserRefreshToken::getUserId));
        return entity != null ? entity.getUserId() : null;
    }

    /** 组装 TokenVO，字段严格对齐前端 UserInfo */
    private TokenVO buildTokenVO(String accessToken, String rawRefreshToken, User user) {
        UserInfoVO userInfoVO = new UserInfoVO();
        userInfoVO.setId(user.getUserNo());
        userInfoVO.setUsername(user.getUsername());
        userInfoVO.setNickname(user.getNickname());
        userInfoVO.setAvatar(user.getAvatarUrl());
        userInfoVO.setBio(user.getBio());
        userInfoVO.setStatus("online");
        if (user.getCreatedAt() != null) {
            userInfoVO.setCreatedAt(user.getCreatedAt().format(ISO_FMT));
        }
        TokenVO vo = new TokenVO();
        vo.setAccessToken(accessToken);
        vo.setRefreshToken(rawRefreshToken);
        vo.setAccessTokenExpire(1800L);
        vo.setUser(userInfoVO);
        return vo;
    }

    /** SHA-256 哈希，返回小写十六进制字符串 */
    private static String sha256Hex(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}
