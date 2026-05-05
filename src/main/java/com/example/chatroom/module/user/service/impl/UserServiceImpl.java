package com.example.chatroom.module.user.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.example.chatroom.cache.user.UserCacheService;
import com.example.chatroom.common.constant.RedisKeyConst;
import com.example.chatroom.common.exception.BizException;
import com.example.chatroom.common.response.ResultCode;
import com.example.chatroom.common.util.JwtUtil;
import com.example.chatroom.common.util.SnowflakeIdGenerator;
import com.example.chatroom.module.auth.domain.vo.TokenVO;
import com.example.chatroom.module.auth.domain.vo.UserInfoVO;
import com.example.chatroom.module.user.domain.dto.RegisterDTO;
import com.example.chatroom.module.user.domain.dto.UpdateFriendDTO;
import com.example.chatroom.module.user.domain.dto.UpdateProfileDTO;
import com.example.chatroom.module.user.domain.dto.UpdateUserDTO;
import com.example.chatroom.module.user.domain.entity.User;
import com.example.chatroom.module.user.domain.entity.UserFriend;
import com.example.chatroom.module.user.domain.entity.UserProfile;
import com.example.chatroom.module.user.domain.entity.UserRefreshToken;
import com.example.chatroom.module.user.domain.vo.UserProfileVO;
import com.example.chatroom.module.user.domain.vo.UserVO;
import com.example.chatroom.module.user.mapper.UserFriendMapper;
import com.example.chatroom.module.user.mapper.UserMapper;
import com.example.chatroom.module.user.mapper.UserProfileMapper;
import com.example.chatroom.module.user.mapper.UserRefreshTokenMapper;
import com.example.chatroom.module.user.service.UserService;
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
import org.springframework.util.StringUtils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 用户服务实现
 *
 * 注册流程：
 * 1. 字段查重（先查 Redis 缓存，再查 DB）：用户名 / 手机号 / 邮箱
 * 2. Redisson 分布式锁（lock:register:{username}）防并发重复注册
 * 3. 锁内 double check 再次查重
 * 4. 小事务：insert user + insert refreshToken（通过 AopContext 代理调用保证事务生效）
 * 5. 事务提交后写 Redis（pipeline）+ 布隆过滤器
 * 6. 返回 TokenVO，前端无需二次登录
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class UserServiceImpl implements UserService {

    private final UserMapper userMapper;
    private final UserRefreshTokenMapper refreshTokenMapper;
    private final UserFriendMapper userFriendMapper;
    private final UserProfileMapper userProfileMapper;
    private final SnowflakeIdGenerator idGenerator;
    private final BCryptPasswordEncoder passwordEncoder;
    private final JwtUtil jwtUtil;
    private final RedisTemplate<String, Object> redisTemplate;
    private final UserCacheService userCacheService;
    private final RedissonClient redissonClient;

    @Value("${jwt.refresh-token-expire:604800}")
    private long refreshTokenExpire; // 秒，默认 7 天

    private static final DateTimeFormatter ISO_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    // ─────────────────────────────────────────────────────────────────────────
    // 注册
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    // 不加方法级 @Transactional：BCrypt 加密（慢操作）和 Redis 操作均在事务外；
    // 小事务通过 persistNewUser() 代理调用完成。
    public TokenVO register(RegisterDTO dto, String clientIp) {

        // ── 1. 字段查重（先查缓存，再查 DB）────────────────────────────────
        checkDuplicateFields(dto);

        // ── 2. 加分布式锁，防并发重复注册 ──────────────────────────────────
        RLock lock = redissonClient.getLock("lock:register:" + dto.getUsername());
        try {
            if (!lock.tryLock(3, 10, TimeUnit.SECONDS)) {
                throw new BizException(ResultCode.TOO_MANY_REQUESTS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BizException(ResultCode.TOO_MANY_REQUESTS);
        }

        try {
            // ── 3. 锁内 double check ─────────────────────────────────────
            checkDuplicateFields(dto);

            // ── 4. BCrypt 加密（慢操作，在锁内但在事务外，不占 DB 连接）──
            String passwordHash = passwordEncoder.encode(dto.getPassword());

            // ── 5. 构建用户实体 ──────────────────────────────────────────
            User user = new User();
            user.setUserNo(String.valueOf(idGenerator.nextId()));
            user.setUsername(dto.getUsername());
            user.setNickname(dto.getNickname());
            user.setPasswordHash(passwordHash);
            user.setPhone(dto.getPhone());
            user.setEmail(dto.getEmail());
            user.setStatus(1);

            // ── 6. 生成 Refresh Token ────────────────────────────────────
            String rawRefreshToken = UUID.randomUUID().toString().replace("-", "");
            String tokenHash = sha256Hex(rawRefreshToken);

            // ── 7. 小事务：insert user + insert refreshToken ─────────────
            // 通过 AopContext.currentProxy() 拿代理对象调用，保证 @Transactional 生效
            ((UserServiceImpl) AopContext.currentProxy())
                    .persistNewUser(user, tokenHash, clientIp, dto.getDeviceInfo());

            // ── 8. 事务提交后写 Redis（pipeline）+ 布隆过滤器 ────────────
            writeRegisterCachePipeline(user, tokenHash);

            // ── 9. 生成 Access Token，组装 TokenVO 返回 ──────────────────
            String accessToken = jwtUtil.generateAccessToken(user.getId(), user.getUsername());
            log.info("[Register] 注册成功: userId={}, username={}", user.getId(), user.getUsername());
            return buildTokenVO(accessToken, rawRefreshToken, user);

        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    /**
     * 字段查重：用户名 / 手机号 / 邮箱
     * 策略：先查 Redis 缓存（布隆过滤器 + 用户信息缓存），再查 DB
     * 注意：布隆过滤器只能判断"一定不存在"，命中时仍需 DB 确认
     */
    private void checkDuplicateFields(RegisterDTO dto) {
        // 用户名：直接查 DB（用户名无独立缓存，布隆过滤器基于 userId 不适用）
        Long usernameCount = userMapper.selectCount(
                new LambdaQueryWrapper<User>().eq(User::getUsername, dto.getUsername()));
        if (usernameCount > 0) {
            throw new BizException(ResultCode.USERNAME_DUPLICATE);
        }

        // 手机号：非空时查重
        if (StringUtils.hasText(dto.getPhone())) {
            Long phoneCount = userMapper.selectCount(
                    new LambdaQueryWrapper<User>().eq(User::getPhone, dto.getPhone()));
            if (phoneCount > 0) {
                throw new BizException(ResultCode.PHONE_DUPLICATE);
            }
        }

        // 邮箱：非空时查重
        if (StringUtils.hasText(dto.getEmail())) {
            Long emailCount = userMapper.selectCount(
                    new LambdaQueryWrapper<User>().eq(User::getEmail, dto.getEmail()));
            if (emailCount > 0) {
                throw new BizException(ResultCode.EMAIL_DUPLICATE);
            }
        }
    }

    /**
     * 小事务：insert user + insert refreshToken + update lastLoginAt
     * 通过代理调用保证事务生效，不可改为 private
     */
    @Transactional(rollbackFor = Exception.class)
    public void persistNewUser(User user, String tokenHash, String clientIp, String deviceInfo) {
        // insert user
        userMapper.insert(user);

        // insert refreshToken
        UserRefreshToken tokenEntity = new UserRefreshToken();
        tokenEntity.setUserId(user.getId());
        tokenEntity.setTokenHash(tokenHash);
        tokenEntity.setDeviceInfo(deviceInfo);
        tokenEntity.setIp(clientIp);
        tokenEntity.setExpiresAt(LocalDateTime.now().plusSeconds(refreshTokenExpire));
        tokenEntity.setRevoked(0);
        refreshTokenMapper.insert(tokenEntity);

        // 记录首次登录时间
        User update = new User();
        update.setId(user.getId());
        update.setLastLoginAt(LocalDateTime.now());
        update.setLastLoginIp(clientIp);
        userMapper.updateById(update);
    }

    /**
     * 注册成功后写 Redis（pipeline）+ 布隆过滤器
     * 与登录的 writeLoginCachePipeline 逻辑一致：
     *   user:info:{userId}          → User 实体（含布隆过滤器 add，单独调用）
     *   token:refresh:{tokenHash}   → userId
     *   user:login:current:{userId} → tokenHash
     */
    private void writeRegisterCachePipeline(User user, String tokenHash) {
        // 布隆过滤器 + 用户信息缓存（内部有 Redisson 操作，不能进 pipeline）
        userCacheService.put(user);

        // token:refresh 和 user:login:current 两个 key 用 pipeline 一次写入
        final String refreshKey = RedisKeyConst.TOKEN_REFRESH + tokenHash;
        final String currentKey = RedisKeyConst.USER_LOGIN_CURRENT + user.getId();
        final String userIdStr = String.valueOf(user.getId());

        redisTemplate.executePipelined((org.springframework.data.redis.core.RedisCallback<Object>) conn -> {
            byte[] rKey = redisTemplate.getStringSerializer().serialize(refreshKey);
            byte[] cKey = redisTemplate.getStringSerializer().serialize(currentKey);
            byte[] rVal = redisTemplate.getStringSerializer().serialize(userIdStr);
            byte[] cVal = redisTemplate.getStringSerializer().serialize(tokenHash);
            conn.set(rKey, rVal);
            conn.expire(rKey, refreshTokenExpire);
            conn.set(cKey, cVal);
            conn.expire(cKey, refreshTokenExpire);
            return null;
        });
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 其他用户操作
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public UserVO getMe(Long userId) {
        User user = userMapper.selectById(userId);
        if (user == null) throw new BizException(ResultCode.USER_NOT_FOUND);
        return toVO(user);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void updateMe(Long userId, UpdateUserDTO dto) {
        User user = new User();
        user.setId(userId);
        user.setNickname(dto.getNickname());
        user.setBio(dto.getBio());
        user.setGender(dto.getGender());
        userMapper.updateById(user);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void updatePassword(Long userId, String oldPassword, String newPassword) {
        User user = userMapper.selectById(userId);
        if (user == null) throw new BizException(ResultCode.USER_NOT_FOUND);
        if (!passwordEncoder.matches(oldPassword, user.getPasswordHash())) {
            throw new BizException(ResultCode.PASSWORD_ERROR);
        }
        User update = new User();
        update.setId(userId);
        update.setPasswordHash(passwordEncoder.encode(newPassword));
        userMapper.updateById(update);
    }

    @Override
    public void updateAvatar(Long userId, String avatarUrl) {
        User update = new User();
        update.setId(userId);
        update.setAvatarUrl(avatarUrl);
        userMapper.updateById(update);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void deleteMe(Long userId) {
        userMapper.deleteById(userId);
    }

    @Override
    public UserVO getUserByNo(String userNo) {
        User user = userMapper.selectOne(
                new LambdaQueryWrapper<User>().eq(User::getUserNo, userNo));
        if (user == null) throw new BizException(ResultCode.USER_NOT_FOUND);
        return toVO(user);
    }

    @Override
    public List<UserVO> searchUsers(String keyword) {
        List<User> users = userMapper.selectList(
                new LambdaQueryWrapper<User>()
                        .like(User::getUsername, keyword)
                        .or()
                        .like(User::getPhone, keyword)
                        .last("LIMIT 20"));
        return users.stream().map(this::toVO).collect(Collectors.toList());
    }

    @Override
    public List<UserVO> getFriends(Long userId) {
        // 查出当前用户的所有正常好友关系（status=1）
        List<UserFriend> friendRelations = userFriendMapper.selectList(
                new LambdaQueryWrapper<UserFriend>()
                        .eq(UserFriend::getUserId, userId)
                        .eq(UserFriend::getStatus, 1));
        if (friendRelations.isEmpty()) {
            return List.of();
        }
        List<Long> friendIds = friendRelations.stream()
                .map(UserFriend::getFriendId)
                .collect(Collectors.toList());
        List<User> users = userMapper.selectBatchIds(friendIds);
        return users.stream().map(this::toVO).collect(Collectors.toList());
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void addFriend(Long userId, String friendUserNo) {
        // 查目标用户
        User friend = userMapper.selectOne(
                new LambdaQueryWrapper<User>().eq(User::getUserNo, friendUserNo));
        if (friend == null) throw new BizException(ResultCode.USER_NOT_FOUND);
        if (friend.getId().equals(userId)) throw new BizException(ResultCode.CANNOT_ADD_SELF);

        // 检查是否已是好友
        Long exists = userFriendMapper.selectCount(
                new LambdaQueryWrapper<UserFriend>()
                        .eq(UserFriend::getUserId, userId)
                        .eq(UserFriend::getFriendId, friend.getId()));
        if (exists > 0) throw new BizException(ResultCode.ALREADY_FRIEND);

        // 双向写入好友关系
        UserFriend a = new UserFriend();
        a.setUserId(userId);
        a.setFriendId(friend.getId());
        a.setStatus(1);
        userFriendMapper.insert(a);

        UserFriend b = new UserFriend();
        b.setUserId(friend.getId());
        b.setFriendId(userId);
        b.setStatus(1);
        userFriendMapper.insert(b);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void deleteFriend(Long userId, Long friendId) {
        // 校验好友关系存在
        Long exists = userFriendMapper.selectCount(
                new LambdaQueryWrapper<UserFriend>()
                        .eq(UserFriend::getUserId, userId)
                        .eq(UserFriend::getFriendId, friendId));
        if (exists == 0) throw new BizException(ResultCode.NOT_FRIEND);

        // 双向删除
        userFriendMapper.delete(
                new LambdaQueryWrapper<UserFriend>()
                        .eq(UserFriend::getUserId, userId)
                        .eq(UserFriend::getFriendId, friendId));
        userFriendMapper.delete(
                new LambdaQueryWrapper<UserFriend>()
                        .eq(UserFriend::getUserId, friendId)
                        .eq(UserFriend::getFriendId, userId));
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void updateFriend(Long userId, Long friendId, UpdateFriendDTO dto) {
        UserFriend record = userFriendMapper.selectOne(
                new LambdaQueryWrapper<UserFriend>()
                        .eq(UserFriend::getUserId, userId)
                        .eq(UserFriend::getFriendId, friendId));
        if (record == null) throw new BizException(ResultCode.NOT_FRIEND);

        if (dto.getRemark() != null) record.setRemark(dto.getRemark());
        if (dto.getStatus() != null) record.setStatus(dto.getStatus());
        userFriendMapper.updateById(record);
    }

    @Override
    public UserProfileVO getProfile(Long userId) {
        UserProfile profile = userProfileMapper.selectOne(
                new LambdaQueryWrapper<UserProfile>().eq(UserProfile::getUserId, userId));
        if (profile == null) return new UserProfileVO();
        UserProfileVO vo = new UserProfileVO();
        vo.setRealName(profile.getRealName());
        vo.setBirthday(profile.getBirthday());
        vo.setRegion(profile.getRegion());
        vo.setSignature(profile.getSignature());
        vo.setBackgroundUrl(profile.getBackgroundUrl());
        return vo;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void updateProfile(Long userId, UpdateProfileDTO dto) {
        UserProfile profile = userProfileMapper.selectOne(
                new LambdaQueryWrapper<UserProfile>().eq(UserProfile::getUserId, userId));
        if (profile == null) {
            profile = new UserProfile();
            profile.setUserId(userId);
            profile.setRealName(dto.getRealName());
            profile.setBirthday(dto.getBirthday());
            profile.setRegion(dto.getRegion());
            profile.setSignature(dto.getSignature());
            profile.setBackgroundUrl(dto.getBackgroundUrl());
            userProfileMapper.insert(profile);
        } else {
            if (dto.getRealName() != null) profile.setRealName(dto.getRealName());
            if (dto.getBirthday() != null) profile.setBirthday(dto.getBirthday());
            if (dto.getRegion() != null) profile.setRegion(dto.getRegion());
            if (dto.getSignature() != null) profile.setSignature(dto.getSignature());
            if (dto.getBackgroundUrl() != null) profile.setBackgroundUrl(dto.getBackgroundUrl());
            userProfileMapper.updateById(profile);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 私有工具方法
    // ─────────────────────────────────────────────────────────────────────────

    /** 组装 TokenVO，字段与 AuthServiceImpl.buildTokenVO 保持一致 */
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

    @Override
    public List<User> batchGetUsers(List<Long> userIds) {
        return userCacheService.batchGetUserByIds(userIds);
    }

    private UserVO toVO(User user) {
        UserVO vo = new UserVO();
        vo.setUserNo(user.getUserNo());
        vo.setUsername(user.getUsername());
        vo.setNickname(user.getNickname());
        vo.setAvatarUrl(user.getAvatarUrl());
        vo.setEmail(user.getEmail());
        vo.setPhone(user.getPhone());
        vo.setGender(user.getGender());
        vo.setBio(user.getBio());
        vo.setStatus(user.getStatus());
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
