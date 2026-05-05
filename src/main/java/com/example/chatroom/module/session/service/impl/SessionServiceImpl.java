package com.example.chatroom.module.session.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.example.chatroom.cache.session.SessionCacheService;
import com.example.chatroom.common.constant.RedisKeyConst;
import com.example.chatroom.common.exception.BizException;
import com.example.chatroom.common.response.PageResult;
import com.example.chatroom.common.response.ResultCode;
import com.example.chatroom.common.util.SnowflakeIdGenerator;
import com.example.chatroom.module.message.mapper.ChatMessageMapper;
import com.example.chatroom.module.session.domain.dto.CreateGroupSessionDTO;
import com.example.chatroom.module.session.domain.dto.CreateSingleSessionDTO;
import com.example.chatroom.module.session.domain.dto.LastReadPositionDTO;
import com.example.chatroom.module.session.domain.dto.UpdateGroupSessionDTO;
import com.example.chatroom.module.session.domain.entity.Session;
import com.example.chatroom.module.session.domain.entity.SessionMember;
import com.example.chatroom.module.session.domain.vo.ReadPositionVO;
import com.example.chatroom.module.session.domain.vo.SessionListVO;
import com.example.chatroom.module.session.domain.vo.SessionMemberVO;
import com.example.chatroom.module.session.domain.vo.SessionVO;
import com.example.chatroom.module.session.mapper.SessionMapper;
import com.example.chatroom.module.session.mapper.SessionMemberMapper;
import com.example.chatroom.module.session.service.SessionService;
import com.example.chatroom.module.user.domain.entity.User;
import com.example.chatroom.module.user.mapper.UserMapper;
import com.example.chatroom.module.user.service.UserService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 会话服务实现
 *
 * <h3>listSessions 缓存策略</h3>
 * <pre>
 * 1. 先读分页缓存（key = user:session:list:{userId}:p{page}:s{size}）
 * 2. 命中 → 对每个会话查 ZSet 未读数 + 最后一条消息 → 返回
 * 3. 未命中 → SET NX 抢锁
 *    3a. 抢到锁 → Double Check → 查 DB → 异步提交 cacheRebuildExecutor 写缓存（单源异步回溯）
 *              → 当前线程填充未读数 + 最后一条消息 → 返回
 *    3b. 未抢到锁 → 直接返回空，前端展示"获取中"，100ms 后轮询
 * </pre>
 *
 * <h3>未读数 + 最后一条消息策略</h3>
 * <pre>
 * 每个会话维护一个消息 ZSet，score=msgId，value=消息内容JSON，上限500条，TTL 7天
 * 小群（< 1000人）：key = msg:buf:{sessionId}
 * 大群（>= 1000人）：key = msg:buf:shard:{sessionId}:{userId % N}，每个分片内容相同（异步复制）
 *
 * 查未读数：ZCOUNT key (lastReadMsgId +inf
 * 取最后一条消息：ZREVRANGE key 0 0（score 最大的那条）
 * ZSet 不存在 → 回源 DB COUNT + 取最新一条，不回写 ZSet（由消息模块维护）
 * </pre>
 */
@Slf4j
@Service
public class SessionServiceImpl implements SessionService {

    private static final int SESSION_LIST_TTL_BASE   = 300;
    private static final int SESSION_LIST_TTL_JITTER = 120;

    private final SessionMapper        sessionMapper;
    private final SessionMemberMapper  sessionMemberMapper;
    private final UserMapper           userMapper;
    private final UserService          userService;
    private final ChatMessageMapper    chatMessageMapper;
    private final StringRedisTemplate  redisTemplate;
    private final RedissonClient       redissonClient;
    private final SnowflakeIdGenerator snowflakeIdGenerator;
    private final ObjectMapper         objectMapper;
    private final ThreadPoolExecutor   cacheRebuildExecutor;
    private final SessionCacheService  sessionCacheService;

    @Value("${chat.group.write-fanout-threshold:1000}")
    private int writeFanoutThreshold;

    /** 群聊人数上限，超出时建群/拉人均拒绝 */
    @Value("${chat.group.max-member-count:2000}")
    private int maxMemberCount;

    public SessionServiceImpl(
            SessionMapper sessionMapper,
            SessionMemberMapper sessionMemberMapper,
            UserMapper userMapper,
            UserService userService,
            ChatMessageMapper chatMessageMapper,
            StringRedisTemplate redisTemplate,
            RedissonClient redissonClient,
            SnowflakeIdGenerator snowflakeIdGenerator,
            ObjectMapper objectMapper,
            @Qualifier("cacheRebuildExecutor") ThreadPoolExecutor cacheRebuildExecutor,
            SessionCacheService sessionCacheService) {
        this.sessionMapper        = sessionMapper;
        this.sessionMemberMapper  = sessionMemberMapper;
        this.userMapper           = userMapper;
        this.userService          = userService;
        this.chatMessageMapper    = chatMessageMapper;
        this.redisTemplate        = redisTemplate;
        this.redissonClient       = redissonClient;
        this.snowflakeIdGenerator = snowflakeIdGenerator;
        this.objectMapper         = objectMapper;
        this.cacheRebuildExecutor = cacheRebuildExecutor;
        this.sessionCacheService  = sessionCacheService;
    }

    // =========================================================================
    // getReadPositions — 拉取已读位置
    // =========================================================================

    /**
     * 分页拉取当前用户各会话的已读位置（lastReadMsgId）
     *
     * <h3>数据库表设计</h3>
     * <pre>
     * 表：session_member
     * ┌─────────────────────┬──────────────┬──────────────────────────────────────────────────┐
     * │ 字段                │ 类型         │ 说明                                             │
     * ├─────────────────────┼──────────────┼──────────────────────────────────────────────────┤
     * │ id                  │ BIGINT PK    │ 自增主键                                         │
     * │ session_id          │ BIGINT       │ 关联 session.id，INDEX                           │
     * │ user_id             │ BIGINT       │ 关联 user.id，INDEX                              │
     * │ last_read_msg_id    │ BIGINT NULL  │ 最后已读消息 ID（雪花ID），NULL=从未读过          │
     * │                     │              │ 只增不减：UPDATE ... WHERE last_read_msg_id < ?  │
     * │ role                │ TINYINT      │ 1=普通成员 2=管理员 3=群主                       │
     * │ is_muted            │ TINYINT      │ 是否被禁言                                       │
     * │ is_pinned           │ TINYINT      │ 是否置顶                                         │
     * │ is_disturb          │ TINYINT      │ 是否免打扰                                       │
     * │ joined_at           │ DATETIME     │ 加入时间                                         │
     * │ left_at             │ DATETIME NULL│ 退出时间，NULL=在群中                            │
     * │ created_at          │ DATETIME     │ 创建时间                                         │
     * │ updated_at          │ DATETIME     │ 更新时间                                         │
     * └─────────────────────┴──────────────┴──────────────────────────────────────────────────┘
     * 联合唯一索引：UNIQUE KEY uk_session_user (session_id, user_id)
     * 查询索引：    INDEX idx_user_session (user_id, session_id DESC)
     *
     * 表：session
     * ┌─────────────────────┬──────────────┬──────────────────────────────────────────────────┐
     * │ session_no          │ VARCHAR(64)  │ 对外唯一标识，单聊="{minId}_{maxId}"，群聊="g_{id}"│
     * │ status              │ TINYINT      │ 1=正常 2=已解散                                  │
     * │ deleted_at          │ DATETIME NULL│ 软删除时间                                       │
     * └─────────────────────┴──────────────┴──────────────────────────────────────────────────┘
     * </pre>
     *
     * <h3>实现细节</h3>
     * <pre>
     * 1. 纯 DB 查询，不走 Redis 缓存
     *    - 已读位置是用户私有数据，不同用户互不干扰，无热点问题
     *    - 数据量小（每用户最多几百个会话），DB 查询足够快
     *    - 避免缓存与 DB 不一致导致已读位置回退
     *
     * 2. 分页策略：LIMIT offset, size（offset 分页）
     *    - 每页约 15 条，前端首次加载时分批拉取
     *    - 按 session_id DESC 排序，最近活跃的会话优先返回
     *    - 深度分页（offset > 500）时性能可接受，因为单用户会话数通常 < 200
     *
     * 3. 过滤条件
     *    - sm.left_at IS NULL：只返回当前在群中的会话
     *    - s.deleted_at IS NULL AND s.status = 1：过滤已解散/软删除的会话
     *
     * 4. lastReadMsgId 语义
     *    - NULL：用户从未在该会话中上报过已读，前端应把所有消息视为未读
     *    - 非 NULL：用户最后一次上报已读时的消息 ID（雪花ID，单调递增）
     *    - 该字段只增不减，由 updateLastReadMsgId 保证（WHERE last_read_msg_id < ?）
     *
     * 5. 调用时机
     *    - 前端首次加载 / 重新登录时调用，把本地缓存的 lastReadMsgId 与服务端对齐
     *    - 之后的增量更新通过 WebSocket 推送，不需要再轮询此接口
     * </pre>
     *
     * @param userId 当前用户 ID
     * @param page   页码（从 1 开始）
     * @param size   每页条数（建议 15，最大 50）
     * @return 分页结果，每条包含 sessionNo + lastReadMsgId
     */
    @Override
    public PageResult<ReadPositionVO> getReadPositions(Long userId, int page, int size) {
        page = Math.max(1, page);
        size = Math.min(Math.max(1, size), 50);   // 单页最多 50 条，防止过大查询
        int offset = (page - 1) * size;

        // 1. 查分页数据
        List<LastReadPositionDTO> rows =
                sessionMemberMapper.selectLastReadPositions(userId, offset, size);

        // 2. 查总数（用于前端判断是否还有下一页）
        long total = sessionMemberMapper.countValidSessions(userId);

        // 3. DTO → VO
        List<ReadPositionVO> voList = rows.stream()
                .map(dto -> {
                    ReadPositionVO vo = new ReadPositionVO();
                    vo.setSessionNo(dto.getSessionNo());
                    vo.setLastReadMsgId(dto.getLastReadMsgId());
                    return vo;
                })
                .collect(Collectors.toList());

        // 4. 组装分页结果
        PageResult<ReadPositionVO> result = new PageResult<>();
        result.setList(voList);
        result.setTotal(total);
        result.setHasMore((long) page * size < total);
        result.setNextCursor(null);   // 此接口使用 offset 分页，不需要游标
        return result;
    }

    // =========================================================================
    // listSessions
    // =========================================================================

    @Override
    public SessionListVO listSessions(Long userId, int page, int size, Map<String, Long> lastReadMsgIds) {
        page = Math.max(1, page);
        size = Math.min(Math.max(1, size), 100);

        String cacheKey = buildSessionListKey(userId, page, size);

        // 1. 读分页缓存
        String cached = redisTemplate.opsForValue().get(cacheKey);
        if (cached != null) {
            try {
                List<SessionVO> list = objectMapper.readValue(cached, new TypeReference<>() {});
                // 把前端传来的 lastReadMsgId 写入 VO
                applyClientLastRead(list, lastReadMsgIds);
                // 实时填充未读数 + 最后一条消息
                fillUnreadAndLastMsg(userId, list);
                long total = sessionMapper.countByUserId(userId);
                return buildResult(list, page, size, total);
            } catch (Exception e) {
                log.warn("[SessionList] 缓存反序列化失败, userId={}", userId, e);
            }
        }

        // 2. 未命中，尝试抢锁
        String lockKey = RedisKeyConst.SESSION_LIST_REBUILD_LOCK + userId + ":" + page + ":" + size;
        RLock lock = redissonClient.getLock(lockKey);
        boolean locked = false;
        try {
            locked = lock.tryLock(0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 3b. 没抢到锁，直接返回空，前端轮询
        if (!locked) {
            return SessionListVO.of(buildPageResult(Collections.emptyList(), page, size, 0L), 0);
        }

        try {
            // 3a. 抢到锁，Double Check
            cached = redisTemplate.opsForValue().get(cacheKey);
            if (cached != null) {
                try {
                    List<SessionVO> list = objectMapper.readValue(cached, new TypeReference<>() {});
                    applyClientLastRead(list, lastReadMsgIds);
                    fillUnreadAndLastMsg(userId, list);
                    long total = sessionMapper.countByUserId(userId);
                    return buildResult(list, page, size, total);
                } catch (Exception e) {
                    log.warn("[SessionList] Double Check 反序列化失败", e);
                }
            }

            // 查 DB 重建列表
            int offset = (page - 1) * size;
            List<Session> sessions = sessionMapper.selectPageByUserId(userId, offset, size);
            long total = sessionMapper.countByUserId(userId);

            List<SessionVO> voList = sessions.stream()
                    .map(s -> {
                        SessionMember member = sessionMemberMapper.selectMember(s.getId(), userId);
                        return toVO(s, member);
                    })
                    .collect(Collectors.toList());

            // 单源异步回溯：异步写缓存，当前线程不等
            final List<SessionVO> toCache = new ArrayList<>(voList);
            cacheRebuildExecutor.execute(() -> {
                try {
                    String json = objectMapper.writeValueAsString(toCache);
                    int ttl = SESSION_LIST_TTL_BASE
                            + ThreadLocalRandom.current().nextInt(SESSION_LIST_TTL_JITTER);
                    redisTemplate.opsForValue().set(cacheKey, json, ttl, TimeUnit.SECONDS);
                } catch (Exception ex) {
                    log.error("[SessionList] 异步写缓存失败, userId={}", userId, ex);
                }
            });

            // 当前线程实时填充未读数 + 最后一条消息后返回
            applyClientLastRead(voList, lastReadMsgIds);
            fillUnreadAndLastMsg(userId, voList);
            return buildResult(voList, page, size, total);

        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    // =========================================================================
    // 未读数 + 最后一条消息填充
    // =========================================================================

    /**
     * 把前端传来的 lastReadMsgId 覆盖写入 VO
     * （优先用前端值，前端没传则保留 toVO 时从 session_member 读到的值）
     */
    private void applyClientLastRead(List<SessionVO> list, Map<String, Long> lastReadMsgIds) {
        if (lastReadMsgIds == null || lastReadMsgIds.isEmpty()) return;
        for (SessionVO vo : list) {
            Long clientVal = lastReadMsgIds.get(vo.getSessionNo());
            if (clientVal != null) {
                vo.setLastReadMsgId(clientVal);
            }
        }
    }

    /**
     * 对列表中每个会话实时填充：
     * 1. unreadCount
     *    - ZSet 不存在 / 为空                        → 回源 DB count
     *    - ZSet 存在但 lastReadMsgId < ZSet 最小score → 回源 DB count（ZSet 数据不完整）
     *    - 否则                                       → ZCOUNT (lastReadMsgId +inf
     * 2. lastMsgContent / lastMsgAt
     *    - ZSet 非空 → 取 score 最大的那条
     *    - ZSet 为空 → 回源 DB 取最新一条
     */
    private void fillUnreadAndLastMsg(Long userId, List<SessionVO> list) {
        if (list == null || list.isEmpty()) return;

        for (SessionVO vo : list) {
            Long sessionId   = vo.getSessionId();
            Long lastReadId  = vo.getLastReadMsgId() != null ? vo.getLastReadMsgId() : 0L;
            int  memberCount = vo.getMemberCount() != null ? vo.getMemberCount() : 0;

            String zsetKey = resolveZSetKey(sessionId, memberCount, userId);

            // --- 取 ZSet 最小 score（最老消息的 msgId）和最大 score（最新消息）---
            // rangeWithScores(key, 0, 0) = 最小 score 那条
            Set<ZSetOperations.TypedTuple<String>> minSet =
                    redisTemplate.opsForZSet().rangeWithScores(zsetKey, 0, 0);
            Set<ZSetOperations.TypedTuple<String>> maxSet =
                    redisTemplate.opsForZSet().reverseRangeWithScores(zsetKey, 0, 0);

            boolean zsetExists = minSet != null && !minSet.isEmpty();

            // --- 未读数 ---
            if (!zsetExists) {
                // ZSet 不存在或为空，回源 DB
                vo.setUnreadCount(chatMessageMapper.countUnread(sessionId, lastReadId));
            } else {
                long minScore = minSet.iterator().next().getScore().longValue();
                if (lastReadId < minScore) {
                    // lastReadMsgId 比 ZSet 最小 score 还小，ZSet 数据不完整，回源 DB
                    vo.setUnreadCount(chatMessageMapper.countUnread(sessionId, lastReadId));
                } else {
                    // ZSet 数据完整，直接 ZCOUNT
                    Long zcount = redisTemplate.opsForZSet().count(
                            zsetKey, (double) lastReadId + 1, Double.MAX_VALUE);
                    vo.setUnreadCount(zcount != null ? zcount.intValue() : 0);
                }
            }

            // --- 最后一条消息 ---
            if (zsetExists && maxSet != null && !maxSet.isEmpty()) {
                // ZSet 非空，取 score 最大（最新）的那条
                String msgJson = maxSet.iterator().next().getValue();
                if (msgJson != null) {
                    try {
                        Map<String, Object> msgMap = objectMapper.readValue(
                                msgJson, new TypeReference<>() {});
                        Object content = msgMap.get("content");
                        if (content != null) {
                            vo.setLastMsgContent(content.toString());
                        }
                        Object createdAt = msgMap.get("createdAt");
                        if (createdAt != null && vo.getLastMsgAt() == null) {
                            vo.setLastMsgAt(objectMapper.convertValue(createdAt, LocalDateTime.class));
                        }
                    } catch (Exception e) {
                        log.warn("[SessionList] 解析 ZSet 消息 JSON 失败, sessionId={}", sessionId, e);
                    }
                }
            } else {
                // ZSet 为空，回源 DB 取最新一条
                var latest = chatMessageMapper.selectLatest(sessionId, 1);
                if (!latest.isEmpty()) {
                    vo.setLastMsgContent(latest.get(0).getContent());
                    vo.setLastMsgAt(latest.get(0).getCreatedAt());
                }
            }
        }
    }

    /**
     * 根据会话成员数决定使用哪个 ZSet key
     * 小群（< threshold）：msg:buf:{sessionId}
     * 大群（>= threshold）：msg:buf:shard:{sessionId}:{userId % N}
     */
    private String resolveZSetKey(Long sessionId, int memberCount, Long userId) {
        if (memberCount >= writeFanoutThreshold) {
            int shard = (int) (userId % RedisKeyConst.MSG_BUF_SHARD_COUNT);
            return RedisKeyConst.MSG_BUF_SHARD + sessionId + ":" + shard;
        }
        return RedisKeyConst.MSG_BUF + sessionId;
    }

    // =========================================================================
    // getOrCreateSingleSession
    // =========================================================================

    @Override
    @Transactional(rollbackFor = Exception.class)
    public SessionVO getOrCreateSingleSession(Long userId, CreateSingleSessionDTO dto) {
        User target = userMapper.selectOne(
                new LambdaQueryWrapper<User>()
                        .eq(User::getUserNo, dto.getTargetUserNo())
                        .isNull(User::getDeletedAt));
        if (target == null) throw new BizException(ResultCode.USER_NOT_FOUND);

        long minId = Math.min(userId, target.getId());
        long maxId = Math.max(userId, target.getId());
        String sessionNo = minId + "_" + maxId;

        Session session = sessionMapper.selectSingleSession(sessionNo);
        if (session != null) {
            SessionMember myMember = sessionMemberMapper.selectMember(session.getId(), userId);
            return toVO(session, myMember);
        }

        session = new Session();
        session.setSessionNo(sessionNo);
        session.setType(1);
        session.setName(target.getNickname() != null ? target.getNickname() : target.getUsername());
        session.setAvatarUrl(target.getAvatarUrl());
        session.setOwnerId(userId);
        session.setMemberCount(2);
        session.setStatus(1);
        sessionMapper.insert(session);

        // 新会话 ID 写入布隆过滤器，防止缓存穿透
        redissonClient.<Long>getBloomFilter(RedisKeyConst.BLOOM_SESSION_IDS).add(session.getId());

        insertMember(session.getId(), userId, 1);
        insertMember(session.getId(), target.getId(), 1);

        evictSessionListCache(userId);
        evictSessionListCache(target.getId());

        return toVO(session, sessionMemberMapper.selectMember(session.getId(), userId));
    }

    // =========================================================================
    // createGroupSession
    // =========================================================================

    @Override
    @Transactional(rollbackFor = Exception.class)
    public SessionVO createGroupSession(Long userId, CreateGroupSessionDTO dto) {
        List<User> members = userMapper.selectList(
                new LambdaQueryWrapper<User>()
                        .in(User::getUserNo, dto.getMemberUserNos())
                        .isNull(User::getDeletedAt));

        Set<Long> memberIds = members.stream().map(User::getId).collect(Collectors.toSet());
        memberIds.add(userId);

        // 建群时校验人数上限（含群主自己）
        if (memberIds.size() > maxMemberCount) {
            throw new BizException(ResultCode.GROUP_MEMBER_LIMIT_EXCEEDED);
        }

        String sessionNo = "g_" + snowflakeIdGenerator.nextId();
        Session session = new Session();
        session.setSessionNo(sessionNo);
        session.setType(2);
        session.setName(dto.getName());
        session.setOwnerId(userId);
        session.setMemberCount(memberIds.size());
        session.setStatus(1);
        sessionMapper.insert(session);

        // 新会话 ID 写入布隆过滤器，防止缓存穿透
        redissonClient.<Long>getBloomFilter(RedisKeyConst.BLOOM_SESSION_IDS).add(session.getId());

        for (Long memberId : memberIds) {
            insertMember(session.getId(), memberId, memberId.equals(userId) ? 3 : 1);
        }

        memberIds.forEach(this::evictSessionListCache);

        return toVO(session, sessionMemberMapper.selectMember(session.getId(), userId));
    }

    // =========================================================================
    // getSessionDetail
    // =========================================================================

    @Override
    public SessionVO getSessionDetail(Long userId, Long sessionId) {
        // 走缓存（session:info:{sessionId}），TTL 5min+抖动，未命中回源 DB
        Session session = sessionCacheService.getSessionById(sessionId);
        if (session == null) throw new BizException(ResultCode.SESSION_NOT_FOUND);

        SessionMember member = sessionMemberMapper.selectMember(sessionId, userId);
        if (member == null) throw new BizException(ResultCode.NOT_IN_SESSION);

        return toVO(session, member);
    }

    // =========================================================================
    // leaveSession
    // =========================================================================

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void leaveSession(Long userId, Long sessionId) {
        Session session = sessionMapper.selectById(sessionId);
        if (session == null) throw new BizException(ResultCode.SESSION_NOT_FOUND);

        SessionMember member = sessionMemberMapper.selectMember(sessionId, userId);
        if (member == null) throw new BizException(ResultCode.NOT_IN_SESSION);

        LocalDateTime now = LocalDateTime.now();
        if (session.getType() == 1) {
            sessionMemberMapper.softDelete(sessionId, userId, now);
            sessionCacheService.removeMember(sessionId, userId);
        } else {
            if (userId.equals(session.getOwnerId())) {
                // 群主解散群，整个 Set 直接删掉
                sessionMemberMapper.softDeleteAll(sessionId, now);
                session.setStatus(2);
                sessionMapper.updateById(session);
                sessionCacheService.evictMembers(sessionId);
            } else {
                sessionMemberMapper.softDelete(sessionId, userId, now);
                session.setMemberCount(sessionMemberMapper.countMembers(sessionId));
                sessionMapper.updateById(session);
                sessionCacheService.removeMember(sessionId, userId);
            }
        }
        evictSessionListCache(userId);
    }

    // =========================================================================
    // inviteMembers
    // =========================================================================

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void inviteMembers(Long userId, Long sessionId, List<String> memberUserNos) {
        Session session = sessionMapper.selectById(sessionId);
        if (session == null) throw new BizException(ResultCode.SESSION_NOT_FOUND);
        if (session.getType() != 2) throw new BizException(ResultCode.FORBIDDEN);
        if (sessionMemberMapper.selectMember(sessionId, userId) == null)
            throw new BizException(ResultCode.NOT_IN_SESSION);

        // ---------------------------------------------------------------
        // 第一层防线：分布式锁（按 sessionId 粒度）
        // 保证同一个群同一时刻只有一个拉人请求在执行，消除并发窗口。
        // 等待最多 3 秒，超时说明有其他请求正在操作，直接拒绝。
        // ---------------------------------------------------------------
        String lockKey = "lock:invite:session:" + sessionId;
        RLock lock = redissonClient.getLock(lockKey);
        boolean locked;
        try {
            locked = lock.tryLock(3, 10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BizException(ResultCode.SERVER_ERROR);
        }
        if (!locked) {
            throw new BizException(ResultCode.TOO_MANY_REQUESTS);
        }

        try {
            // 加锁后重新读一次最新人数，防止锁等待期间已被其他请求修改
            session = sessionMapper.selectById(sessionId);

            List<User> newMembers = userMapper.selectList(
                    new LambdaQueryWrapper<User>()
                            .in(User::getUserNo, memberUserNos)
                            .isNull(User::getDeletedAt));

            // 计算本次实际新增人数（排除已在群中的）
            List<User> toAdd = new ArrayList<>();
            for (User u : newMembers) {
                if (sessionMemberMapper.selectMember(sessionId, u.getId()) == null) {
                    toAdd.add(u);
                }
            }
            if (toAdd.isEmpty()) return;

            // 锁内前置校验：当前人数 + 新增人数 > 上限则直接拒绝
            if (session.getMemberCount() + toAdd.size() > maxMemberCount) {
                throw new BizException(ResultCode.GROUP_MEMBER_LIMIT_EXCEEDED);
            }

            // 批量插入新成员
            for (User u : toAdd) {
                insertMember(sessionId, u.getId(), 1);
                evictSessionListCache(u.getId());
                // 直接往 Set 里加，Set 不存在时跳过等下次重建
                sessionCacheService.addMember(sessionId, u.getId());
            }

            // ---------------------------------------------------------------
            // 第二层防线：带上限条件的 UPDATE（数据库乐观兜底）
            // UPDATE session SET member_count = member_count + delta
            //   WHERE id = ? AND member_count + delta <= maxMemberCount
            // 返回 0 说明 DB 层面已超限（Redis 故障导致锁失效的极端场景），
            // 抛异常触发事务回滚，撤销上方所有 insertMember 操作。
            // ---------------------------------------------------------------
            int affected = sessionMapper.updateMemberCountWithLimit(
                    sessionId, toAdd.size(), maxMemberCount);
            if (affected == 0) {
                throw new BizException(ResultCode.GROUP_MEMBER_LIMIT_EXCEEDED);
            }

        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    // =========================================================================
    // kickMember
    // =========================================================================

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void kickMember(Long operatorId, Long sessionId, Long targetUserId) {
        Session session = sessionMapper.selectById(sessionId);
        if (session == null) throw new BizException(ResultCode.SESSION_NOT_FOUND);

        SessionMember operator = sessionMemberMapper.selectMember(sessionId, operatorId);
        if (operator == null) throw new BizException(ResultCode.NOT_IN_SESSION);
        if (operator.getRole() < 2) throw new BizException(ResultCode.NO_ADMIN_PERMISSION);

        SessionMember target = sessionMemberMapper.selectMember(sessionId, targetUserId);
        if (target == null) throw new BizException(ResultCode.NOT_IN_SESSION);
        if (target.getRole() >= operator.getRole()) throw new BizException(ResultCode.NO_ADMIN_PERMISSION);

        sessionMemberMapper.softDelete(sessionId, targetUserId, LocalDateTime.now());
        session.setMemberCount(sessionMemberMapper.countMembers(sessionId));
        sessionMapper.updateById(session);

        evictSessionListCache(targetUserId);
        sessionCacheService.removeMember(sessionId, targetUserId);
    }

    // =========================================================================
    // updateGroupSession — 修改群聊信息
    // =========================================================================

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void updateGroupSession(Long operatorId, Long sessionId, UpdateGroupSessionDTO dto) {
        Session session = sessionCacheService.getSessionById(sessionId);
        if (session == null) throw new BizException(ResultCode.SESSION_NOT_FOUND);
        if (session.getType() != 2) throw new BizException(ResultCode.FORBIDDEN);

        // 鉴权：仅群主或管理员可修改
        SessionMember operator = sessionMemberMapper.selectMember(sessionId, operatorId);
        if (operator == null) throw new BizException(ResultCode.NOT_IN_SESSION);
        if (operator.getRole() < 2) throw new BizException(ResultCode.NO_ADMIN_PERMISSION);

        // 只更新非 null 字段
        boolean changed = false;
        if (dto.getName() != null) {
            session.setName(dto.getName());
            changed = true;
        }
        if (dto.getAvatarUrl() != null) {
            session.setAvatarUrl(dto.getAvatarUrl());
            changed = true;
        }
        if (changed) {
            sessionMapper.updateById(session);
            // 失效会话信息缓存，下次 getSessionDetail 重建
            sessionCacheService.evict(sessionId);
            // 失效操作人的会话列表缓存，其他成员等自然过期
            evictSessionListCache(operatorId);
        }
    }

    // =========================================================================
    // getMembers — 获取会话成员列表
    // =========================================================================

    /**
     * 获取会话成员列表
     *
     * <h3>缓存策略</h3>
     * <pre>
     * 1. 布隆过滤器拦截非法 sessionId（防穿透）
     * 2. 查 Redis Set（session:members:{sessionId}）取所有 userId
     *    命中 → 批量查用户信息（UserMapper.selectBatchByIds）→ 组装 VO 返回
     *    未命中 → 查 DB 取 userId → 异步重建 Set → 批量查用户信息 → 返回
     * 3. 批量查用户信息走 UserCacheService（单个用户有独立缓存防护）
     * </pre>
     *
     * <h3>鉴权</h3>
     * 调用方必须是该会话的在群成员，否则抛 NOT_IN_SESSION
     */
    @Override
    public List<SessionMemberVO> getMembers(Long userId, Long sessionId) {
        // ① 鉴权：调用方必须在群中
        // 优先走 Redis Set SISMEMBER（O(1)），Set 不存在时再回源 DB 兜底，避免每次都打 DB
        String membersKey = RedisKeyConst.SESSION_MEMBERS + sessionId;
        Boolean setExists = redisTemplate.hasKey(membersKey);
        if (Boolean.TRUE.equals(setExists)) {
            // Set 存在，直接用 SISMEMBER 判断
            Boolean isMember = redisTemplate.opsForSet().isMember(membersKey, String.valueOf(userId));
            if (!Boolean.TRUE.equals(isMember)) throw new BizException(ResultCode.NOT_IN_SESSION);
        } else {
            // Set 不存在（缓存未建立），回源 DB 兜底
            SessionMember self = sessionMemberMapper.selectMember(sessionId, userId);
            if (self == null) throw new BizException(ResultCode.NOT_IN_SESSION);
        }

        // ② 从缓存（或 DB）取成员 userId 列表
        List<Long> memberUserIds = sessionCacheService.getMemberUserIds(sessionId);
        if (memberUserIds == null || memberUserIds.isEmpty()) {
            return Collections.emptyList();
        }

        // ④ 批量查用户信息（走缓存，Pipeline MGET + 未命中批量回源 DB）
        List<User> users = userService.batchGetUsers(memberUserIds);

        // ⑤ 查 userId → role 映射（用于填充角色字段）
        Map<Long, SessionMember> roleMap = sessionMemberMapper.selectMemberRoles(sessionId);

        // ⑥ 组装 VO
        return users.stream()
                .map(u -> {
                    SessionMemberVO vo = new SessionMemberVO();
                    vo.setUserNo(u.getUserNo());
                    vo.setUsername(u.getUsername());
                    vo.setNickname(u.getNickname());
                    vo.setAvatarUrl(u.getAvatarUrl());
                    vo.setBio(u.getBio());
                    vo.setGender(u.getGender());
                    SessionMember sm = roleMap.get(u.getId());
                    vo.setRole(sm != null ? sm.getRole() : 1);
                    return vo;
                })
                .collect(Collectors.toList());
    }

    // =========================================================================
    // 私有辅助方法
    // =========================================================================

    private SessionListVO buildResult(List<SessionVO> list, int page, int size, long total) {
        int totalUnread = list.stream()
                .mapToInt(vo -> vo.getUnreadCount() != null ? vo.getUnreadCount() : 0)
                .sum();
        return SessionListVO.of(buildPageResult(list, page, size, total), totalUnread);
    }

    private PageResult<SessionVO> buildPageResult(List<SessionVO> list, int page, int size, long total) {
        PageResult<SessionVO> r = new PageResult<>();
        r.setList(list);
        r.setTotal(total);
        r.setHasMore((long) page * size < total);
        r.setNextCursor(null);
        return r;
    }

    private String buildSessionListKey(Long userId, int page, int size) {
        return RedisKeyConst.USER_SESSION_LIST + userId + ":p" + page + ":s" + size;
    }

    /**
     * 失效某用户的会话列表分页缓存
     *
     * <p>注意事项：
     * <ul>
     *   <li>用 SCAN 替代 KEYS：KEYS 全量扫描会阻塞 Redis 单线程，高并发下影响所有命令响应；
     *       SCAN 游标分批扫描，每批 count=100，不阻塞主线程</li>
     *   <li>用 UNLINK 替代 DEL：DEL 同步回收内存，value 较大时有明显耗时；
     *       UNLINK 先从 keyspace 摘除 key（O(1)），实际内存回收交给后台线程异步完成</li>
     * </ul>
     */
    private void evictSessionListCache(Long userId) {
        String pattern = RedisKeyConst.USER_SESSION_LIST + userId + ":*";
        try {
            // SCAN 游标分批扫描，count=100 表示每批期望扫描的槽位数（实际返回数量不固定）
            ScanOptions options = ScanOptions.scanOptions().match(pattern).count(100).build();
            List<String> keys = new ArrayList<>();
            try (Cursor<String> cursor = redisTemplate.scan(options)) {
                cursor.forEachRemaining(keys::add);
            }
            if (!keys.isEmpty()) {
                // UNLINK 异步删除，不阻塞 Redis 主线程
                redisTemplate.unlink(keys);
            }
        } catch (Exception e) {
            log.warn("[SessionList] 失效缓存失败, userId={}", userId, e);
        }
    }

    private void insertMember(Long sessionId, Long userId, int role) {
        SessionMember member = new SessionMember();
        member.setSessionId(sessionId);
        member.setUserId(userId);
        member.setRole(role);
        member.setIsMuted(0);
        member.setIsPinned(0);
        member.setIsDisturb(0);
        member.setJoinedAt(LocalDateTime.now());
        sessionMemberMapper.insert(member);
    }

    /**
     * Session + SessionMember → SessionVO
     * 不含未读数和最后一条消息（由 fillUnreadAndLastMsg 实时填充）
     */
    private SessionVO toVO(Session session, SessionMember member) {
        SessionVO vo = new SessionVO();
        vo.setSessionNo(session.getSessionNo());
        vo.setType(session.getType());
        vo.setName(session.getName());
        vo.setAvatarUrl(session.getAvatarUrl());
        vo.setLastMsgContent(session.getLastMsgContent());
        vo.setLastMsgAt(session.getLastMsgAt());
        vo.setMemberCount(session.getMemberCount());
        vo.setUnreadCount(0);
        // 内部字段
        vo.setSessionId(session.getId());
        if (member != null) {
            vo.setIsPinned(member.getIsPinned() != null && member.getIsPinned() == 1);
            vo.setIsDisturb(member.getIsDisturb() != null && member.getIsDisturb() == 1);
            // 服务端存储的 lastReadMsgId 作为兜底，前端传来的值会在 applyClientLastRead 里覆盖
            vo.setLastReadMsgId(member.getLastReadMsgId());
        }
        return vo;
    }
}
