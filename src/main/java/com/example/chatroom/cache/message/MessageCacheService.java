package com.example.chatroom.cache.message;

import com.example.chatroom.cache.session.SessionCacheService;
import com.example.chatroom.common.constant.RedisKeyConst;
import com.example.chatroom.mq.dto.ChatMessageMQDTO;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 消息缓存服务
 *
 * <h3>消息缓冲区 ZSet</h3>
 * <pre>
 * score = msgId（雪花ID，天然有序）
 * value = 消息 JSON
 * 上限  = 按群人数分三档：小群500条 / 中群1000条 / 大群2000条，超出裁剪最老的
 * TTL   = MSG_BUF_TTL（7天）
 *
 * 小群（memberCount < writeFanoutThreshold）：
 *   key = msg:buf:{sessionId}
 *   写：单 key 写入
 *   读：直接读该 key
 *
 * 大群（memberCount >= writeFanoutThreshold）：
 *   key = msg:buf:shard:{sessionId}:{shard}，shard = userId % MSG_BUF_SHARD_COUNT
 *   写：fan-out 写全部 MSG_BUF_SHARD_COUNT 个分片（内容相同）
 *   读：按 userId % MSG_BUF_SHARD_COUNT 定位到自己的分片读
 *   删：遍历删全部分片
 * </pre>
 *
 * <h3>鉴权优化</h3>
 * 发消息前先 SISMEMBER 查群成员 Set，命中则直接通过，
 * Set 不存在时回源 DB（由 SessionCacheService.getMemberUserIds 处理）。
 *
 * <h3>幂等暂存</h3>
 * msgNo → msgId 映射，TTL 2分钟，用于重复请求返回已有 msgId。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MessageCacheService {

    private final StringRedisTemplate stringRedisTemplate;
    private final SessionCacheService sessionCacheService;
    private final ObjectMapper objectMapper;

    /** 大群写扩散阈值，与 SessionServiceImpl 保持一致 */
    @Value("${chat.group.write-fanout-threshold:1000}")
    private int writeFanoutThreshold;

    /**
     * Lua：原子写入消息到 ZSet，裁剪超出上限的旧消息，刷新 TTL
     * KEYS[1] = zsetKey
     * ARGV[1] = score（msgId）
     * ARGV[2] = value（消息JSON）
     * ARGV[3] = maxSize
     * ARGV[4] = ttlSeconds
     */
    private static final RedisScript<Void> ZADD_TRIM_SCRIPT = new DefaultRedisScript<>(
            "redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2]) " +
            "local size = redis.call('ZCARD', KEYS[1]) " +
            "if tonumber(size) > tonumber(ARGV[3]) then " +
            "  redis.call('ZREMRANGEBYRANK', KEYS[1], 0, tonumber(size) - tonumber(ARGV[3]) - 1) " +
            "end " +
            "redis.call('EXPIRE', KEYS[1], ARGV[4]) " +
            "return nil",
            Void.class
    );

    // =========================================================================
    // 消息写入 ZSet 缓存
    // =========================================================================

    /**
     * 将消息写入 ZSet 缓冲区
     *
     * <p>小群：写单个 key（msg:buf:{sessionId}）
     * <p>大群：fan-out 写全部 MSG_BUF_SHARD_COUNT 个分片，保证每个分片内容相同，
     * 读时按 userId % N 定位分片，不会出现某个分片缺数据的情况。
     */
    public void putMessage(ChatMessageMQDTO dto) {
        String value;
        try {
            value = objectMapper.writeValueAsString(dto);
        } catch (JsonProcessingException e) {
            log.error("[MessageCache] 序列化消息失败, msgId={}", dto.getMsgId(), e);
            return;
        }

        String scoreStr   = String.valueOf(dto.getMsgId());
        String maxSizeStr = String.valueOf(resolveMaxSize(dto.getMemberCount()));
        String ttlStr     = String.valueOf(RedisKeyConst.MSG_BUF_TTL);

        List<String> keys = resolveWriteKeys(dto.getSessionId(), dto.getMemberCount());
        for (String key : keys) {
            stringRedisTemplate.execute(ZADD_TRIM_SCRIPT, List.of(key),
                    scoreStr, value, maxSizeStr, ttlStr);
        }
    }

    /**
     * 回滚：从 ZSet 中删除指定消息（发送失败时调用）
     *
     * <p>小群删单 key，大群遍历删全部分片。
     * memberCount 未知时（发送端回滚场景）传 null，走保守策略：两种 key 都尝试删。
     */
    public void removeMessage(Long sessionId, Long msgId) {
        removeMessage(sessionId, msgId, null);
    }

    /**
     * 回滚：从 ZSet 中删除指定消息（带 memberCount，精确路由）
     */
    public void removeMessage(Long sessionId, Long msgId, Integer memberCount) {
        List<String> keys = resolveWriteKeys(sessionId, memberCount);
        for (String key : keys) {
            stringRedisTemplate.opsForZSet().removeRangeByScore(key, msgId, msgId);
        }
    }

    // =========================================================================
    // 消息读取 ZSet 缓存（按 userId 路由到对应分片）
    // =========================================================================

    /**
     * 从 ZSet 缓冲区读取消息（before 方向：score < cursor，取最新 size 条）
     *
     * <p>小群读 msg:buf:{sessionId}，大群按 userId % N 读对应分片。
     * 返回 null 表示 ZSet 不存在（需回源 DB）。
     */
    public List<ChatMessageMQDTO> getMessagesBefore(Long sessionId, Long userId,
                                                     Integer memberCount, Long cursor, int size) {
        String zsetKey = resolveReadKey(sessionId, userId, memberCount);
        if (!Boolean.TRUE.equals(stringRedisTemplate.hasKey(zsetKey))) {
            return null; // ZSet 不存在，回源 DB
        }
        double maxScore = cursor != null ? (double) cursor - 1 : Double.MAX_VALUE;
        Set<String> values = stringRedisTemplate.opsForZSet()
                .reverseRangeByScore(zsetKey, 0, maxScore, 0, size);
        return deserialize(values);
    }

    /**
     * 从 ZSet 缓冲区读取消息（after 方向：score > cursor，取最早 size 条）
     *
     * <p>返回 null 表示 ZSet 不存在（需回源 DB）。
     */
    public List<ChatMessageMQDTO> getMessagesAfter(Long sessionId, Long userId,
                                                    Integer memberCount, Long cursor, int size) {
        String zsetKey = resolveReadKey(sessionId, userId, memberCount);
        if (!Boolean.TRUE.equals(stringRedisTemplate.hasKey(zsetKey))) {
            return null; // ZSet 不存在，回源 DB
        }
        double minScore = cursor != null ? (double) cursor + 1 : 0;
        Set<String> values = stringRedisTemplate.opsForZSet()
                .rangeByScore(zsetKey, minScore, Double.MAX_VALUE, 0, size);
        return deserialize(values);
    }

    // =========================================================================
    // 鉴权：用户是否在会话中
    // =========================================================================

    /**
     * 判断用户是否在会话中（优先走 Redis SISMEMBER，Set 不存在时回源 DB）
     */
    public boolean isMember(Long sessionId, Long userId) {
        String setKey = RedisKeyConst.SESSION_MEMBERS + sessionId;

        // ① SISMEMBER，Set 存在且命中 → 直接返回 true
        Boolean isMember = stringRedisTemplate.opsForSet().isMember(setKey, String.valueOf(userId));
        if (Boolean.TRUE.equals(isMember)) {
            return true;
        }
        // ② key 存在但 SISMEMBER=false → 确实不在
        if (Boolean.TRUE.equals(stringRedisTemplate.hasKey(setKey))) {
            return false;
        }
        // ③ Set 不存在，回源 DB（顺带触发 Set 异步重建）
        List<Long> memberIds = sessionCacheService.getMemberUserIds(sessionId);
        if (memberIds == null) return false;
        return memberIds.contains(userId);
    }

    // =========================================================================
    // 幂等暂存：msgNo → msgId，TTL 2分钟
    // =========================================================================

    /**
     * SET NX 写入 msgNo → msgId，首次写入返回 true，重复返回 false
     */
    public boolean setMsgIdIfAbsent(String msgNo, Long msgId) {
        String key = RedisKeyConst.MSG_IDEM + msgNo;
        Boolean ok = stringRedisTemplate.opsForValue()
                .setIfAbsent(key, String.valueOf(msgId), 2, TimeUnit.MINUTES);
        return Boolean.TRUE.equals(ok);
    }

    /**
     * 获取已暂存的 msgId（重复请求时返回给前端）
     */
    public Long getMsgId(String msgNo) {
        String key = RedisKeyConst.MSG_IDEM + msgNo;
        String val = stringRedisTemplate.opsForValue().get(key);
        return val != null ? Long.parseLong(val) : null;
    }

    // =========================================================================
    // 内部：ZSet 上限分档
    // =========================================================================

    /**
     * 根据群人数返回对应档位的 ZSet 上限
     *
     * <pre>
     * 小群（< 100） ：500 条
     * 中群（100 ~ 999）：1000 条
     * 大群（>= 1000）：2000 条
     * memberCount 为 null 时降级用小群上限（保守策略）
     * </pre>
     */
    private int resolveMaxSize(Integer memberCount) {
        if (memberCount == null || memberCount < RedisKeyConst.MSG_BUF_MEDIUM_THRESHOLD) {
            return RedisKeyConst.MSG_BUF_MAX_SIZE_SMALL;
        }
        if (memberCount < RedisKeyConst.MSG_BUF_LARGE_THRESHOLD) {
            return RedisKeyConst.MSG_BUF_MAX_SIZE_MEDIUM;
        }
        return RedisKeyConst.MSG_BUF_MAX_SIZE_LARGE;
    }

    // =========================================================================
    // 内部：ZSet key 路由
    // =========================================================================

    /**
     * 写路由：返回需要写入的所有 ZSet key
     *
     * <p>小群：返回单个 [msg:buf:{sessionId}]
     * <p>大群：fan-out，返回全部 MSG_BUF_SHARD_COUNT 个分片 key
     * <p>memberCount 为 null（保守策略）：两种 key 全部返回，确保不遗漏
     */
    private List<String> resolveWriteKeys(Long sessionId, Integer memberCount) {
        if (memberCount != null && memberCount < writeFanoutThreshold) {
            // 小群：单 key
            return List.of(RedisKeyConst.MSG_BUF + sessionId);
        }
        if (memberCount != null) {
            // 大群：fan-out 写全部分片
            List<String> keys = new ArrayList<>(RedisKeyConst.MSG_BUF_SHARD_COUNT);
            for (int shard = 0; shard < RedisKeyConst.MSG_BUF_SHARD_COUNT; shard++) {
                keys.add(RedisKeyConst.MSG_BUF_SHARD + sessionId + ":" + shard);
            }
            return keys;
        }
        // memberCount 未知（保守策略）：小群 key + 所有分片 key 都删
        List<String> keys = new ArrayList<>(RedisKeyConst.MSG_BUF_SHARD_COUNT + 1);
        keys.add(RedisKeyConst.MSG_BUF + sessionId);
        for (int shard = 0; shard < RedisKeyConst.MSG_BUF_SHARD_COUNT; shard++) {
            keys.add(RedisKeyConst.MSG_BUF_SHARD + sessionId + ":" + shard);
        }
        return keys;
    }

    /**
     * 读路由：返回当前用户应该读的 ZSet key
     *
     * <p>小群读 msg:buf:{sessionId}，大群按 userId % N 读对应分片。
     * memberCount 为 null 时降级读小群 key（兜底）。
     */
    private String resolveReadKey(Long sessionId, Long userId, Integer memberCount) {
        if (memberCount != null && memberCount >= writeFanoutThreshold && userId != null) {
            int shard = (int) (userId % RedisKeyConst.MSG_BUF_SHARD_COUNT);
            return RedisKeyConst.MSG_BUF_SHARD + sessionId + ":" + shard;
        }
        return RedisKeyConst.MSG_BUF + sessionId;
    }

    // =========================================================================
    // 历史消息翻阅缓存（session 级公共 ZSet，score=msgId，value=消息JSON）
    // =========================================================================

    /**
     * 从历史消息 ZSet 中读取指定范围的消息（before 方向）
     *
     * <p>key = msg:history:{sessionId}，score = msgId。
     * 取 score &lt; cursor 的最新 size 条（倒序），cursor=null 表示取最新一页。
     * 返回 null 表示 ZSet 不存在（key 不存在），调用方需回源 DB。
     * 返回空列表表示 ZSet 存在但该范围内没有数据。
     *
     * @param sessionId 会话 ID
     * @param cursor    翻页游标（before 方向的 msgId），null 表示首次加载
     * @param size      每页条数
     * @return 消息列表（倒序，最新的在前），null 表示 ZSet 不存在
     */
    public List<com.example.chatroom.module.message.domain.entity.ChatMessage> getHistoryMessages(
            Long sessionId, Long cursor, int size) {
        String key = RedisKeyConst.MSG_HISTORY + sessionId;
        if (!Boolean.TRUE.equals(stringRedisTemplate.hasKey(key))) {
            return null; // ZSet 不存在，回源 DB
        }
        double maxScore = cursor != null ? (double) cursor - 1 : Double.MAX_VALUE;
        Set<String> values = stringRedisTemplate.opsForZSet()
                .reverseRangeByScore(key, 0, maxScore, 0, size);
        if (values == null || values.isEmpty()) {
            return null; // 该范围无数据，与 key 不存在统一返回 null，调用方回源 DB
        }
        return values.stream()
                .map(v -> {
                    try {
                        return objectMapper.readValue(v,
                                com.example.chatroom.module.message.domain.entity.ChatMessage.class);
                    } catch (JsonProcessingException e) {
                        log.warn("[MessageCache] 历史缓存反序列化失败: {}", v, e);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * 将 DB 回源的消息批量写入历史消息 ZSet
     *
     * <p>score = msgId，value = 消息 JSON。
     * 每次写入后刷新 TTL = {@link RedisKeyConst#MSG_HISTORY_TTL}（10分钟）。
     * 写入失败只打 warn 日志，不影响主流程。
     *
     * @param sessionId 会话 ID
     * @param messages  从 DB 查出的消息列表
     */
    public void putHistoryMessages(Long sessionId,
            List<com.example.chatroom.module.message.domain.entity.ChatMessage> messages) {
        if (messages == null || messages.isEmpty()) return;
        String key = RedisKeyConst.MSG_HISTORY + sessionId;
        try {
            Set<org.springframework.data.redis.core.ZSetOperations.TypedTuple<String>> tuples =
                    new java.util.HashSet<>(messages.size());
            for (com.example.chatroom.module.message.domain.entity.ChatMessage msg : messages) {
                String json = objectMapper.writeValueAsString(msg);
                tuples.add(org.springframework.data.redis.core.ZSetOperations.TypedTuple
                        .of(json, (double) msg.getId()));
            }
            stringRedisTemplate.opsForZSet().add(key, tuples);
            stringRedisTemplate.expire(key, RedisKeyConst.MSG_HISTORY_TTL, TimeUnit.SECONDS);
            log.debug("[MessageCache] 写入历史缓存 {} 条, sessionId={}", messages.size(), sessionId);
        } catch (JsonProcessingException e) {
            log.warn("[MessageCache] 历史缓存序列化失败, sessionId={}", sessionId, e);
        }
    }

    /**
     * 撤回消息：Pipeline 合并删除热消息 ZSet + 历史消息 ZSet，一次网络往返
     *
     * <p>热消息 ZSet 可能有多个 key（大群分片），历史消息 ZSet 固定一个 key，
     * 全部打包进同一个 Pipeline 批量执行，减少 RTT。
     *
     * @param sessionId   会话 ID
     * @param msgId       被撤回的消息 ID
     * @param memberCount 群人数（用于路由热消息 ZSet 分片），null 时保守删全部
     */
    public void revokeMessageCache(Long sessionId, Long msgId, Integer memberCount) {
        List<String> hotKeys = resolveWriteKeys(sessionId, memberCount);
        String historyKey = RedisKeyConst.MSG_HISTORY + sessionId;
        double score = (double) msgId;

        try {
            stringRedisTemplate.executePipelined((org.springframework.data.redis.core.RedisCallback<Object>) connection -> {
                // 删热消息 ZSet（小群 1 个 key，大群 4 个分片 key）
                for (String key : hotKeys) {
                    connection.zSetCommands().zRemRangeByScore(
                            key.getBytes(),
                            org.springframework.data.domain.Range.closed(score, score));
                }
                // 删历史消息 ZSet
                connection.zSetCommands().zRemRangeByScore(
                        historyKey.getBytes(),
                        org.springframework.data.domain.Range.closed(score, score));
                return null;
            });
            log.debug("[MessageCache] Pipeline 撤回缓存, sessionId={}, msgId={}", sessionId, msgId);
        } catch (Exception e) {
            log.warn("[MessageCache] Pipeline 撤回缓存失败, sessionId={}, msgId={}", sessionId, msgId, e);
        }
    }

    // =========================================================================
    // 内部：ZSet value 反序列化
    // =========================================================================

    private List<ChatMessageMQDTO> deserialize(Set<String> values) {
        if (values == null) return Collections.emptyList();
        return values.stream()
                .map(v -> {
                    try {
                        return objectMapper.readValue(v, ChatMessageMQDTO.class);
                    } catch (JsonProcessingException e) {
                        log.warn("[MessageCache] 反序列化消息失败: {}", v, e);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
}
