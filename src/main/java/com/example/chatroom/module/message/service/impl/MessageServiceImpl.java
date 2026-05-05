package com.example.chatroom.module.message.service.impl;

import com.example.chatroom.cache.bloom.BloomFilterInitializer;
import com.example.chatroom.cache.message.MessageCacheService;
import com.example.chatroom.cache.session.SessionCacheService;
import com.example.chatroom.common.exception.BizException;
import com.example.chatroom.common.response.PageResult;
import com.example.chatroom.common.response.ResultCode;
import com.example.chatroom.common.util.SnowflakeIdGenerator;
import com.example.chatroom.module.message.domain.dto.SendMessageDTO;
import com.example.chatroom.module.message.domain.entity.ChatMessage;
import com.example.chatroom.module.message.domain.entity.LocalMsgOutbox;
import com.example.chatroom.module.message.domain.vo.MessageVO;
import com.example.chatroom.module.message.domain.vo.SendMessageVO;
import com.example.chatroom.module.message.mapper.ChatMessageMapper;
import com.example.chatroom.module.message.mapper.LocalMsgOutboxMapper;
import com.example.chatroom.module.message.service.MessageService;
import com.example.chatroom.module.session.domain.entity.Session;
import com.example.chatroom.module.session.domain.entity.SessionMember;
import com.example.chatroom.module.session.mapper.SessionMemberMapper;
import com.example.chatroom.mq.dto.ChatMessageMQDTO;
import com.example.chatroom.mq.producer.ChatMessageProducer;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 消息服务实现
 *
 * <h3>发送消息主链路</h3>
 * <pre>
 * ① 按用户ID限流（注解层，10条/秒）
 * ② 布隆过滤器：sessionId 是否存在
 * ③ 缓存鉴权：SISMEMBER 判断用户是否在会话中
 * ④ 幂等锁（Redisson）：同一 msgNo 只处理一次
 *    └─ Double Check：Redis 暂存 msgNo→msgId，重复请求直接返回
 * ⑤ 生成 msgId（雪花算法）
 * ⑥ 写消息到 ZSet 缓存（msg:buf:{sessionId}）
 * ⑦ 写本地事务表 local_msg_outbox（status=0，待发送）
 * ⑧ 发送到 RabbitMQ（Publisher Confirm）
 * ⑨ 返回 {msgId, msgNo, status="sending"}
 *
 * 失败回滚：⑥ 写缓存后若 ⑦⑧ 失败，删除 ZSet 中该消息
 * </pre>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MessageServiceImpl implements MessageService {

    private final LocalMsgOutboxMapper outboxMapper;
    private final ChatMessageMapper chatMessageMapper;
    private final SessionMemberMapper sessionMemberMapper;
    private final ChatMessageProducer mqProducer;
    private final SnowflakeIdGenerator idGenerator;
    private final SessionCacheService sessionCacheService;
    private final MessageCacheService messageCacheService;
    private final RedissonClient redissonClient;
    private final ObjectMapper objectMapper;

    private static final String MSG_IDEM_LOCK_PREFIX = "lock:msg:idem:";

    // =========================================================================
    // 发送消息
    // =========================================================================

    @Override
    @Transactional(rollbackFor = Exception.class)
    public SendMessageVO sendMessage(Long senderId, SendMessageDTO dto) {

        // ② 布隆过滤器：sessionId 是否存在（就绪才拦截）
        if (BloomFilterInitializer.sessionBloomReady.get()) {
            org.redisson.api.RBloomFilter<Long> bloom =
                    redissonClient.getBloomFilter(com.example.chatroom.common.constant.RedisKeyConst.BLOOM_SESSION_IDS);
            if (!bloom.contains(dto.getSessionId())) {
                throw new BizException(ResultCode.SESSION_NOT_FOUND);
            }
        }

        // ③ 缓存鉴权：SISMEMBER 判断用户是否在会话中
        if (!messageCacheService.isMember(dto.getSessionId(), senderId)) {
            throw new BizException(ResultCode.ILLEGAL_SESSION);
        }

        // ④ 幂等锁：同一 msgNo 只处理一次
        String lockKey = MSG_IDEM_LOCK_PREFIX + dto.getMsgNo();
        RLock lock = redissonClient.getLock(lockKey);
        try {
            // 最多等待 1s，持有 10s（看门狗不适合短链路，固定 leaseTime）
            if (!lock.tryLock(1, 10, TimeUnit.SECONDS)) {
                // 抢锁失败说明另一个线程正在处理同一条消息，等它写完再 Double Check
                Long existingMsgId = messageCacheService.getMsgId(dto.getMsgNo());
                if (existingMsgId != null) {
                    return buildDuplicateVO(dto.getMsgNo(), existingMsgId);
                }
                throw new BizException(ResultCode.MSG_DUPLICATE);
            }

            try {
                // Double Check：进锁后再查一次，防止并发重复处理
                Long existingMsgId = messageCacheService.getMsgId(dto.getMsgNo());
                if (existingMsgId != null) {
                    return buildDuplicateVO(dto.getMsgNo(), existingMsgId);
                }

                return doSend(senderId, dto);

            } finally {
                if (lock.isHeldByCurrentThread()) lock.unlock();
            }

        } catch (BizException e) {
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BizException(ResultCode.SERVER_ERROR);
        }
    }

    /**
     * 核心发送逻辑（已在幂等锁内）
     */
    private SendMessageVO doSend(Long senderId, SendMessageDTO dto) {
        // ⑤ 生成 msgId
        long msgId = idGenerator.nextId();

        // 构造 MQ DTO（后续写缓存和发 MQ 都用这个对象）
        Session session = sessionCacheService.getSessionById(dto.getSessionId());
        if (session == null) throw new BizException(ResultCode.SESSION_NOT_FOUND);

        ChatMessageMQDTO mqDTO = buildMQDTO(msgId, senderId, dto, session);

        // ⑥ 写消息到 ZSet 缓存（先写缓存，保证快速可见）
        messageCacheService.putMessage(mqDTO);

        // 暂存 msgNo → msgId（幂等 Double Check 用，TTL 2min）
        messageCacheService.setMsgIdIfAbsent(dto.getMsgNo(), msgId);

        try {
            // ⑦ 写本地事务表（status=0，待发送）
            LocalMsgOutbox outbox = buildOutbox(msgId, senderId, dto, mqDTO);
            outboxMapper.insert(outbox);

            // ⑧ 发送到 RabbitMQ（Publisher Confirm）
            mqProducer.sendWithConfirm(outbox.getPayload(), dto.getMsgNo());

        } catch (Exception e) {
            // ⑦⑧ 失败：回滚 ZSet 缓存中的消息
            log.error("[Message] 写Outbox或发MQ失败，回滚缓存, msgId={}, msgNo={}", msgId, dto.getMsgNo(), e);
            messageCacheService.removeMessage(dto.getSessionId(), msgId);
            throw new BizException(ResultCode.SERVER_ERROR);
        }

        // ⑨ 返回 sending
        SendMessageVO vo = new SendMessageVO();
        vo.setMsgId(msgId);
        vo.setMsgNo(dto.getMsgNo());
        vo.setStatus("sending");
        return vo;
    }

    // =========================================================================
    // listMessages：游标分页（ZSet 缓存命中 → miss 回源 DB）
    // =========================================================================

    @Override
    public PageResult<MessageVO> listMessages(Long userId, Long sessionId,
                                              Long cursor, int size, String direction) {
        // 鉴权：用户必须在会话中
        if (!messageCacheService.isMember(sessionId, userId)) {
            throw new BizException(ResultCode.ILLEGAL_SESSION);
        }

        // 限制 size 范围，防止恶意大查询
        int safeSize = Math.min(Math.max(size, 1), 50);

        // 获取 memberCount 用于大群/小群分片路由
        Session session = sessionCacheService.getSessionById(sessionId);
        Integer memberCount = session != null ? session.getMemberCount() : null;

        List<ChatMessageMQDTO> cached;
        boolean hitCache;

        if ("before".equals(direction)) {
            // before：加载比 cursor 更早的消息（cursor=null 表示首次加载，取最新）
            cached = messageCacheService.getMessagesBefore(sessionId, userId, memberCount, cursor, safeSize);
            if (cached == null) {
                // ZSet 不存在，直接回源 DB
                hitCache = false;
                cached = Collections.emptyList();
            } else {
                // ZSet 存在，但需要判断缓存是否覆盖了请求范围
                // 若 cursor 比 ZSet 最小 msgId 还小，说明请求的是更老的数据，需回源 DB
                Long minMsgId = messageCacheService.getMinMsgId(sessionId, userId, memberCount);
                hitCache = cursor == null || minMsgId == null || cursor > minMsgId || !cached.isEmpty();
            }
        } else {
            // after：加载比 cursor 更新的消息
            cached = messageCacheService.getMessagesAfter(sessionId, userId, memberCount, cursor, safeSize);
            hitCache = cached != null;
            if (!hitCache) cached = Collections.emptyList();
        }

        List<MessageVO> voList;
        boolean hasMore;
        Long nextCursor;

        if (hitCache && !cached.isEmpty()) {
            // 缓存命中
            voList = cached.stream().map(this::toVO).collect(Collectors.toList());
            // before 方向：返回的是倒序，需要正序给前端
            if ("before".equals(direction)) {
                Collections.reverse(voList);
            }
            nextCursor = "before".equals(direction)
                    ? voList.get(0).getMsgId()          // 最早那条的 msgId 作为下一页游标
                    : voList.get(voList.size() - 1).getMsgId();
            hasMore = cached.size() == safeSize;
        } else {
            // 缓存 miss，回源 DB
            List<ChatMessage> dbList;
            if ("before".equals(direction)) {
                dbList = cursor == null
                        ? chatMessageMapper.selectLatest(sessionId, safeSize)
                        : chatMessageMapper.selectBefore(sessionId, cursor, safeSize);
            } else {
                // after 方向：DB 没有专门的 after 查询，用 before 的反向逻辑
                // 实际业务中 after 主要用于实时追新，缓存通常命中，这里做兜底
                dbList = cursor == null
                        ? chatMessageMapper.selectLatest(sessionId, safeSize)
                        : chatMessageMapper.selectBefore(sessionId, cursor, safeSize);
            }
            voList = dbList.stream().map(this::toVO).collect(Collectors.toList());
            // selectLatest/selectBefore 返回的是 DESC，需要正序
            Collections.reverse(voList);
            hasMore = dbList.size() == safeSize;
            nextCursor = voList.isEmpty() ? null : voList.get(0).getMsgId();
        }

        return PageResult.of(voList, hasMore ? nextCursor : null, hasMore);
    }

    // =========================================================================
    // revokeMessage：2分钟内撤回
    // =========================================================================

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void revokeMessage(Long userId, Long msgId) {
        ChatMessage msg = chatMessageMapper.selectById(msgId);
        if (msg == null || msg.getStatus() != 1) {
            throw new BizException(ResultCode.MSG_NOT_FOUND);
        }
        // 只有发送者本人可以撤回
        if (!userId.equals(msg.getSenderId())) {
            throw new BizException(ResultCode.FORBIDDEN);
        }
        // 2分钟内才能撤回
        LocalDateTime deadline = msg.getCreatedAt().plusMinutes(2);
        if (LocalDateTime.now().isAfter(deadline)) {
            throw new BizException(ResultCode.MSG_REVOKE_TIMEOUT);
        }

        // 更新 DB status=2（撤回）
        ChatMessage update = new ChatMessage();
        update.setId(msgId);
        update.setStatus(2);
        chatMessageMapper.updateById(update);

        // 同步更新 ZSet 缓存中的消息（删除旧条目，写入撤回状态的新条目）
        // 简化处理：直接从 ZSet 删除，前端查询时会看到 DB 中 status=2 的记录
        messageCacheService.removeMessage(msg.getSessionId(), msgId);
    }

    // =========================================================================
    // markRead：标记单条消息已读
    // =========================================================================

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void markRead(Long userId, Long msgId) {
        ChatMessage msg = chatMessageMapper.selectById(msgId);
        if (msg == null || msg.getStatus() == 3) {
            throw new BizException(ResultCode.MSG_NOT_FOUND);
        }
        // 鉴权：用户必须在该会话中
        if (!messageCacheService.isMember(msg.getSessionId(), userId)) {
            throw new BizException(ResultCode.ILLEGAL_SESSION);
        }

        // 更新 session_member.last_read_msg_id（只前进，不后退）
        sessionMemberMapper.updateLastReadMsgId(msg.getSessionId(), userId, msgId);

        // 单聊场景：更新 chat_message.is_read=1（只有接收方才更新）
        if (!userId.equals(msg.getSenderId())) {
            ChatMessage update = new ChatMessage();
            update.setId(msgId);
            update.setIsRead(1);
            chatMessageMapper.updateById(update);
        }
    }

    // =========================================================================
    // markAllRead：标记会话全部已读
    // =========================================================================

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void markAllRead(Long userId, Long sessionId) {
        // 鉴权：用户必须在该会话中
        SessionMember member = sessionMemberMapper.selectMember(sessionId, userId);
        if (member == null) {
            throw new BizException(ResultCode.ILLEGAL_SESSION);
        }

        // 取会话最新消息 ID
        Session session = sessionCacheService.getSessionById(sessionId);
        if (session == null) {
            throw new BizException(ResultCode.SESSION_NOT_FOUND);
        }
        Long lastMsgId = session.getLastMsgId();
        if (lastMsgId == null) return; // 会话还没有消息，无需操作

        // 更新 session_member.last_read_msg_id = session.last_msg_id（只前进）
        sessionMemberMapper.updateLastReadMsgId(sessionId, userId, lastMsgId);
    }

    // =========================================================================
    // 内部工具方法
    // =========================================================================

    /** ChatMessageMQDTO → MessageVO（缓存命中时使用） */
    private MessageVO toVO(ChatMessageMQDTO dto) {
        MessageVO vo = new MessageVO();
        vo.setMsgId(dto.getMsgId());
        vo.setMsgNo(dto.getMsgNo());
        vo.setSessionId(dto.getSessionId());
        vo.setSenderId(dto.getSenderId());
        vo.setMsgType(dto.getMsgType());
        vo.setContent(dto.getContent());
        vo.setExtra(dto.getExtra());
        vo.setReplyMsgId(dto.getReplyMsgId());
        vo.setStatus(1); // ZSet 中的消息均为正常状态
        if (dto.getTimestamp() != null) {
            vo.setCreatedAt(java.time.LocalDateTime.ofInstant(
                    java.time.Instant.ofEpochMilli(dto.getTimestamp()),
                    java.time.ZoneId.systemDefault()));
        }
        return vo;
    }

    /** ChatMessage → MessageVO（回源 DB 时使用） */
    private MessageVO toVO(ChatMessage msg) {
        MessageVO vo = new MessageVO();
        vo.setMsgId(msg.getId());
        vo.setMsgNo(msg.getMsgNo());
        vo.setSessionId(msg.getSessionId());
        vo.setSenderId(msg.getSenderId());
        vo.setMsgType(msg.getMsgType());
        vo.setContent(msg.getContent());
        vo.setExtra(msg.getExtra());
        vo.setReplyMsgId(msg.getReplyMsgId());
        vo.setStatus(msg.getStatus());
        vo.setCreatedAt(msg.getCreatedAt());
        return vo;
    }

    private ChatMessageMQDTO buildMQDTO(long msgId, Long senderId, SendMessageDTO dto, Session session) {
        ChatMessageMQDTO mqDTO = new ChatMessageMQDTO();
        mqDTO.setMsgId(msgId);
        mqDTO.setMsgNo(dto.getMsgNo());
        mqDTO.setSessionId(dto.getSessionId());
        mqDTO.setSessionNo(session.getSessionNo());
        mqDTO.setSenderId(senderId);
        mqDTO.setMsgType(dto.getMsgType());
        mqDTO.setContent(dto.getContent());
        mqDTO.setExtra(dto.getExtra());
        mqDTO.setReplyMsgId(dto.getReplyMsgId());
        mqDTO.setTimestamp(System.currentTimeMillis());
        mqDTO.setMemberCount(session.getMemberCount());
        return mqDTO;
    }

    private LocalMsgOutbox buildOutbox(long msgId, Long senderId, SendMessageDTO dto, ChatMessageMQDTO mqDTO) {
        String payload;
        try {
            payload = objectMapper.writeValueAsString(mqDTO);
        } catch (Exception e) {
            throw new BizException(ResultCode.SERVER_ERROR);
        }
        LocalMsgOutbox outbox = new LocalMsgOutbox();
        outbox.setMsgNo(dto.getMsgNo());
        outbox.setSessionId(dto.getSessionId());
        outbox.setSenderId(senderId);
        outbox.setPayload(payload);
        outbox.setStatus(0);
        outbox.setRetryCount(0);
        return outbox;
    }

    private SendMessageVO buildDuplicateVO(String msgNo, Long msgId) {
        SendMessageVO vo = new SendMessageVO();
        vo.setMsgId(msgId);
        vo.setMsgNo(msgNo);
        vo.setStatus("sending");
        return vo;
    }
}
