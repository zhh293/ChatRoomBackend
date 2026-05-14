package com.example.chatroom.module.call.service.impl;

import com.example.chatroom.cache.bloom.BloomFilterInitializer;
import com.example.chatroom.cache.message.MessageCacheService;
import com.example.chatroom.cache.session.SessionCacheService;
import com.example.chatroom.common.constant.RedisKeyConst;
import com.example.chatroom.common.delay.DelayQueueService;
import com.example.chatroom.common.exception.BizException;
import com.example.chatroom.common.response.ResultCode;
import com.example.chatroom.common.util.SnowflakeIdGenerator;
import com.example.chatroom.module.call.domain.dto.CallInitiateDTO;
import com.example.chatroom.module.call.domain.vo.CallInitiateVO;
import com.example.chatroom.module.call.service.CallService;
import com.example.chatroom.module.message.domain.entity.LocalMsgOutbox;
import com.example.chatroom.module.message.mapper.LocalMsgOutboxMapper;
import com.example.chatroom.module.session.domain.entity.Session;
import com.example.chatroom.mq.dto.ChatMessageMQDTO;
import com.example.chatroom.mq.producer.ChatMessageProducer;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.TimeUnit;

/**
 * 语音通话服务实现
 *
 * <h3>发起通话主链路（与文字消息完全一致）</h3>
 * <pre>
 * ① 布隆过滤器校验 sessionId
 * ② 缓存鉴权：SISMEMBER 判断用户是否在会话中
 * ③ 额外校验：会话必须是单聊（type=1）
 * ④ 确定被叫用户
 * ⑤ 忙线检测（Redis）
 * ⑥ 幂等锁（Redisson）：同一 msgNo 只处理一次
 * ⑦ 生成 msgId（雪花算法）
 * ⑧ 写消息到 ZSet 缓存（msg:buf:{sessionId}）
 * ⑨ 写本地事务表 local_msg_outbox（status=0，待发送）
 * ⑩ 发送到 RabbitMQ（Publisher Confirm）
 * ⑪ 返回 {callId, msgId, msgNo, status="sending"}
 *
 * MQ 消费端：入 chat_message + 入 call_record（状态机 INVITING）
 * 失败回滚：写缓存后若 ⑨⑩ 失败，删除 ZSet 中该消息
 * </pre>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CallServiceImpl implements CallService {

    private final LocalMsgOutboxMapper outboxMapper;
    private final SessionCacheService sessionCacheService;
    private final MessageCacheService messageCacheService;
    private final ChatMessageProducer mqProducer;
    private final DelayQueueService delayQueueService;
    private final SnowflakeIdGenerator idGenerator;
    private final RedissonClient redissonClient;
    private final StringRedisTemplate stringRedisTemplate;
    private final ObjectMapper objectMapper;

    /** 语音通话消息类型 */
    private static final int MSG_TYPE_VOICE_CALL = 7;

    private static final String CALL_IDEM_LOCK_PREFIX = "lock:call:idem:";
    private static final String CALL_BUSY_PREFIX = "call:busy:";

    @Override
    @Transactional(rollbackFor = Exception.class)
    public CallInitiateVO initiateCall(Long callerId, CallInitiateDTO dto) {

        // ① 布隆过滤器：sessionId 是否存在
        if (BloomFilterInitializer.sessionBloomReady.get()) {
            org.redisson.api.RBloomFilter<Long> bloom =
                    redissonClient.getBloomFilter(RedisKeyConst.BLOOM_SESSION_IDS);
            if (!bloom.contains(dto.getSessionId())) {
                throw new BizException(ResultCode.SESSION_NOT_FOUND);
            }
        }

        // ② 缓存鉴权：用户是否在会话中
        if (!messageCacheService.isMember(dto.getSessionId(), callerId)) {
            throw new BizException(ResultCode.ILLEGAL_SESSION);
        }

        // ③ 会话必须是单聊
        Session session = sessionCacheService.getSessionById(dto.getSessionId());
        if (session == null) {
            throw new BizException(ResultCode.SESSION_NOT_FOUND);
        }
        if (session.getType() != 1) {
            throw new BizException(ResultCode.CALL_SESSION_NOT_SINGLE);
        }

        // ④ 校验被叫用户（前端传入 calleeId，后端只验证其是否在会话中）
        Long calleeId = dto.getCalleeId();
        if (calleeId.equals(callerId)) {
            throw new BizException(ResultCode.CALL_SESSION_NOT_SINGLE);
        }
        if (!messageCacheService.isMember(dto.getSessionId(), calleeId)) {
            throw new BizException(ResultCode.ILLEGAL_SESSION);
        }

        // ⑤ 忙线检测：双方都不能在通话中
        String callerBusyKey = CALL_BUSY_PREFIX + callerId;
        String calleeBusyKey = CALL_BUSY_PREFIX + calleeId;
        if (Boolean.TRUE.equals(stringRedisTemplate.hasKey(callerBusyKey))) {
            throw new BizException(ResultCode.CALL_CALLER_BUSY);
        }
        if (Boolean.TRUE.equals(stringRedisTemplate.hasKey(calleeBusyKey))) {
            throw new BizException(ResultCode.CALL_CALLEE_BUSY);
        }

        // ⑥ 幂等锁：同一 msgNo 只处理一次
        String lockKey = CALL_IDEM_LOCK_PREFIX + dto.getMsgNo();
        RLock lock = redissonClient.getLock(lockKey);
        try {
            if (!lock.tryLock(1, 10, TimeUnit.SECONDS)) {
                Long existingMsgId = messageCacheService.getMsgId(dto.getMsgNo());
                if (existingMsgId != null) {
                    return buildDuplicateVO(dto.getMsgNo(), existingMsgId);
                }
                throw new BizException(ResultCode.MSG_DUPLICATE);
            }

            try {
                // Double Check
                Long existingMsgId = messageCacheService.getMsgId(dto.getMsgNo());
                if (existingMsgId != null) {
                    return buildDuplicateVO(dto.getMsgNo(), existingMsgId);
                }

                return doInitiateCall(callerId, calleeId, dto, session);

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

    // =========================================================================
    // 核心发起逻辑（已在幂等锁内）
    // =========================================================================

    private CallInitiateVO doInitiateCall(Long callerId, Long calleeId,
                                          CallInitiateDTO dto, Session session) {
        // ⑦ 生成 msgId 和 callId
        long msgId = idGenerator.nextId();
        long callId = idGenerator.nextId();

        // 构造 MQ DTO（msgType=7，extra 里带 callId 和 calleeId）
        ChatMessageMQDTO mqDTO = buildMQDTO(msgId, callId, callerId, calleeId, dto, session);

        // ⑧ 写消息到 ZSet 缓存
        messageCacheService.putMessage(mqDTO);

        // 暂存 msgNo → msgId（幂等 Double Check 用，TTL 2min）
        messageCacheService.setMsgIdIfAbsent(dto.getMsgNo(), msgId);

        try {
            // ⑨ 写本地事务表
            LocalMsgOutbox outbox = buildOutbox(msgId, callerId, dto, mqDTO);
            outboxMapper.insert(outbox);

            // ⑩ 发送到 RabbitMQ
            mqProducer.sendWithConfirm(outbox.getPayload(), dto.getMsgNo());

            // ⑩.5 投递延迟任务：30s 后检查 outbox 是否已落库
            delayQueueService.addTask(dto.getMsgNo(), 30);

        } catch (Exception e) {
            // 失败回滚 ZSet 缓存
            log.error("[Call] 写Outbox或发MQ失败，回滚缓存, msgId={}, msgNo={}",
                    msgId, dto.getMsgNo(), e);
            messageCacheService.removeMessage(dto.getSessionId(), msgId);
            throw new BizException(ResultCode.SERVER_ERROR);
        }

        // ⑪ 返回
        CallInitiateVO vo = new CallInitiateVO();
        vo.setCallId(callId);
        vo.setMsgId(msgId);
        vo.setMsgNo(dto.getMsgNo());
        vo.setStatus("sending");
        return vo;
    }

    // =========================================================================
    // 工具方法
    // =========================================================================

    private ChatMessageMQDTO buildMQDTO(long msgId, long callId, Long callerId, Long calleeId,
                                         CallInitiateDTO dto, Session session) {
        ChatMessageMQDTO mqDTO = new ChatMessageMQDTO();
        mqDTO.setMsgId(msgId);
        mqDTO.setMsgNo(dto.getMsgNo());
        mqDTO.setSessionId(dto.getSessionId());
        mqDTO.setSessionNo(session.getSessionNo());
        mqDTO.setSenderId(callerId);
        mqDTO.setMsgType(MSG_TYPE_VOICE_CALL);
        mqDTO.setContent("[语音通话]");
        // extra 里带 callId 和 calleeId，MQ 消费端用来写 call_record
        mqDTO.setExtra(buildExtra(callId, calleeId));
        mqDTO.setTimestamp(System.currentTimeMillis());
        mqDTO.setMemberCount(session.getMemberCount());
        return mqDTO;
    }

    private String buildExtra(long callId, Long calleeId) {
        try {
            var node = objectMapper.createObjectNode();
            node.put("callId", callId);
            node.put("calleeId", calleeId);
            return objectMapper.writeValueAsString(node);
        } catch (Exception e) {
            return "{}";
        }
    }

    private LocalMsgOutbox buildOutbox(long msgId, Long senderId,
                                        CallInitiateDTO dto, ChatMessageMQDTO mqDTO) {
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

    private CallInitiateVO buildDuplicateVO(String msgNo, Long msgId) {
        CallInitiateVO vo = new CallInitiateVO();
        vo.setMsgId(msgId);
        vo.setMsgNo(msgNo);
        vo.setStatus("sending");
        return vo;
    }
}
