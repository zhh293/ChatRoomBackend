package com.example.chatroom.common.constant;

/**
 * Redis Key 常量
 * 统一管理所有 Redis Key 前缀，避免散落在各处
 */
public interface RedisKeyConst {

    // ===== 用户信息缓存 =====
    String USER_INFO = "user:info:";
    String USER_INFO_NULL = "user:info:null:";

    // ===== 会话缓存 =====
    String SESSION_INFO = "session:info:";
    String SESSION_INFO_NULL = "session:info:null:";
    String SESSION_MEMBERS = "session:members:";

    /**
     * 会话列表分页缓存
     * key   = user:session:list:{userId}:p{page}:s{size}
     * value = List<SessionVO> JSON
     * TTL   = sessionListTtl()（5~7min，随机防雪崩）
     */
    String USER_SESSION_LIST = "user:session:list:";

    /**
     * 会话列表分页缓存重建锁
     * key = cache:rebuild:lock:session:list:{userId}:{page}:{size}
     */
    String SESSION_LIST_REBUILD_LOCK = "cache:rebuild:lock:session:list:";

    /**
     * 消息缓冲区 ZSet（小群，< 1000人）
     * key   = msg:buf:{sessionId}
     * score = msgId
     * value = 消息内容 JSON
     * 上限  = 500 条（超出裁掉最老的）
     * TTL   = 7 天
     */
    String MSG_BUF = "msg:buf:";

    /**
     * 消息缓冲区 ZSet（大群，>= 1000人，按 userId 分片）
     * key   = msg:buf:shard:{sessionId}:{shard}
     * shard = userId % MSG_BUF_SHARD_COUNT
     * score = msgId
     * value = 消息内容 JSON
     * 上限  = 500 条（超出裁掉最老的）
     * TTL   = 7 天
     */
    String MSG_BUF_SHARD = "msg:buf:shard:";

    /**
     * 消息缓冲区 ZSet 上限（按群人数分三档）
     *
     * <pre>
     * 小群（memberCount <  MSG_BUF_MEDIUM_THRESHOLD）：500 条
     * 中群（memberCount <  MSG_BUF_LARGE_THRESHOLD） ：1000 条
     * 大群（memberCount >= MSG_BUF_LARGE_THRESHOLD） ：2000 条
     * </pre>
     */
    int MSG_BUF_MAX_SIZE_SMALL  = 500;
    int MSG_BUF_MAX_SIZE_MEDIUM = 1000;
    int MSG_BUF_MAX_SIZE_LARGE  = 2000;

    /** 中群分档阈值（含）：memberCount >= 100 进入中群 */
    int MSG_BUF_MEDIUM_THRESHOLD = 100;
    /** 大群分档阈值（含）：memberCount >= 1000 进入大群，与 write-fanout-threshold 保持一致 */
    int MSG_BUF_LARGE_THRESHOLD  = 1000;

    /** 消息缓冲区 TTL（秒，7天） */
    int MSG_BUF_TTL = 604800;

    /** 大群消息缓冲区分片数 */
    int MSG_BUF_SHARD_COUNT = 4;

    // ===== WebSocket 在线状态 =====
    String WS_ONLINE = "ws:online:";
    String WS_SYNC_CHANNEL = "ws:sync";
    String WS_PUSH_CHANNEL_PREFIX = "ws:push:";

    /** WebSocket 待 ACK 恢复影子（Hash），key 后缀为 machineId，field=userId:msgId。 */
    String WS_ACK_PENDING = "ws:ack:pending:";

    /** WebSocket 待 ACK 到期索引（ZSet），key 后缀为 machineId，score=expireAt。 */
    String WS_ACK_DEADLINE = "ws:ack:deadline:";

    /**
     * 历史消息翻阅缓存（ZSet，与热消息缓冲区结构一致）
     *
     * <pre>
     * key   = msg:history:{sessionId}
     * score = msgId（雪花ID，天然有序）
     * value = 消息内容 JSON（ChatMessage）
     * TTL   = MSG_HISTORY_TTL（10分钟，冷数据用完即过期）
     *
     * 适用场景：用户向上翻阅超出热消息 ZSet（msg:buf）范围的历史消息。
     * session 级公共缓存，所有用户共享同一个 ZSet，
     * 任意 cursor 范围均可用 ZRANGEBYSCORE 精确命中，
     * 避免多用户翻同一段历史时重复打 DB。
     *
     * 与 msg:buf 的区别：
     *   - TTL 短（10min vs 7d），历史冷数据不长期占内存
     *   - 无条数上限裁剪，翻多深缓多深，靠 TTL 控制生命周期
     *   - 不按 userId 分片，历史消息无写扩散压力
     * </pre>
     */
    String MSG_HISTORY = "msg:history:";

    /** 历史消息翻阅缓存 TTL（秒，10分钟） */
    int MSG_HISTORY_TTL = 600;

    // ===== 消息幂等 =====
    /** 客户端发送请求去重：msgNo -> msgId */
    String MSG_REQUEST_IDEM = "msg:request:idem:";

    /** MQ 消费完成标记：仅在业务处理全部成功后写入 */
    String MSG_CONSUME_DONE = "msg:consume:done:";

    /** MQ 消费完成标记 TTL（秒，7天）；过期后仍由数据库唯一索引兜底 */
    int MSG_CONSUME_DONE_TTL = 604800;

    // ===== Token 黑名单 =====
    String TOKEN_BLACKLIST = "token:blacklist:";

    /**
     * Refresh Token SHA-256 哈希 → userId 映射
     * key  = token:refresh:{sha256Hash}
     * value = userId (Long)
     * TTL  = Refresh Token 有效期（7d）
     */
    String TOKEN_REFRESH = "token:refresh:";

    /**
     * 用户当前登录态：userId → 当前有效的 refreshToken SHA-256 哈希
     * key  = user:login:current:{userId}
     * value = tokenHash (String)
     * TTL  = Refresh Token 有效期（7d，与 token:refresh 保持一致）
     *
     * 用途：
     *   登录前置校验 —— key 存在说明用户已登录，直接用现有 token 重签 accessToken 返回
     *   登出时删除   —— 保证下次登录能走完整流程
     */
    String USER_LOGIN_CURRENT = "user:login:current:";

    // ===== 接口限流 =====
    String RATE_LIMIT = "rate:limit:";

    // ===== 缓存重建锁 =====
    String CACHE_REBUILD_LOCK = "cache:rebuild:lock:";

    /**
     * 群成员列表缓存重建锁
     * key = cache:rebuild:lock:session:members:{sessionId}
     */
    String SESSION_MEMBERS_REBUILD_LOCK = "cache:rebuild:lock:session:members:";

    // ===== 布隆过滤器 =====
    String BLOOM_USER_IDS = "bloom:user:ids";
    String BLOOM_SESSION_IDS = "bloom:session:ids";

    // ===== 空值标记 =====
    String NULL_VALUE = "NULL_VALUE";

    // ===== 延迟队列（Outbox 补偿） =====
    /** 延迟队列 ZSet：score = 到期时间戳(ms)，value = msgNo */
    String DELAY_QUEUE = "delay:outbox:queue";
    /** 就绪队列 List：到期任务 LPUSH 到这里，消费者 BRPOP */
    String DELAY_READY = "delay:outbox:ready";
    /** 处理中队列 ZSet：score = 处理超时时间戳(ms)，value = msgNo */
    String DELAY_PROCESSING = "delay:outbox:processing";

    // ===== 分片上传 =====
    /**
     * 上传任务元信息（Hash）
     * key   = upload:task:{taskId}
     * field = fileMd5 / totalChunks / fileSize / contentType / userId / createdAt
     * TTL   = 24h（任务过期自动清理）
     */
    String UPLOAD_TASK = "upload:task:";

    /**
     * 分片到达状态（Bitmap）
     * key   = upload:bitmap:{taskId}
     * bit N = 1 表示第 N 个分片已成功落盘
     * TTL   = 与 upload:task 保持一致（24h）
     */
    String UPLOAD_BITMAP = "upload:bitmap:";
}
