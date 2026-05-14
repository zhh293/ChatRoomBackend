package com.example.chatroom.module.message.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.example.chatroom.module.message.domain.entity.LocalMsgOutbox;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 本地消息发件箱 Mapper
 */
@Mapper
public interface LocalMsgOutboxMapper extends BaseMapper<LocalMsgOutbox> {

    /**
     * 游标翻页查询待补偿消息（分片广播）
     *
     * <p>条件说明：
     * <ul>
     *   <li>status = 0（待发送）：正常待投递</li>
     *   <li>status = 1（已发送MQ）且超过 30s 未落库：MQ 投递后消费端未回调，视为超时重投</li>
     * </ul>
     * created_at < #{deadline} 保证只捞"飞行超时"的消息，避免把刚写入的正常消息误捞
     * id > #{lastId} 游标翻页，防止深度分页
     * MOD(id, shardTotal) = shardIndex 分片广播，各节点不重叠
     * </p>
     */
    @Select("""
            SELECT * FROM local_msg_outbox
            WHERE status IN (0, 1)
              AND retry_count < #{maxRetry}
              AND created_at < #{deadline}
              AND (next_retry_at IS NULL OR next_retry_at <= NOW())
              AND id > #{lastId}
              AND MOD(id, #{shardTotal}) = #{shardIndex}
            ORDER BY id ASC
            LIMIT #{batchSize}
            """)
    List<LocalMsgOutbox> selectPendingCursor(@Param("lastId") long lastId,
                                             @Param("deadline") LocalDateTime deadline,
                                             @Param("maxRetry") int maxRetry,
                                             @Param("shardIndex") int shardIndex,
                                             @Param("shardTotal") int shardTotal,
                                             @Param("batchSize") int batchSize);

    /**
     * CAS 抢占：将 status=0 → status=1（已发送MQ），防止多节点/任务重跑重复投递
     * 只有 status 仍为 0 时才更新成功（返回 1），否则说明已被其他线程抢走（返回 0）
     */
    @Update("UPDATE local_msg_outbox SET status = 1, updated_at = NOW() WHERE id = #{id} AND status = 0")
    int casClaimForRetry(@Param("id") Long id);

    /**
     * 批量删除已归档的消息（7天前已落库的）
     */
    @Update("DELETE FROM local_msg_outbox WHERE status = 2 AND created_at < #{threshold} LIMIT #{batchSize}")
    int deleteArchivedBatch(@Param("threshold") LocalDateTime threshold,
                            @Param("batchSize") int batchSize);

    /** 根据 msgNo 查询单条记录（延迟队列消费时使用） */
    @Select("SELECT * FROM local_msg_outbox WHERE msg_no = #{msgNo} LIMIT 1")
    LocalMsgOutbox selectByMsgNo(@Param("msgNo") String msgNo);

    /** 根据 msgNo 更新状态（Publisher Confirm 回调用） */
    @Update("UPDATE local_msg_outbox SET status = #{status} WHERE msg_no = #{msgNo}")
    int updateStatusByMsgNo(@Param("msgNo") String msgNo, @Param("status") int status);

    /** 更新重试信息（指数退避） */
    @Update("UPDATE local_msg_outbox SET retry_count = #{retryCount}, next_retry_at = #{nextRetryAt} WHERE id = #{id}")
    int updateRetry(@Param("id") Long id, @Param("retryCount") int retryCount,
                    @Param("nextRetryAt") LocalDateTime nextRetryAt);

    /** 根据 ID 更新状态 */
    @Update("UPDATE local_msg_outbox SET status = #{status} WHERE id = #{id}")
    int updateStatusById(@Param("id") Long id, @Param("status") int status);
}
