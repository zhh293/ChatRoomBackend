package com.example.chatroom.module.session.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.example.chatroom.module.session.domain.entity.Session;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;

/**
 * 会话 Mapper
 */
@Mapper
public interface SessionMapper extends BaseMapper<Session> {

    @Select("SELECT MAX(id) FROM session WHERE deleted_at IS NULL")
    Long selectMaxId();

    @Select("SELECT id FROM session WHERE id > #{lastId} AND deleted_at IS NULL ORDER BY id ASC LIMIT #{batchSize}")
    List<Long> selectIdsBatch(@Param("lastId") long lastId, @Param("batchSize") int batchSize);

    /**
     * 分页查询用户的会话列表（子查询避免深度分页）
     * <p>
     * 先在 session_member 子查询中按 session_id DESC 取当页 ID，
     * 再 JOIN session 表拿完整信息，最终按 last_msg_at DESC 排序。
     * </p>
     *
     * @param userId 当前用户 ID
     * @param offset LIMIT offset（= (page-1)*size）
     * @param size   每页条数
     */
    @Select("""
            SELECT s.*
            FROM session s
            JOIN (
                SELECT session_id
                FROM session_member
                WHERE user_id = #{userId}
                  AND left_at IS NULL
                ORDER BY session_id DESC
                LIMIT #{offset}, #{size}
            ) sm ON s.id = sm.session_id
            WHERE s.deleted_at IS NULL
              AND s.status = 1
            ORDER BY s.last_msg_at DESC
            """)
    List<Session> selectPageByUserId(@Param("userId") Long userId,
                                     @Param("offset") int offset,
                                     @Param("size") int size);

    /**
     * 统计用户参与的有效会话总数（用于判断是否还有下一页）
     */
    @Select("""
            SELECT COUNT(*)
            FROM session_member sm
            JOIN session s ON s.id = sm.session_id
            WHERE sm.user_id = #{userId}
              AND sm.left_at IS NULL
              AND s.deleted_at IS NULL
              AND s.status = 1
            """)
    long countByUserId(@Param("userId") Long userId);

    /**
     * 根据 sessionNo 查询会话（含逻辑删除过滤）
     */
    @Select("SELECT * FROM session WHERE session_no = #{sessionNo} AND deleted_at IS NULL LIMIT 1")
    Session selectBySessionNo(@Param("sessionNo") String sessionNo);

    /**
     * 查询两个用户之间是否已存在单聊会话
     * 通过 session_no 约定（min_max 格式）直接命中
     */
    @Select("SELECT * FROM session WHERE session_no = #{sessionNo} AND type = 1 AND deleted_at IS NULL LIMIT 1")
    Session selectSingleSession(@Param("sessionNo") String sessionNo);

    /**
     * 更新会话最后一条消息信息（消费者落库后调用）
     * 只有新 msgId > 旧 last_msg_id 时才更新，防止乱序覆盖
     */
    @Update("""
            UPDATE session
            SET last_msg_id      = #{lastMsgId},
                last_msg_content = #{lastMsgContent},
                last_msg_at      = #{lastMsgAt},
                updated_at       = NOW()
            WHERE id = #{sessionId}
              AND (last_msg_id IS NULL OR last_msg_id < #{lastMsgId})
            """)
    int updateLastMsg(@Param("sessionId") Long sessionId,
                      @Param("lastMsgId") Long lastMsgId,
                      @Param("lastMsgContent") String lastMsgContent,
                      @Param("lastMsgAt") java.time.LocalDateTime lastMsgAt);

    /**
     * 撤回消息时更新会话最后一条消息摘要（仅当被撤回的消息恰好是 last_msg_id 时才更新）
     * 不改变 last_msg_id 和 last_msg_at，只改摘要内容
     */
    @Update("""
            UPDATE session
            SET last_msg_content = #{lastMsgContent},
                updated_at       = NOW()
            WHERE id = #{sessionId}
              AND last_msg_id = #{msgId}
            """)
    int updateLastMsgContentIfMatch(@Param("sessionId") Long sessionId,
                                    @Param("msgId") Long msgId,
                                    @Param("lastMsgContent") String lastMsgContent);

    /**
     * 带人数上限的 member_count 更新（乐观兜底）
     *
     * <p>只有在 member_count + delta <= maxCount 时才执行更新，
     * 返回影响行数：1 = 成功，0 = 已达上限（并发超限被拦截）。
     *
     * <p>配合分布式锁使用：分布式锁是主防线，此处是最后一道兜底，
     * 防止 Redis 故障导致锁失效时人数仍然超限。
     */
    @Update("""
            UPDATE session
            SET member_count = member_count + #{delta},
                updated_at   = NOW()
            WHERE id = #{sessionId}
              AND member_count + #{delta} <= #{maxCount}
            """)
    int updateMemberCountWithLimit(@Param("sessionId") Long sessionId,
                                   @Param("delta") int delta,
                                   @Param("maxCount") int maxCount);
}
