package com.example.chatroom.module.session.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.example.chatroom.module.session.domain.dto.LastReadPositionDTO;
import com.example.chatroom.module.session.domain.entity.SessionMember;
import org.apache.ibatis.annotations.*;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 会话成员 Mapper
 */
@Mapper
public interface SessionMemberMapper extends BaseMapper<SessionMember> {

    /**
     * 查询超过指定时间未登录的用户ID（离线消息清理任务用）
     */
    @Select("""
            SELECT DISTINCT sm.user_id
            FROM session_member sm
            JOIN user u ON u.id = sm.user_id
            WHERE u.last_login_at < #{threshold}
              AND sm.left_at IS NULL
            LIMIT #{limit}
            """)
    List<Long> queryInactiveUserIds(@Param("threshold") LocalDateTime threshold,
                                    @Param("limit") int limit);

    /**
     * 查询会话的所有在线成员 ID（用于写扩散推送）
     */
    @Select("""
            SELECT user_id
            FROM session_member
            WHERE session_id = #{sessionId}
              AND left_at IS NULL
            """)
    List<Long> selectMemberIds(@Param("sessionId") Long sessionId);

    /**
     * 查询某用户在某会话中的成员记录
     */
    @Select("""
            SELECT *
            FROM session_member
            WHERE session_id = #{sessionId}
              AND user_id = #{userId}
              AND left_at IS NULL
            LIMIT 1
            """)
    SessionMember selectMember(@Param("sessionId") Long sessionId,
                               @Param("userId") Long userId);

    /**
     * 软删除（退出/踢出）：设置 left_at
     */
    @Update("""
            UPDATE session_member
            SET left_at = #{leftAt}, updated_at = #{leftAt}
            WHERE session_id = #{sessionId}
              AND user_id = #{userId}
              AND left_at IS NULL
            """)
    int softDelete(@Param("sessionId") Long sessionId,
                   @Param("userId") Long userId,
                   @Param("leftAt") LocalDateTime leftAt);

    /**
     * 批量软删除（解散群聊时踢出所有成员）
     */
    @Update("""
            UPDATE session_member
            SET left_at = #{leftAt}, updated_at = #{leftAt}
            WHERE session_id = #{sessionId}
              AND left_at IS NULL
            """)
    int softDeleteAll(@Param("sessionId") Long sessionId,
                      @Param("leftAt") LocalDateTime leftAt);

    /**
     * 统计会话当前在线成员数
     */
    @Select("""
            SELECT COUNT(*)
            FROM session_member
            WHERE session_id = #{sessionId}
              AND left_at IS NULL
            """)
    int countMembers(@Param("sessionId") Long sessionId);

    /**
     * 查询会话所有在群成员的 userId 列表（用于重建 Redis Set 缓存）
     * 只返回 userId，不关联 user 表，查询极快
     */
    @Select("""
            SELECT user_id
            FROM session_member
            WHERE session_id = #{sessionId}
              AND left_at IS NULL
            """)
    List<Long> selectMemberUserIds(@Param("sessionId") Long sessionId);

    /**
     * 查询会话所有在群成员的 userId + role（用于组装 SessionMemberVO）
     * 返回 Map 形式：key=userId，value=role
     */
    @Select("""
            SELECT user_id, role
            FROM session_member
            WHERE session_id = #{sessionId}
              AND left_at IS NULL
            """)
    @MapKey("userId")
    java.util.Map<Long, SessionMember> selectMemberRoles(@Param("sessionId") Long sessionId);

    /**
     * 分页查询用户各会话的已读位置
     * <p>
     * 返回字段：session.session_no、session_member.last_read_msg_id
     * 按 session_member.session_id DESC 排序（最近加入/活跃的会话优先）
     * 子查询避免深度分页
     * </p>
     *
     * @param userId 当前用户 ID
     * @param offset LIMIT offset（= (page-1)*size）
     * @param size   每页条数
     */
    @Select("""
            SELECT s.session_no, sm.last_read_msg_id
            FROM session_member sm
            JOIN session s ON s.id = sm.session_id
            WHERE sm.user_id = #{userId}
              AND sm.left_at IS NULL
              AND s.deleted_at IS NULL
              AND s.status = 1
            ORDER BY sm.session_id DESC
            LIMIT #{offset}, #{size}
            """)
    List<LastReadPositionDTO> selectLastReadPositions(@Param("userId") Long userId,
                                                      @Param("offset") int offset,
                                                      @Param("size") int size);

    /**
     * 统计用户参与的有效会话总数（分页用）
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
    long countValidSessions(@Param("userId") Long userId);

    /**
     * 更新用户在某会话的已读位置
     * 只有新值 > 旧值时才更新，防止旧请求覆盖新进度
     */
    @Update("""
            UPDATE session_member
            SET last_read_msg_id = #{lastReadMsgId},
                updated_at       = NOW()
            WHERE session_id     = #{sessionId}
              AND user_id        = #{userId}
              AND left_at        IS NULL
              AND (last_read_msg_id IS NULL OR last_read_msg_id < #{lastReadMsgId})
            """)
    int updateLastReadMsgId(@Param("sessionId") Long sessionId,
                            @Param("userId") Long userId,
                            @Param("lastReadMsgId") Long lastReadMsgId);
}
