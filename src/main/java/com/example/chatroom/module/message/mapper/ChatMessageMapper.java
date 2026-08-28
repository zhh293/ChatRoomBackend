package com.example.chatroom.module.message.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.example.chatroom.module.message.domain.dto.UnreadCountParam;
import com.example.chatroom.module.message.domain.entity.ChatMessage;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;
import java.util.Map;

/**
 * 聊天消息 Mapper（ShardingSphere 自动路由到对应分片表）
 */
@Mapper
public interface ChatMessageMapper extends BaseMapper<ChatMessage> {

    /**
     * 游标向上翻页（加载更早的消息）
     */
    @Select("SELECT * FROM chat_message WHERE session_id = #{sessionId} AND id < #{cursor} AND status IN (1, 2) ORDER BY id DESC LIMIT #{size}")
    List<ChatMessage> selectBefore(@Param("sessionId") Long sessionId,
                                   @Param("cursor") Long cursor,
                                   @Param("size") int size);

    /**
     * 首次加载（取最新 N 条）
     */
    @Select("SELECT * FROM chat_message WHERE session_id = #{sessionId} AND status IN (1, 2) ORDER BY id DESC LIMIT #{size}")
    List<ChatMessage> selectLatest(@Param("sessionId") Long sessionId,
                                   @Param("size") int size);

    /**
     * 统计 lastReadMsgId 之后的未读消息数（读扩散模式 ZSet 缺失时回源 DB 用）
     */
    @Select("SELECT COUNT(*) FROM chat_message WHERE session_id = #{sessionId} AND id > #{lastReadMsgId} AND status = 1")
    int countUnread(@Param("sessionId") Long sessionId,
                    @Param("lastReadMsgId") Long lastReadMsgId);

    /**
     * 游标向下翻页（加载更新的消息，after 方向）
     * 用于：进入会话时以 lastReadMsgId 为游标拉取未读消息
     */
    @Select("SELECT * FROM chat_message WHERE session_id = #{sessionId} AND id > #{cursor} AND status IN (1, 2) ORDER BY id ASC LIMIT #{size}")
    List<ChatMessage> selectAfter(@Param("sessionId") Long sessionId,
                                  @Param("cursor") Long cursor,
                                  @Param("size") int size);

    /**
     * 批量统计多个会话的未读消息数（一次 DB 往返）
     *
     * <p>使用 UNION ALL 将多个 COUNT 查询合并为一条 SQL。
     * ShardingSphere 会根据每个子查询的 session_id 路由到对应分片表并行执行。
     *
     * <p>返回结果为 List<Map>，每个 Map 包含：
     * <ul>
     *   <li>session_id (Long) - 会话 ID</li>
     *   <li>unread_count (Long) - 未读消息数</li>
     * </ul>
     *
     * @param params 批量查询参数列表，每个元素包含 sessionId 和 lastReadMsgId
     * @return 每个会话的未读数
     */
    @Select("""
            <script>
            <foreach collection="params" item="p" separator=" UNION ALL ">
            SELECT #{p.sessionId} AS session_id,
                   COUNT(*) AS unread_count
            FROM chat_message
            WHERE session_id = #{p.sessionId}
              AND id &gt; #{p.lastReadMsgId}
              AND status = 1
            </foreach>
            </script>
            """)
    List<Map<String, Object>> batchCountUnread(@Param("params") List<UnreadCountParam> params);

    /**
     * 根据 msgNo 判断消息是否已入库（走唯一索引 uk_msg_no，带 sessionId 用于分片路由）
     */
    @Select("SELECT id FROM chat_message WHERE session_id = #{sessionId} AND msg_no = #{msgNo} LIMIT 1")
    Long existsByMsgNo(@Param("sessionId") Long sessionId, @Param("msgNo") String msgNo);
}
