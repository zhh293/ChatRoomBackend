package com.example.chatroom.module.message.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.example.chatroom.module.message.domain.entity.ChatMessage;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * 聊天消息 Mapper（ShardingSphere 自动路由到对应分片表）
 */
@Mapper
public interface ChatMessageMapper extends BaseMapper<ChatMessage> {

    /**
     * 游标向上翻页（加载更早的消息）
     */
    @Select("SELECT * FROM chat_message WHERE session_id = #{sessionId} AND id < #{cursor} AND status = 1 ORDER BY id DESC LIMIT #{size}")
    List<ChatMessage> selectBefore(@Param("sessionId") Long sessionId,
                                   @Param("cursor") Long cursor,
                                   @Param("size") int size);

    /**
     * 首次加载（取最新 N 条）
     */
    @Select("SELECT * FROM chat_message WHERE session_id = #{sessionId} AND status = 1 ORDER BY id DESC LIMIT #{size}")
    List<ChatMessage> selectLatest(@Param("sessionId") Long sessionId,
                                   @Param("size") int size);

    /**
     * 统计 lastReadMsgId 之后的未读消息数（读扩散模式 ZSet 缺失时回源 DB 用）
     */
    @Select("SELECT COUNT(*) FROM chat_message WHERE session_id = #{sessionId} AND id > #{lastReadMsgId} AND status = 1")
    int countUnread(@Param("sessionId") Long sessionId,
                    @Param("lastReadMsgId") Long lastReadMsgId);
}
