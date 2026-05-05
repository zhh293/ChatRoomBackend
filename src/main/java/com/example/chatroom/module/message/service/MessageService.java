package com.example.chatroom.module.message.service;

import com.example.chatroom.common.response.PageResult;
import com.example.chatroom.module.message.domain.dto.SendMessageDTO;
import com.example.chatroom.module.message.domain.vo.MessageVO;
import com.example.chatroom.module.message.domain.vo.SendMessageVO;

/**
 * 消息服务接口
 */
public interface MessageService {

    /**
     * 发送消息（核心链路）
     * 写本地消息表 → 发 RabbitMQ → 返回 sending 状态
     */
    SendMessageVO sendMessage(Long senderId, SendMessageDTO dto);

    /**
     * 历史消息游标分页查询
     * @param sessionId 会话ID
     * @param cursor    游标（消息ID），null 表示首次加载
     * @param size      每页数量
     * @param direction before（加载更早）/ after（加载更新）
     */
    PageResult<MessageVO> listMessages(Long userId, Long sessionId,
                                       Long cursor, int size, String direction);

    /** 撤回消息（2分钟内） */
    void revokeMessage(Long userId, Long msgId);

    /** 标记消息已读 */
    void markRead(Long userId, Long msgId);

    /** 标记会话全部已读 */
    void markAllRead(Long userId, Long sessionId);
}
