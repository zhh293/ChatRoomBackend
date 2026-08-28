package com.example.chatroom.mq.dto;

import lombok.Data;

/**
 * RabbitMQ 消息体 DTO
 */
@Data
public class ChatMessageMQDTO {

    private Long msgId;
    private String msgNo;
    private Long sessionId;
    private String sessionNo;
    private Long senderId;
    private Integer msgType;
    private String content;
    private String extra;
    private Long replyMsgId;
    private Long timestamp;
    /** 会话当前成员数，用于判断大群/小群分片路由（消费端写 ZSet 时使用） */
    private Integer memberCount;

    /** 消息状态：1-正常 2-撤回（缓存中使用，撤回时 Lua 脚本会原子更新此字段） */
    private Integer status;
}
