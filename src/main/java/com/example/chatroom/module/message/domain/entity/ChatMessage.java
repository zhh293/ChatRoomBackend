package com.example.chatroom.module.message.domain.entity;

import com.baomidou.mybatisplus.annotation.*;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 聊天消息实体（水平分片表，ShardingSphere 路由到 chat_message_0~15）
 */
@Data
@TableName("chat_message")
public class ChatMessage {

    /** 消息ID（雪花算法，全局唯一，非自增） */
    @TableId(type = IdType.INPUT)
    private Long id;

    /** 消息业务编号（客户端生成，用于幂等） */
    private String msgNo;

    /** 会话ID（分片键） */
    private Long sessionId;

    private Long senderId;

    /** 1文本 2图片 3语音 4视频 5文件 6系统通知 */
    private Integer msgType;

    private String content;

    /** 扩展字段（JSON，如图片宽高、文件大小等） */
    private String extra;

    private Long replyMsgId;

    /** 1正常 2撤回 3删除 */
    private Integer status;

    /** 单聊是否已读 */
    private Integer isRead;

    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createdAt;

    @TableField(fill = FieldFill.INSERT_UPDATE)
    private LocalDateTime updatedAt;
}
