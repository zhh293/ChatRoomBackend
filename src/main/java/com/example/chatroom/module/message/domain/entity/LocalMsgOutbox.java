package com.example.chatroom.module.message.domain.entity;

import com.baomidou.mybatisplus.annotation.*;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 本地消息发件箱（Outbox Pattern，保证消息可靠投递）
 * status: 0待发送 1已发送MQ 2已消费落库 3发送失败
 */
@Data
@TableName("local_msg_outbox")
public class LocalMsgOutbox {

    @TableId(type = IdType.AUTO)
    private Long id;

    private String msgNo;
    private Long sessionId;
    private Long senderId;
    /** MQ 消息体 JSON */
    private String payload;
    /** 0待发送 1已发送MQ 2已消费落库 3发送失败 */
    private Integer status;
    private Integer retryCount;
    private LocalDateTime nextRetryAt;

    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createdAt;

    @TableField(fill = FieldFill.INSERT_UPDATE)
    private LocalDateTime updatedAt;
}
