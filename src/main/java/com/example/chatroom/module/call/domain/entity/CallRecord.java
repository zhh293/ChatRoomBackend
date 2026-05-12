package com.example.chatroom.module.call.domain.entity;

import com.baomidou.mybatisplus.annotation.*;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 语音通话记录实体
 * <p>通过 messageId 与 chat_message 关联</p>
 */
@Data
@TableName("call_record")
public class CallRecord {

    @TableId(type = IdType.INPUT)
    private Long id;

    /** 通话唯一标识 */
    private Long callId;

    /** 所属会话ID */
    private Long sessionId;

    /** 关联聊天消息ID */
    private Long messageId;

    /** 主叫用户ID */
    private Long callerId;

    /** 被叫用户ID */
    private Long calleeId;

    /** 通话状态（CallStatus 枚举值） */
    private String status;

    /** 结束原因（CallEndReason 枚举值） */
    private String endReason;

    /** 接听时间 */
    private LocalDateTime answerTime;

    /** 通话结束时间 */
    private LocalDateTime endTime;

    /** 通话时长(秒) */
    private Integer durationSeconds;

    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createdAt;

    @TableField(fill = FieldFill.INSERT_UPDATE)
    private LocalDateTime updatedAt;
}
