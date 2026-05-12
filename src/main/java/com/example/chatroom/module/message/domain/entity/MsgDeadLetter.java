package com.example.chatroom.module.message.domain.entity;

import com.baomidou.mybatisplus.annotation.*;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 消息死信表（重试彻底失败后落库，保留完整信息供人工介入）
 *
 * <p>字段与 chat_message 对齐，额外增加失败原因和重试次数，
 * 人工排查修复后可直接 INSERT INTO chat_message SELECT ... FROM msg_dead_letter。
 */
@Data
@TableName("msg_dead_letter")
public class MsgDeadLetter {

    @TableId(type = IdType.AUTO)
    private Long id;

    /** 消息ID（雪花算法生成） */
    private Long msgId;

    /** 消息业务编号（前端生成，幂等键） */
    private String msgNo;

    /** 会话ID */
    private Long sessionId;

    /** 会话编号 */
    private String sessionNo;

    /** 发送者ID */
    private Long senderId;

    /** 消息类型：1文本 2图片 3语音 4视频 5文件 6系统 7语音通话 */
    private Integer msgType;

    /** 消息内容 */
    private String content;

    /** 扩展字段JSON */
    private String extra;

    /** 回复消息ID */
    private Long replyMsgId;

    /** 消息时间戳（发送时的毫秒时间戳） */
    private Long msgTimestamp;

    /** 会话成员数 */
    private Integer memberCount;

    /** 最后一次失败原因 */
    private String failReason;

    /** 重试次数 */
    private Integer retryCount;

    /** 处理状态：0待处理 1已人工修复 2已放弃 */
    private Integer status;

    /** MQ 原始 payload（完整 JSON，兜底） */
    @TableField("raw_payload")
    private String rawPayload;

    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createdAt;

    @TableField(fill = FieldFill.INSERT_UPDATE)
    private LocalDateTime updatedAt;
}
