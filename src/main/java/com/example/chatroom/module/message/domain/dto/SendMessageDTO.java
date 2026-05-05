package com.example.chatroom.module.message.domain.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

/**
 * 发送消息请求 DTO
 */
@Data
public class SendMessageDTO {

    /** 会话数据库 ID（雪花ID，前端直接传，省去 sessionNo 转换） */
    @NotNull(message = "会话ID不能为空")
    private Long sessionId;

    /** 客户端生成的消息唯一编号（UUID，用于幂等） */
    @NotBlank(message = "消息编号不能为空")
    private String msgNo;

    /** 1文本 2图片 3语音 4视频 5文件 */
    @NotNull(message = "消息类型不能为空")
    @Min(value = 1, message = "消息类型不合法")
    @Max(value = 5, message = "消息类型不合法")
    private Integer msgType;

    private String content;

    /** 引用回复的消息ID（可选） */
    private Long replyMsgId;

    /** 扩展字段 JSON（可选，如图片宽高、文件大小等） */
    private String extra;
}
