package com.example.chatroom.module.message.domain.vo;

import lombok.Data;

import java.time.LocalDateTime;

/**
 * 消息响应 VO
 */
@Data
public class MessageVO {

    private Long msgId;
    private String msgNo;
    private Long sessionId;
    private Long senderId;
    private String senderNickname;
    private String senderAvatarUrl;
    private Integer msgType;
    private String content;
    private String extra;
    private Long replyMsgId;
    private Integer status;
    private LocalDateTime createdAt;
}
