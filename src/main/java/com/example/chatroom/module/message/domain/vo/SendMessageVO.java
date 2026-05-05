package com.example.chatroom.module.message.domain.vo;

import lombok.Data;

/**
 * 发送消息响应 VO
 */
@Data
public class SendMessageVO {

    private Long msgId;
    private String msgNo;
    /** sending / sent / failed */
    private String status;
}
