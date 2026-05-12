package com.example.chatroom.module.call.domain.vo;

import lombok.Data;

/**
 * 发起通话响应
 */
@Data
public class CallInitiateVO {

    /** 通话唯一标识 */
    private Long callId;

    /** 关联的消息ID */
    private Long msgId;

    /** 幂等编号（原样返回） */
    private String msgNo;

    /** 发送状态：sending */
    private String status;
}
