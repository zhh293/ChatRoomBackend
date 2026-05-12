package com.example.chatroom.module.call.domain.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

/**
 * 发起语音通话请求
 */
@Data
public class CallInitiateDTO {

    /** 会话ID */
    @NotNull(message = "会话ID不能为空")
    private Long sessionId;

    /** 客户端生成的幂等编号（UUID） */
    @NotBlank(message = "消息编号不能为空")
    private String msgNo;
}
