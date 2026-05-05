package com.example.chatroom.module.session.domain.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

/**
 * 创建单聊会话请求 DTO
 */
@Data
public class CreateSingleSessionDTO {

    @NotBlank(message = "对方用户编号不能为空")
    private String targetUserNo;
}
