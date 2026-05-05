package com.example.chatroom.module.session.domain.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;
import lombok.Data;

import java.util.List;

/**
 * 创建群聊会话请求 DTO
 */
@Data
public class CreateGroupSessionDTO {

    @NotBlank(message = "群名不能为空")
    @Size(max = 128, message = "群名最长128位")
    private String name;

    @NotEmpty(message = "初始成员不能为空")
    private List<String> memberUserNos;
}
