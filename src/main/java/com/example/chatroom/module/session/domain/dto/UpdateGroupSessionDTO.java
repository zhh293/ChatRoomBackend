package com.example.chatroom.module.session.domain.dto;

import jakarta.validation.constraints.Size;
import lombok.Data;

/**
 * 修改群聊信息请求 DTO
 * <p>字段均为可选，前端只传需要修改的字段，null 表示不修改</p>
 */
@Data
public class UpdateGroupSessionDTO {

    /** 群名，1~50 字符 */
    @Size(min = 1, max = 50, message = "群名长度须在 1~50 字符之间")
    private String name;

    /** 群头像 URL */
    @Size(max = 512, message = "头像 URL 长度不能超过 512 字符")
    private String avatarUrl;
}
