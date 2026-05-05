package com.example.chatroom.module.user.domain.dto;

import lombok.Data;

import java.time.LocalDate;

/**
 * 更新用户扩展信息请求 DTO
 */
@Data
public class UpdateProfileDTO {

    private String realName;
    private LocalDate birthday;
    private String region;
    private String signature;
    private String backgroundUrl;
}
