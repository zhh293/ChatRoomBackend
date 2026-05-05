package com.example.chatroom.module.user.domain.vo;

import lombok.Data;

import java.time.LocalDate;

/**
 * 用户扩展信息响应 VO
 */
@Data
public class UserProfileVO {

    private String realName;
    private LocalDate birthday;
    private String region;
    private String signature;
    private String backgroundUrl;
}
