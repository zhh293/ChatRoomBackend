package com.example.chatroom.module.user.domain.vo;

import lombok.Data;

/**
 * 用户信息响应 VO（对外展示字段）
 */
@Data
public class UserVO {

    private String userNo;
    private String username;
    private String nickname;
    private String avatarUrl;
    private String email;
    private String phone;
    private Integer gender;
    private String bio;
    private Integer status;
}
