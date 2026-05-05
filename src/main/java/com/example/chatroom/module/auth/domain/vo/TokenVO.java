package com.example.chatroom.module.auth.domain.vo;

import lombok.Data;

/**
 * 登录/刷新 Token 响应 VO
 *
 * 字段与前端 authApi 严格对齐：
 *   data.data.accessToken
 *   data.data.refreshToken
 *   data.data.user  →  UserInfo { id, username, nickname, avatar, bio, status, createdAt }
 */
@Data
public class TokenVO {

    private String accessToken;

    private String refreshToken;

    /** Access Token 过期时间（秒），前端可用于倒计时刷新 */
    private Long accessTokenExpire;

    /** 当前登录用户基础信息，登录/刷新后前端直接写入 authStore，无需再请求 /users/me */
    private UserInfoVO user;
}
