package com.example.chatroom.module.auth.service;

import com.example.chatroom.module.auth.domain.dto.LoginDTO;
import com.example.chatroom.module.auth.domain.vo.TokenVO;

/**
 * 认证服务接口
 */
public interface AuthService {

    /** 登录，返回双 Token */
    TokenVO login(LoginDTO dto, String clientIp);

    /** 登出，将 Access Token 加入黑名单，撤销 Refresh Token */
    void logout(String accessToken, String refreshToken);

    /** 刷新 Access Token */
    TokenVO refresh(String refreshToken);
}
