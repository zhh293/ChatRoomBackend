package com.example.chatroom.module.auth.controller;

import com.example.chatroom.common.annotation.RateLimit;
import com.example.chatroom.common.response.Result;
import com.example.chatroom.module.auth.domain.dto.LoginDTO;
import com.example.chatroom.module.auth.domain.vo.TokenVO;
import com.example.chatroom.module.auth.service.AuthService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

/**
 * 认证模块 Controller
 *
 * 响应结构（与前端 authApi 严格对齐）：
 * <pre>
 * {
 *   "code": 0,
 *   "msg": "success",
 *   "data": {
 *     "accessToken": "...",
 *     "refreshToken": "...",
 *     "accessTokenExpire": 1800,
 *     "user": {
 *       "id": "...",          // userNo
 *       "username": "...",
 *       "nickname": "...",
 *       "avatar": "...",      // avatarUrl
 *       "bio": "...",
 *       "status": "online",
 *       "createdAt": "..."
 *     }
 *   }
 * }
 * </pre>
 */
@RestController
@RequestMapping("/api/v1/auth")
@RequiredArgsConstructor
public class AuthController {

    private final AuthService authService;

    /**
     * 登录
     * 限流：每个 IP 每秒最多 2 次，防暴力破解
     */
    @PostMapping("/login")
    @RateLimit(key = "auth:login", permitsPerSecond = 2.0, limitByIp = true)
    public Result<TokenVO> login(@Valid @RequestBody LoginDTO dto, HttpServletRequest request) {
        String clientIp = getClientIp(request);
        return Result.ok(authService.login(dto, clientIp));
    }

    /**
     * 注册（路由在 UserController，此处仅保留登出/刷新）
     */

    /**
     * 登出
     * 将 Access Token 加入黑名单，撤销 Refresh Token
     */
    @PostMapping("/logout")
    public Result<Void> logout(
            @RequestHeader(value = "Authorization", required = false) String authHeader,
            @RequestBody(required = false) LogoutBody body) {
        String accessToken = null;
        if (authHeader != null && authHeader.startsWith("Bearer ")) {
            accessToken = authHeader.substring(7);
        }
        String refreshToken = (body != null) ? body.refreshToken() : null;
        authService.logout(accessToken, refreshToken);
        return Result.ok();
    }

    /**
     * 刷新 Access Token
     * 前端传 { refreshToken: "..." }，返回新的双 Token + user 信息
     */
    @PostMapping("/refresh")
    @RateLimit(key = "auth:refresh", permitsPerSecond = 5.0, limitByIp = true)
    public Result<TokenVO> refresh(@RequestBody RefreshBody body) {
        return Result.ok(authService.refresh(body.refreshToken()));
    }

    // ── 内部 Record，避免为简单请求体单独建 DTO 文件 ──────────────────────

    record LogoutBody(String refreshToken) {}

    record RefreshBody(String refreshToken) {}

    // ── 工具方法 ──────────────────────────────────────────────────────────

    private String getClientIp(HttpServletRequest request) {
        String[] headers = {"X-Forwarded-For", "X-Real-IP", "Proxy-Client-IP"};
        for (String header : headers) {
            String ip = request.getHeader(header);
            if (ip != null && !ip.isEmpty() && !"unknown".equalsIgnoreCase(ip)) {
                return ip.split(",")[0].trim();
            }
        }
        return request.getRemoteAddr();
    }
}
