package com.example.chatroom.module.user.controller;

import com.example.chatroom.common.annotation.RateLimit;
import com.example.chatroom.common.interceptor.UserContext;
import com.example.chatroom.common.response.Result;
import com.example.chatroom.module.auth.domain.vo.TokenVO;
import com.example.chatroom.module.user.domain.dto.RegisterDTO;
import com.example.chatroom.module.user.domain.dto.UpdateFriendDTO;
import com.example.chatroom.module.user.domain.dto.UpdateProfileDTO;
import com.example.chatroom.module.user.domain.dto.UpdateUserDTO;
import com.example.chatroom.module.user.domain.vo.UserProfileVO;
import com.example.chatroom.module.user.domain.vo.UserVO;
import com.example.chatroom.module.user.service.UserService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * 用户模块 Controller
 */
@RestController
@RequestMapping("/api/v1/users")
@RequiredArgsConstructor
public class UserController {

    private final UserService userService;

    /**
     * 用户注册
     * 限流：同一 IP 每秒最多 2 次，防暴力注册
     * 注册成功后直接返回 TokenVO，前端无需二次登录
     */
    @PostMapping("/register")
    @RateLimit(key = "register", permitsPerSecond = 2.0, limitByIp = true)
    public Result<TokenVO> register(@Valid @RequestBody RegisterDTO dto,
                                    HttpServletRequest request) {
        String clientIp = getClientIp(request);
        return Result.ok(userService.register(dto, clientIp));
    }

    /** 获取当前用户信息 */
    @GetMapping("/me")
    public Result<UserVO> getMe() {
        return Result.ok(userService.getMe(UserContext.getRequired()));
    }

    /** 修改当前用户基础信息 */
    @PutMapping("/me")
    public Result<Void> updateMe(@Valid @RequestBody UpdateUserDTO dto) {
        userService.updateMe(UserContext.getRequired(), dto);
        return Result.ok();
    }

    /** 注销账号 */
    @DeleteMapping("/me")
    public Result<Void> deleteMe() {
        userService.deleteMe(UserContext.getRequired());
        return Result.ok();
    }

    /** 查看指定用户公开信息 */
    @GetMapping("/{userNo}")
    public Result<UserVO> getUserByNo(@PathVariable String userNo) {
        return Result.ok(userService.getUserByNo(userNo));
    }

    /** 搜索用户 */
    @GetMapping("/search")
    public Result<List<UserVO>> searchUsers(@RequestParam String keyword) {
        return Result.ok(userService.searchUsers(keyword));
    }

    /** 获取好友列表 */
    @GetMapping("/me/friends")
    public Result<List<UserVO>> getFriends() {
        return Result.ok(userService.getFriends(UserContext.getRequired()));
    }

    /** 添加好友 */
    @PostMapping("/me/friends")
    public Result<Void> addFriend(@RequestParam String friendUserNo) {
        userService.addFriend(UserContext.getRequired(), friendUserNo);
        return Result.ok();
    }

    /** 删除好友 */
    @DeleteMapping("/me/friends/{friendId}")
    public Result<Void> deleteFriend(@PathVariable Long friendId) {
        userService.deleteFriend(UserContext.getRequired(), friendId);
        return Result.ok();
    }

    /** 修改好友备注 / 拉黑 */
    @PutMapping("/me/friends/{friendId}")
    public Result<Void> updateFriend(@PathVariable Long friendId,
                                     @RequestBody UpdateFriendDTO dto) {
        userService.updateFriend(UserContext.getRequired(), friendId, dto);
        return Result.ok();
    }

    /** 更新头像 URL */
    @PutMapping("/me/avatar")
    public Result<Void> updateAvatar(@RequestParam String avatarUrl) {
        userService.updateAvatar(UserContext.getRequired(), avatarUrl);
        return Result.ok();
    }

    /** 修改密码 */
    @PutMapping("/me/password")
    public Result<Void> updatePassword(@RequestBody PasswordBody body) {
        userService.updatePassword(UserContext.getRequired(), body.oldPassword(), body.newPassword());
        return Result.ok();
    }

    /** 获取用户扩展信息 */
    @GetMapping("/me/profile")
    public Result<UserProfileVO> getProfile() {
        return Result.ok(userService.getProfile(UserContext.getRequired()));
    }

    /** 更新用户扩展信息 */
    @PutMapping("/me/profile")
    public Result<Void> updateProfile(@RequestBody UpdateProfileDTO dto) {
        userService.updateProfile(UserContext.getRequired(), dto);
        return Result.ok();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 内部 Record
    // ─────────────────────────────────────────────────────────────────────────

    record PasswordBody(String oldPassword, String newPassword) {}

    // ─────────────────────────────────────────────────────────────────────────
    // 私有工具方法
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * 获取客户端真实 IP（兼容反向代理）
     */
    private String getClientIp(HttpServletRequest request) {
        String ip = request.getHeader("X-Forwarded-For");
        if (ip != null && !ip.isBlank() && !"unknown".equalsIgnoreCase(ip)) {
            // X-Forwarded-For 可能包含多个 IP，取第一个（最原始客户端）
            return ip.split(",")[0].trim();
        }
        ip = request.getHeader("X-Real-IP");
        if (ip != null && !ip.isBlank() && !"unknown".equalsIgnoreCase(ip)) {
            return ip;
        }
        return request.getRemoteAddr();
    }
}
