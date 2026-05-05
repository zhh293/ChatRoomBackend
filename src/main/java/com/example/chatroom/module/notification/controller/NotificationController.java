package com.example.chatroom.module.notification.controller;

import com.example.chatroom.common.interceptor.UserContext;
import com.example.chatroom.common.response.Result;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

/**
 * 系统通知 Controller
 * TODO: 实现通知列表、已读等接口
 */
@RestController
@RequestMapping("/api/v1/notifications")
@RequiredArgsConstructor
public class NotificationController {

    /** 系统通知列表 */
    @GetMapping
    public Result<?> listNotifications(
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "20") int size) {
        // TODO: 实现
        return Result.ok();
    }

    /** 标记通知已读 */
    @PutMapping("/{id}/read")
    public Result<Void> markRead(@PathVariable Long id) {
        // TODO: 实现
        return Result.ok();
    }

    /** 全部通知已读 */
    @PutMapping("/read-all")
    public Result<Void> markAllRead() {
        // TODO: 实现
        return Result.ok();
    }
}
