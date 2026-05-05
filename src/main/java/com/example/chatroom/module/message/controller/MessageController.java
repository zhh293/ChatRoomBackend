package com.example.chatroom.module.message.controller;

import com.example.chatroom.common.annotation.RateLimit;
import com.example.chatroom.common.interceptor.UserContext;
import com.example.chatroom.common.response.PageResult;
import com.example.chatroom.common.response.Result;
import com.example.chatroom.module.message.domain.dto.SendMessageDTO;
import com.example.chatroom.module.message.domain.vo.MessageVO;
import com.example.chatroom.module.message.domain.vo.SendMessageVO;
import com.example.chatroom.module.message.service.MessageService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

/**
 * 消息模块 Controller
 */
@RestController
@RequestMapping("/api/v1/messages")
@RequiredArgsConstructor
public class MessageController {

    private final MessageService messageService;

    /** 发送消息（按用户ID限流：每秒最多10条） */
    @PostMapping
    @RateLimit(key = "sendMessage", permitsPerSecond = 10.0, limitByUserId = true)
    public Result<SendMessageVO> sendMessage(@Valid @RequestBody SendMessageDTO dto) {
        return Result.ok(messageService.sendMessage(UserContext.getRequired(), dto));
    }

    /** 历史消息游标分页查询 */
    @GetMapping
    public Result<PageResult<MessageVO>> listMessages(
            @RequestParam Long sessionId,
            @RequestParam(required = false) Long cursor,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(defaultValue = "before") String direction) {
        return Result.ok(messageService.listMessages(
                UserContext.getRequired(), sessionId, cursor, size, direction));
    }

    /** 撤回消息 */
    @DeleteMapping("/{msgId}")
    public Result<Void> revokeMessage(@PathVariable Long msgId) {
        messageService.revokeMessage(UserContext.getRequired(), msgId);
        return Result.ok();
    }

    /** 标记消息已读 */
    @PostMapping("/{msgId}/read")
    public Result<Void> markRead(@PathVariable Long msgId) {
        messageService.markRead(UserContext.getRequired(), msgId);
        return Result.ok();
    }

    /** 标记会话全部已读 */
    @PostMapping("/read-all")
    public Result<Void> markAllRead(@RequestParam Long sessionId) {
        messageService.markAllRead(UserContext.getRequired(), sessionId);
        return Result.ok();
    }
}
