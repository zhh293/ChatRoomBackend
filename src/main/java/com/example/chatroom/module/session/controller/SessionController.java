package com.example.chatroom.module.session.controller;

import com.example.chatroom.common.interceptor.UserContext;
import com.example.chatroom.common.response.PageResult;
import com.example.chatroom.common.response.Result;
import com.example.chatroom.module.session.domain.dto.CreateGroupSessionDTO;
import com.example.chatroom.module.session.domain.dto.CreateSingleSessionDTO;
import com.example.chatroom.module.session.domain.dto.UpdateGroupSessionDTO;
import com.example.chatroom.module.session.domain.vo.ReadPositionVO;
import com.example.chatroom.module.session.domain.vo.SessionListVO;
import com.example.chatroom.module.session.domain.vo.SessionMemberVO;
import com.example.chatroom.module.session.domain.vo.SessionVO;
import com.example.chatroom.module.session.service.SessionService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 会话模块 Controller
 */
@RestController
@RequestMapping("/api/v1/sessions")
@RequiredArgsConstructor
public class SessionController {

    private final SessionService sessionService;

    /**
     * 获取当前用户的会话列表（分页）
     *
     * @param page           页码，从 1 开始，默认 1
     * @param size           每页条数，默认 20
     * @param lastReadMsgIds 前端本地记录的各会话已读位置，key=sessionNo，value=lastReadMsgId
     *                       用于服务端实时计算未读数，不传则全部按 DB 中存储值处理
     */
    @GetMapping
    public Result<SessionListVO> listSessions(
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(required = false) Map<String, Long> lastReadMsgIds) {
        if (lastReadMsgIds == null) lastReadMsgIds = Collections.emptyMap();
        return Result.ok(sessionService.listSessions(UserContext.getRequired(), page, size, lastReadMsgIds));
    }

    /**
     * 分页拉取当前用户各会话的已读位置
     *
     * <p><b>接口：</b> GET /api/v1/sessions/read-positions
     *
     * <p><b>调用时机：</b>
     * 前端首次加载或重新登录时调用，把本地缓存的 lastReadMsgId 与服务端对齐。
     * 之后的增量更新通过 WebSocket 推送，不需要再轮询此接口。
     *
     * <p><b>数据库设计：</b>
     * 数据来源于 session_member.last_read_msg_id，该字段：
     * <ul>
     *   <li>NULL = 用户从未在该会话中上报过已读，前端应把所有消息视为未读</li>
     *   <li>非 NULL = 用户最后一次上报已读时的消息 ID（雪花ID，单调递增）</li>
     *   <li>只增不减：上报时执行 UPDATE ... WHERE last_read_msg_id &lt; #{newId}，防止旧请求覆盖新进度</li>
     * </ul>
     *
     * <p><b>分页策略：</b>
     * 按 session_id DESC 排序（最近活跃的会话优先），每页建议 15 条，最大 50 条。
     * 前端分批拉取直到 hasMore=false 为止。
     *
     * @param page 页码，从 1 开始，默认 1
     * @param size 每页条数，默认 15，最大 50
     * @return 分页结果，每条包含 sessionNo + lastReadMsgId
     */
    @GetMapping("/read-positions")
    public Result<PageResult<ReadPositionVO>> getReadPositions(
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "15") int size) {
        return Result.ok(sessionService.getReadPositions(UserContext.getRequired(), page, size));
    }

    /** 创建或获取单聊会话 */
    @PostMapping("/single")
    public Result<SessionVO> getOrCreateSingleSession(@Valid @RequestBody CreateSingleSessionDTO dto) {
        return Result.ok(sessionService.getOrCreateSingleSession(UserContext.getRequired(), dto));
    }

    /** 创建群聊会话 */
    @PostMapping("/group")
    public Result<SessionVO> createGroupSession(@Valid @RequestBody CreateGroupSessionDTO dto) {
        return Result.ok(sessionService.createGroupSession(UserContext.getRequired(), dto));
    }

    /** 修改群聊信息（群名/头像），仅群主或管理员可操作 */
    @PutMapping("/{sessionId}")
    public Result<Void> updateGroupSession(@PathVariable Long sessionId,
                                           @Valid @RequestBody UpdateGroupSessionDTO dto) {
        sessionService.updateGroupSession(UserContext.getRequired(), sessionId, dto);
        return Result.ok();
    }

    /** 获取会话详情 */
    @GetMapping("/{sessionId}")
    public Result<SessionVO> getSessionDetail(@PathVariable Long sessionId) {
        return Result.ok(sessionService.getSessionDetail(UserContext.getRequired(), sessionId));
    }

    /** 退出/解散会话 */
    @DeleteMapping("/{sessionId}")
    public Result<Void> leaveSession(@PathVariable Long sessionId) {
        sessionService.leaveSession(UserContext.getRequired(), sessionId);
        return Result.ok();
    }

    /** 邀请成员入群 */
    @PostMapping("/{sessionId}/members")
    public Result<Void> inviteMembers(@PathVariable Long sessionId,
                                      @RequestBody java.util.List<String> memberUserNos) {
        sessionService.inviteMembers(UserContext.getRequired(), sessionId, memberUserNos);
        return Result.ok();
    }

    /** 踢出成员 */
    @DeleteMapping("/{sessionId}/members/{userId}")
    public Result<Void> kickMember(@PathVariable Long sessionId,
                                   @PathVariable Long userId) {
        sessionService.kickMember(UserContext.getRequired(), sessionId, userId);
        return Result.ok();
    }

    /**
     * 获取会话成员列表
     *
     * <p>调用方必须是该会话的在群成员，否则返回 NOT_IN_SESSION。
     *
     * @param sessionId 会话 ID
     * @return 成员列表，含用户基本信息 + 角色
     */
    @GetMapping("/{sessionId}/members")
    public Result<List<SessionMemberVO>> getMembers(@PathVariable Long sessionId) {
        return Result.ok(sessionService.getMembers(UserContext.getRequired(), sessionId));
    }
}
