package com.example.chatroom.module.session.domain.vo;

import com.example.chatroom.common.response.PageResult;
import lombok.Data;

/**
 * 会话列表响应 VO
 * 在分页结果基础上额外返回当前用户所有会话的未读总数
 */
@Data
public class SessionListVO {

    /** 分页会话列表（每条 SessionVO 含各自的 unreadCount） */
    private PageResult<SessionVO> page;

    /** 当前用户所有会话未读数之和（用于 App 角标展示） */
    private Integer totalUnread;

    public static SessionListVO of(PageResult<SessionVO> page, int totalUnread) {
        SessionListVO vo = new SessionListVO();
        vo.page = page;
        vo.totalUnread = totalUnread;
        return vo;
    }
}
