package com.example.chatroom.module.session.domain.dto;

import lombok.Data;

/**
 * 已读位置查询结果 DTO
 * 对应 session_member JOIN session 的查询结果
 */
@Data
public class LastReadPositionDTO {

    /** 会话编号（对前端暴露的唯一标识） */
    private String sessionNo;

    /**
     * 该用户在该会话中最后已读的消息 ID
     * null 表示从未读过任何消息
     */
    private Long lastReadMsgId;
}
