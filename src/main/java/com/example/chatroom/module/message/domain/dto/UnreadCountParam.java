package com.example.chatroom.module.message.domain.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 批量查询未读数的单条入参
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class UnreadCountParam {
    /** 会话 ID（用于分片路由） */
    private Long sessionId;
    /** 该会话的 lastReadMsgId */
    private Long lastReadMsgId;
}
