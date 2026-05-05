package com.example.chatroom.module.session.domain.vo;

import lombok.Data;

/**
 * 已读位置响应 VO
 * <p>
 * 对应接口 GET /api/v1/sessions/read-positions 的单条数据
 */
@Data
public class ReadPositionVO {

    /**
     * 会话编号（前端唯一标识）
     */
    private String sessionNo;

    /**
     * 该用户在该会话中最后已读的消息 ID
     * null 表示从未读过任何消息（即 last_read_msg_id IS NULL）
     */
    private Long lastReadMsgId;
}
