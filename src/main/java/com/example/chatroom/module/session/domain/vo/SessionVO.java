package com.example.chatroom.module.session.domain.vo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 会话响应 VO
 */
@Data
public class SessionVO {

    private String sessionNo;
    private Integer type;
    private String name;
    private String avatarUrl;
    private String lastMsgContent;
    private LocalDateTime lastMsgAt;
    private Integer unreadCount;
    private Boolean isPinned;
    private Boolean isDisturb;
    private Integer memberCount;

    /** 内部字段，不对前端暴露，用于 fillUnreadCount 时直接定位 Redis key */
    @JsonIgnore
    private Long sessionId;

    /** 内部字段，前端传入的 lastReadMsgId，用于计算未读数 */
    @JsonIgnore
    private Long lastReadMsgId;
}
