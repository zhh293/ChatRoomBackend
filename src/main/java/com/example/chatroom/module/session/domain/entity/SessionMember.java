package com.example.chatroom.module.session.domain.entity;

import com.baomidou.mybatisplus.annotation.*;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 会话成员表实体
 */
@Data
@TableName("session_member")
public class SessionMember {

    @TableId(type = IdType.AUTO)
    private Long id;

    private Long sessionId;
    private Long userId;
    /** 1普通成员 2管理员 3群主 */
    private Integer role;
    private String alias;
    private Integer isMuted;
    private Integer isPinned;
    private Integer isDisturb;
    /** 已读到的最后消息ID（用于计算未读数） */
    private Long lastReadMsgId;
    private LocalDateTime joinedAt;
    private LocalDateTime leftAt;

    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createdAt;

    @TableField(fill = FieldFill.INSERT_UPDATE)
    private LocalDateTime updatedAt;
}
