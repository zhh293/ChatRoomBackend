package com.example.chatroom.module.session.domain.entity;

import com.baomidou.mybatisplus.annotation.*;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 会话表实体
 */
@Data
@TableName("session")
public class Session {

    @TableId(type = IdType.AUTO)
    private Long id;

    private String sessionNo;
    /** 1单聊 2群聊 */
    private Integer type;
    private String name;
    private String avatarUrl;
    private Long ownerId;
    private Long lastMsgId;
    private String lastMsgContent;
    private LocalDateTime lastMsgAt;
    private Integer memberCount;
    /** 1正常 2解散 */
    private Integer status;

    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createdAt;

    @TableField(fill = FieldFill.INSERT_UPDATE)
    private LocalDateTime updatedAt;

    @TableLogic
    private LocalDateTime deletedAt;
}
