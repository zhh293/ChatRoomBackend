package com.example.chatroom.module.user.domain.entity;

import com.baomidou.mybatisplus.annotation.*;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 好友关系表
 */
@Data
@TableName("user_friend")
public class UserFriend {

    @TableId(type = IdType.AUTO)
    private Long id;

    private Long userId;
    private Long friendId;
    private String remark;
    /** 1正常 2拉黑 */
    private Integer status;

    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createdAt;

    @TableField(fill = FieldFill.INSERT_UPDATE)
    private LocalDateTime updatedAt;
}
