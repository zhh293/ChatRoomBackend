package com.example.chatroom.module.notification.domain.entity;

import com.baomidou.mybatisplus.annotation.*;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 系统通知表实体
 */
@Data
@TableName("system_notification")
public class SystemNotification {

    @TableId(type = IdType.AUTO)
    private Long id;

    private Long userId;
    /** 1好友申请 2被踢出群 3系统公告 */
    private Integer type;
    private String title;
    private String content;
    private String extra;
    private Integer isRead;

    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createdAt;
}
