package com.example.chatroom.module.user.domain.entity;

import com.baomidou.mybatisplus.annotation.*;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 用户表实体
 */
@Data
@TableName("user")
public class User {

    @TableId(type = IdType.AUTO)
    private Long id;

    private String userNo;
    private String username;
    private String nickname;
    private String passwordHash;
    private String avatarUrl;
    private String email;
    private String phone;
    private Integer gender;
    private String bio;
    /** 账号状态：1正常 2禁用 3注销 */
    private Integer status;
    private LocalDateTime lastLoginAt;
    private String lastLoginIp;

    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createdAt;

    @TableField(fill = FieldFill.INSERT_UPDATE)
    private LocalDateTime updatedAt;

    @TableLogic
    private LocalDateTime deletedAt;
}
