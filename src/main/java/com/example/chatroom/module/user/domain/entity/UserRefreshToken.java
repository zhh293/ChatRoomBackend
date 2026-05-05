package com.example.chatroom.module.user.domain.entity;

import com.baomidou.mybatisplus.annotation.*;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * Refresh Token 存储表
 */
@Data
@TableName("user_refresh_token")
public class UserRefreshToken {

    @TableId(type = IdType.AUTO)
    private Long id;

    private Long userId;
    /** Refresh Token SHA256 哈希 */
    private String tokenHash;
    private String deviceInfo;
    private String ip;
    private LocalDateTime expiresAt;
    /** 是否已撤销：0否 1是 */
    private Integer revoked;

    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createdAt;
}
