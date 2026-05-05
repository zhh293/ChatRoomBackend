package com.example.chatroom.module.auth.domain.vo;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

/**
 * 认证模块内嵌用户信息 VO
 *
 * 字段名与前端 UserInfo 接口严格对齐：
 * <pre>
 * interface UserInfo {
 *   id: string;          ← userNo（对外唯一标识，非数据库自增 id）
 *   username: string;
 *   nickname: string;
 *   avatar?: string;     ← avatarUrl
 *   bio?: string;
 *   status?: 'online' | 'offline';
 *   createdAt?: string;
 * }
 * </pre>
 */
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class UserInfoVO {

    /** 对外唯一标识（雪花 ID 字符串），前端 UserInfo.id */
    private String id;

    private String username;

    private String nickname;

    /** 头像 URL，前端字段名为 avatar */
    private String avatar;

    private String bio;

    /** 在线状态：online / offline，登录后默认 online */
    private String status;

    /** 注册时间，ISO 8601 字符串 */
    private String createdAt;
}
