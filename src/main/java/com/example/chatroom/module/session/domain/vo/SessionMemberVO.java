package com.example.chatroom.module.session.domain.vo;

import lombok.Data;

/**
 * 会话成员响应 VO
 * 对应接口 GET /api/v1/sessions/{sessionNo}/members 的单条数据
 */
@Data
public class SessionMemberVO {

    /** 用户对外唯一标识 */
    private String userNo;

    private String username;
    private String nickname;
    private String avatarUrl;
    private String bio;
    private Integer gender;

    /**
     * 成员角色
     * 1=普通成员 2=管理员 3=群主
     */
    private Integer role;
}
