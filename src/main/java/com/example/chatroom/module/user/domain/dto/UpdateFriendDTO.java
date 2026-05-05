package com.example.chatroom.module.user.domain.dto;

import lombok.Data;

/**
 * 修改好友关系请求 DTO（备注 / 拉黑）
 */
@Data
public class UpdateFriendDTO {

    /** 好友备注，null 表示不修改 */
    private String remark;

    /**
     * 好友状态：1 正常，2 拉黑
     * null 表示不修改
     */
    private Integer status;
}
