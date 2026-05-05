package com.example.chatroom.module.user.domain.dto;

import jakarta.validation.constraints.Size;
import lombok.Data;

/**
 * 修改用户基础信息 DTO
 */
@Data
public class UpdateUserDTO {

    @Size(max = 64, message = "昵称最长64位")
    private String nickname;

    @Size(max = 256, message = "简介最长256位")
    private String bio;

    /** 性别：0未知 1男 2女 */
    private Integer gender;
}
