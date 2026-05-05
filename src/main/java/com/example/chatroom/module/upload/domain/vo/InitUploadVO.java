package com.example.chatroom.module.upload.domain.vo;

import lombok.Data;

/**
 * 初始化上传任务响应 VO
 */
@Data
public class InitUploadVO {

    /** 上传任务 ID，后续所有分片上传和查询都需要携带 */
    private String taskId;

    /** 任务过期时间（Unix 时间戳，秒），前端可据此提示用户 */
    private long expireAt;
}
