package com.example.chatroom.module.upload.domain.vo;

import lombok.Data;

/**
 * 合并完成响应 VO
 *
 * 前端调用 POST /api/v1/upload/{taskId}/complete 后，
 * 后端合并分片、校验 MD5、上传 OSS，返回此 VO。
 * 前端拿到 url 后调用更新头像接口即可。
 */
@Data
public class UploadResultVO {

    /** 文件在 OSS 上的公网访问 URL */
    private String url;

    /** 文件 MIME 类型 */
    private String contentType;

    /** 文件字节数 */
    private long fileSize;
}
