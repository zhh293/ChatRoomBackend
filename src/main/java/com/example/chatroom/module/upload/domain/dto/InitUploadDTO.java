package com.example.chatroom.module.upload.domain.dto;

import jakarta.validation.constraints.*;
import lombok.Data;

/**
 * 初始化上传任务请求 DTO
 *
 * 前端在开始上传前调用 POST /api/v1/upload/init，
 * 后端创建任务 ID 并返回，后续所有分片上传都携带此 taskId。
 */
@Data
public class InitUploadDTO {

    /**
     * 整个文件的 MD5（小写十六进制，32位）
     * 用于最终合并后的完整性校验
     */
    @NotBlank(message = "fileMd5 不能为空")
    @Pattern(regexp = "^[a-f0-9]{32}$", message = "fileMd5 格式不正确（小写十六进制 32 位）")
    private String fileMd5;

    /**
     * 分片总数
     * 后端据此初始化 bitmap，并在合并时校验所有分片是否到齐
     */
    @NotNull(message = "totalChunks 不能为空")
    @Min(value = 1, message = "分片数至少为 1")
    @Max(value = 1000, message = "分片数不能超过 1000")
    private Integer totalChunks;

    /**
     * 文件总字节数
     * 用于前置校验文件大小是否超限
     */
    @NotNull(message = "fileSize 不能为空")
    @Positive(message = "fileSize 必须为正数")
    private Long fileSize;

    /**
     * 文件 MIME 类型（如 image/jpeg）
     * 后端会与白名单比对，不在白名单内直接拒绝
     */
    @NotBlank(message = "contentType 不能为空")
    private String contentType;

    /**
     * 原始文件名（含扩展名，如 avatar.jpg）
     * 用于生成 OSS objectKey 时保留扩展名
     */
    @NotBlank(message = "fileName 不能为空")
    @Size(max = 255, message = "文件名过长")
    private String fileName;
}
