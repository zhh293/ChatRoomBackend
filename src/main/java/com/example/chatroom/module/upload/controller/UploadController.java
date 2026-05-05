package com.example.chatroom.module.upload.controller;

import com.example.chatroom.common.annotation.RateLimit;
import com.example.chatroom.common.interceptor.UserContext;
import com.example.chatroom.common.response.Result;
import com.example.chatroom.module.upload.domain.dto.InitUploadDTO;
import com.example.chatroom.module.upload.domain.vo.InitUploadVO;
import com.example.chatroom.module.upload.domain.vo.UploadResultVO;
import com.example.chatroom.module.upload.domain.vo.UploadStatusVO;
import com.example.chatroom.module.upload.service.UploadService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import lombok.RequiredArgsConstructor;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

/**
 * 通用文件分片上传 Controller
 *
 * 接口一览：
 * ┌─────────────────────────────────────────────────────────────────────────┐
 * │ POST   /api/v1/upload/init              初始化上传任务，返回 taskId      │
 * │ POST   /api/v1/upload/{taskId}/chunk    上传单个分片                     │
 * │ GET    /api/v1/upload/{taskId}/status   查询分片状态（断点续传）          │
 * │ POST   /api/v1/upload/{taskId}/complete 合并分片，上传 OSS，返回 URL     │
 * └─────────────────────────────────────────────────────────────────────────┘
 *
 * 所有接口均需登录（JwtAuthFilter 不在白名单），userId 从 UserContext 获取。
 * 限流：初始化接口按 IP 限流（防止恶意创建大量任务），分片上传按用户限流。
 */
@Validated
@RestController
@RequestMapping("/api/v1/upload")
@RequiredArgsConstructor
public class UploadController {

    private final UploadService uploadService;

    // ─────────────────────────────────────────────────────────────────────────
    // 1. 初始化上传任务
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * 初始化上传任务
     *
     * 请求体（JSON）：
     * {
     *   "fileMd5":     "d41d8cd98f00b204e9800998ecf8427e",  // 整个文件的 MD5
     *   "totalChunks": 5,                                   // 分片总数
     *   "fileSize":    5242880,                             // 文件总字节数
     *   "contentType": "image/jpeg",                        // MIME 类型
     *   "fileName":    "avatar.jpg"                         // 原始文件名
     * }
     *
     * 响应：{ "taskId": "xxx", "expireAt": 1700000000 }
     */
    @PostMapping("/init")
    @RateLimit(key = "upload:init", permitsPerSecond = 5.0, limitByIp = true)
    public Result<InitUploadVO> initUpload(@Valid @RequestBody InitUploadDTO dto) {
        return Result.ok(uploadService.initUpload(dto, UserContext.getRequired()));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 2. 上传单个分片
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * 上传单个分片（multipart/form-data）
     *
     * 表单字段：
     *   file        → 分片二进制内容（MultipartFile）
     *   chunkIndex  → 分片序号（0-based）
     *   chunkMd5    → 该分片内容的 MD5（小写十六进制 32 位），用于校验传输完整性
     *
     * 幂等：同一分片重复上传（超时重传场景）会被 bitmap 去重，不会写多余文件。
     * 超时重传：前端收到非 2xx 响应时重传，后端幂等保证安全。
     */
    @PostMapping("/{taskId}/chunk")
    @RateLimit(key = "upload:chunk", permitsPerSecond = 20.0)
    public Result<Void> uploadChunk(
            @PathVariable String taskId,
            @RequestParam int chunkIndex,
            @RequestParam
            @NotBlank(message = "chunkMd5 不能为空")
            @Pattern(regexp = "^[a-fA-F0-9]{32}$", message = "chunkMd5 格式不正确")
            String chunkMd5,
            @RequestPart("file") MultipartFile file) {

        uploadService.uploadChunk(taskId, chunkIndex, chunkMd5, file,
                UserContext.getRequired());
        return Result.ok();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 3. 查询分片上传状态（断点续传）
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * 查询分片上传状态
     *
     * 响应示例：
     * {
     *   "taskId":       "xxx",
     *   "totalChunks":  5,
     *   "uploadedCount": 3,
     *   "missingChunks": [1, 3],   // 前端只需重传这两个分片
     *   "complete":     false
     * }
     */
    @GetMapping("/{taskId}/status")
    public Result<UploadStatusVO> getStatus(@PathVariable String taskId) {
        return Result.ok(uploadService.getStatus(taskId, UserContext.getRequired()));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 4. 合并分片并上传 OSS
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * 合并分片，上传 OSS，返回文件 URL
     *
     * 前端在所有分片上传完毕（或通过 status 接口确认 complete=true）后调用。
     * 后端会：
     *   1. 校验所有分片已到齐
     *   2. 按序合并临时分片文件
     *   3. 校验整体 MD5
     *   4. 上传到 OSS
     *   5. 清理临时文件和 Redis key
     *   6. 返回 URL
     *
     * 响应示例：
     * {
     *   "url":         "https://cdn.example.com/avatar/2024/01/01/xxx.jpg",
     *   "contentType": "image/jpeg",
     *   "fileSize":    5242880
     * }
     */
    @PostMapping("/{taskId}/complete")
    @RateLimit(key = "upload:complete", permitsPerSecond = 5.0)
    public Result<UploadResultVO> complete(@PathVariable String taskId) {
        return Result.ok(uploadService.complete(taskId, UserContext.getRequired()));
    }
}
