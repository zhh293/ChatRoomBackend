package com.example.chatroom.module.upload.service;

import com.example.chatroom.module.upload.domain.dto.InitUploadDTO;
import com.example.chatroom.module.upload.domain.vo.InitUploadVO;
import com.example.chatroom.module.upload.domain.vo.UploadResultVO;
import com.example.chatroom.module.upload.domain.vo.UploadStatusVO;
import org.springframework.web.multipart.MultipartFile;

/**
 * 分片上传服务接口
 *
 * 完整流程：
 *   1. initUpload()    → 创建任务，返回 taskId
 *   2. uploadChunk()   × N（并发/顺序均可）
 *   3. getStatus()     → 断点续传时查询缺失分片（可选）
 *   4. complete()      → 合并分片、校验 MD5、上传 OSS，返回 URL
 */
public interface UploadService {

    /**
     * 初始化上传任务
     * 校验文件类型、大小，创建 Redis Hash（元信息）+ Bitmap（分片状态），返回 taskId
     *
     * @param dto      初始化参数
     * @param userId   当前登录用户 ID（任务归属，防止越权操作）
     */
    InitUploadVO initUpload(InitUploadDTO dto, Long userId);

    /**
     * 上传单个分片
     *
     * WAL 原则：先将分片写入临时目录，再更新 bitmap。
     * 幂等：若 bitmap 对应位已为 1，说明该分片已成功落盘，直接返回成功，不重复写文件。
     *
     * @param taskId     任务 ID
     * @param chunkIndex 分片序号（0-based）
     * @param chunkMd5   分片内容的 MD5（小写十六进制 32 位），用于校验传输完整性
     * @param file       分片文件内容
     * @param userId     当前登录用户 ID（校验任务归属）
     */
    void uploadChunk(String taskId, int chunkIndex, String chunkMd5,
                     MultipartFile file, Long userId);

    /**
     * 查询分片上传状态（断点续传入口）
     * 返回已上传分片数量和缺失分片列表，前端据此只重传缺失部分
     *
     * @param taskId 任务 ID
     * @param userId 当前登录用户 ID（校验任务归属）
     */
    UploadStatusVO getStatus(String taskId, Long userId);

    /**
     * 合并分片并上传 OSS
     *
     * 步骤：
     *   1. 校验所有分片已到齐（bitmap 全为 1）
     *   2. 按序号顺序用缓冲流合并临时分片文件
     *   3. 校验合并后文件的 MD5 与初始化时提交的 fileMd5 一致
     *   4. 上传到 OSS，获取 URL
     *   5. 清理临时分片目录和 Redis key
     *
     * @param taskId 任务 ID
     * @param userId 当前登录用户 ID（校验任务归属）
     */
    UploadResultVO complete(String taskId, Long userId);
}
