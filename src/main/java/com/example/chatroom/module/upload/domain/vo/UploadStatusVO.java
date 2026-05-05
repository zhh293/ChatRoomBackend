package com.example.chatroom.module.upload.domain.vo;

import lombok.Data;

import java.util.List;

/**
 * 分片上传状态查询响应 VO
 *
 * 断点续传时，前端调用 GET /api/v1/upload/{taskId}/status，
 * 根据 missingChunks 列表只重传缺失的分片，无需全量重传。
 */
@Data
public class UploadStatusVO {

    /** 上传任务 ID */
    private String taskId;

    /** 分片总数 */
    private int totalChunks;

    /** 已成功上传的分片数量 */
    private int uploadedCount;

    /**
     * 缺失的分片序号列表（0-based）
     * 前端遍历此列表，只重传这些分片
     */
    private List<Integer> missingChunks;

    /** 是否全部分片已上传（true 时可调用合并接口） */
    private boolean complete;
}
