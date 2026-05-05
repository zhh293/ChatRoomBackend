package com.example.chatroom.common.oss;

import java.io.InputStream;

/**
 * OSS 客户端抽象接口
 *
 * 当前为占位实现，后续接入阿里云 OSS / 腾讯云 COS / MinIO 时，
 * 只需提供对应的实现类并注入 Spring 容器，上传服务无需改动。
 *
 * 实现示例：
 *   - MinioOssClient   → 使用 io.minio:minio SDK
 *   - AliyunOssClient  → 使用 com.aliyun.oss:aliyun-sdk-oss SDK
 */
public interface OssClient {

    /**
     * 上传文件到 OSS
     *
     * @param objectKey   对象存储路径（如 avatar/2024/01/abc.jpg）
     * @param inputStream 文件输入流
     * @param contentType MIME 类型
     * @param size        文件字节数
     * @return 可访问的公网 URL
     */
    String upload(String objectKey, InputStream inputStream, String contentType, long size);
}
