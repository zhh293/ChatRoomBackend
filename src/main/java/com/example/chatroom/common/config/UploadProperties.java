package com.example.chatroom.common.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

/**
 * 分片上传配置属性
 * 对应 application.yml 中的 upload.* 节点
 */
@Data
@Component
@ConfigurationProperties(prefix = "upload")
public class UploadProperties {

    /** 临时分片存储目录 */
    private String tmpDir = "/tmp/chatroom-upload";

    /** 单个文件最大字节数（默认 10MB） */
    private long maxFileSize = 10 * 1024 * 1024L;

    /** 单个分片最大字节数（默认 2MB） */
    private long maxChunkSize = 2 * 1024 * 1024L;

    /** 上传任务过期时间（秒，默认 24h） */
    private long taskExpire = 86400L;

    /**
     * 允许上传的 MIME 类型白名单（逗号分隔字符串，由 Spring 自动绑定为 List）
     * 默认仅允许常见图片格式
     */
    private List<String> allowedTypes = Arrays.asList(
            "image/jpeg", "image/png", "image/gif", "image/webp"
    );

    /** OSS 相关配置 */
    private Oss oss = new Oss();

    @Data
    public static class Oss {
        private String endpoint = "http://localhost:9000";
        private String bucket = "chatroom";
        private String accessKey = "minioadmin";
        private String secretKey = "minioadmin";
        /** 返回给前端的 URL 前缀（CDN 域名或 OSS 公网地址） */
        private String urlPrefix = "http://localhost:9000/chatroom";
    }
}
