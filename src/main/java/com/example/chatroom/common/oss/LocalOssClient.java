package com.example.chatroom.common.oss;

import com.example.chatroom.common.config.UploadProperties;
import com.example.chatroom.common.exception.BizException;
import com.example.chatroom.common.response.ResultCode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * 本地文件系统 OSS 占位实现
 *
 * 仅用于开发/测试环境，将文件保存到本地磁盘并返回相对 URL。
 * 生产环境请提供真实的 OssClient 实现（MinIO / 阿里云 / 腾讯云），
 * 注入后本类会被 @ConditionalOnMissingBean 自动跳过。
 *
 * 注意：本实现不支持集群部署，多节点时文件只存在于单台机器上。
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnMissingBean(value = OssClient.class, ignored = LocalOssClient.class)
public class LocalOssClient implements OssClient {

    private final UploadProperties uploadProperties;

    @Override
    public String upload(String objectKey, InputStream inputStream, String contentType, long size) {
        try {
            // 存储到 tmpDir 的上级目录（与分片临时目录隔离）
            Path storageRoot = Paths.get(uploadProperties.getTmpDir()).getParent()
                    .resolve("chatroom-storage");
            Path target = storageRoot.resolve(objectKey);
            Files.createDirectories(target.getParent());
            Files.copy(inputStream, target, StandardCopyOption.REPLACE_EXISTING);

            String url = uploadProperties.getOss().getUrlPrefix() + "/" + objectKey;
            log.info("[LocalOss] 文件已保存到本地: path={}, url={}", target, url);
            return url;
        } catch (IOException e) {
            log.error("[LocalOss] 文件保存失败: objectKey={}", objectKey, e);
            throw new BizException(ResultCode.UPLOAD_OSS_FAILED);
        }
    }
}
