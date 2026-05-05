package com.example.chatroom.module.upload.service.impl;

import com.example.chatroom.common.config.UploadProperties;
import com.example.chatroom.common.constant.RedisKeyConst;
import com.example.chatroom.common.exception.BizException;
import com.example.chatroom.common.oss.OssClient;
import com.example.chatroom.common.response.ResultCode;
import com.example.chatroom.module.upload.domain.dto.InitUploadDTO;
import com.example.chatroom.module.upload.domain.vo.InitUploadVO;
import com.example.chatroom.module.upload.domain.vo.UploadResultVO;
import com.example.chatroom.module.upload.domain.vo.UploadStatusVO;
import com.example.chatroom.module.upload.service.UploadService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.nio.file.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * 分片上传服务实现
 *
 * Redis Key 说明：
 *   upload:task:{taskId}    → Hash，存任务元信息（fileMd5/totalChunks/fileSize/contentType/fileName/userId）
 *   upload:bitmap:{taskId}  → String（当 bitmap 用），bit N=1 表示第 N 片已落盘
 *
 * 临时目录结构：
 *   {tmpDir}/{taskId}/chunk_{index}   → 单个分片文件
 *
 * 安全设计：
 *   - 每个接口都校验 userId 与任务归属，防止越权操作他人任务
 *   - 分片 MD5 校验防传输损坏
 *   - 合并后整体 MD5 校验防拼接错误
 *   - 文件类型白名单 + 文件头魔数双重校验
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class UploadServiceImpl implements UploadService {

    // Redis Hash field 名称常量
    private static final String FIELD_FILE_MD5      = "fileMd5";
    private static final String FIELD_TOTAL_CHUNKS  = "totalChunks";
    private static final String FIELD_FILE_SIZE     = "fileSize";
    private static final String FIELD_CONTENT_TYPE  = "contentType";
    private static final String FIELD_FILE_NAME     = "fileName";
    private static final String FIELD_USER_ID       = "userId";

    // 各类型文件的魔数（magic bytes）前缀，用于二次校验
    private static final Map<String, byte[]> MAGIC_BYTES = Map.of(
            "image/jpeg", new byte[]{(byte) 0xFF, (byte) 0xD8, (byte) 0xFF},
            "image/png",  new byte[]{(byte) 0x89, 0x50, 0x4E, 0x47},
            "image/gif",  new byte[]{0x47, 0x49, 0x46, 0x38},
            "image/webp", new byte[]{0x52, 0x49, 0x46, 0x46}  // RIFF header
    );

    private static final int BUFFER_SIZE = 8 * 1024; // 8KB 缓冲流

    private final RedisTemplate<String, Object> redisTemplate;
    private final UploadProperties uploadProperties;
    private final OssClient ossClient;

    // ─────────────────────────────────────────────────────────────────────────
    // 1. 初始化上传任务
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public InitUploadVO initUpload(InitUploadDTO dto, Long userId) {

        // ── 文件类型白名单校验 ────────────────────────────────────────────
        if (!uploadProperties.getAllowedTypes().contains(dto.getContentType())) {
            throw new BizException(ResultCode.UPLOAD_FILE_TYPE_NOT_ALLOWED);
        }

        // ── 文件大小校验 ──────────────────────────────────────────────────
        if (dto.getFileSize() > uploadProperties.getMaxFileSize()) {
            throw new BizException(ResultCode.UPLOAD_FILE_TOO_LARGE);
        }

        // ── 生成任务 ID ───────────────────────────────────────────────────
        String taskId = UUID.randomUUID().toString().replace("-", "");
        String taskKey   = RedisKeyConst.UPLOAD_TASK   + taskId;
        String bitmapKey = RedisKeyConst.UPLOAD_BITMAP + taskId;
        long   expire    = uploadProperties.getTaskExpire();

        // ── 写 Redis Hash（元信息）────────────────────────────────────────
        Map<String, Object> taskMeta = new HashMap<>();
        taskMeta.put(FIELD_FILE_MD5,     dto.getFileMd5());
        taskMeta.put(FIELD_TOTAL_CHUNKS, String.valueOf(dto.getTotalChunks()));
        taskMeta.put(FIELD_FILE_SIZE,    String.valueOf(dto.getFileSize()));
        taskMeta.put(FIELD_CONTENT_TYPE, dto.getContentType());
        taskMeta.put(FIELD_FILE_NAME,    dto.getFileName());
        taskMeta.put(FIELD_USER_ID,      String.valueOf(userId));
        redisTemplate.opsForHash().putAll(taskKey, taskMeta);
        redisTemplate.expire(taskKey, expire, TimeUnit.SECONDS);

        // ── 初始化 bitmap（全 0，所有分片未到达）─────────────────────────
        // 只需设置 TTL，bitmap 默认全 0，无需显式初始化每一位
        redisTemplate.opsForValue().set(bitmapKey, "", expire, TimeUnit.SECONDS);

        // ── 创建临时目录 ──────────────────────────────────────────────────
        Path taskDir = Paths.get(uploadProperties.getTmpDir(), taskId);
        try {
            Files.createDirectories(taskDir);
        } catch (IOException e) {
            log.error("[Upload] 创建临时目录失败: taskId={}", taskId, e);
            throw new BizException(ResultCode.SERVER_ERROR);
        }

        log.info("[Upload] 任务初始化: taskId={}, userId={}, totalChunks={}, fileSize={}",
                taskId, userId, dto.getTotalChunks(), dto.getFileSize());

        InitUploadVO vo = new InitUploadVO();
        vo.setTaskId(taskId);
        vo.setExpireAt(Instant.now().getEpochSecond() + expire);
        return vo;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 2. 上传单个分片
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public void uploadChunk(String taskId, int chunkIndex, String chunkMd5,
                            MultipartFile file, Long userId) {

        // ── 加载并校验任务元信息 ──────────────────────────────────────────
        Map<String, Object> meta = loadAndValidateMeta(taskId, userId);
        int totalChunks = Integer.parseInt((String) meta.get(FIELD_TOTAL_CHUNKS));

        // ── 分片序号越界校验 ──────────────────────────────────────────────
        if (chunkIndex < 0 || chunkIndex >= totalChunks) {
            throw new BizException(ResultCode.UPLOAD_CHUNK_INDEX_OUT_OF_RANGE);
        }

        // ── 分片大小校验 ──────────────────────────────────────────────────
        if (file.getSize() > uploadProperties.getMaxChunkSize()) {
            throw new BizException(ResultCode.UPLOAD_FILE_TOO_LARGE);
        }

        // ── 幂等检查：bitmap 对应位已为 1，说明该分片已成功落盘 ───────────
        String bitmapKey = RedisKeyConst.UPLOAD_BITMAP + taskId;
        Boolean alreadyUploaded = redisTemplate.opsForValue().getBit(bitmapKey, chunkIndex);
        if (Boolean.TRUE.equals(alreadyUploaded)) {
            log.debug("[Upload] 分片已存在，跳过（幂等）: taskId={}, chunkIndex={}", taskId, chunkIndex);
            return;
        }

        // ── 读取分片字节 ──────────────────────────────────────────────────
        byte[] chunkBytes;
        try {
            chunkBytes = file.getBytes();
        } catch (IOException e) {
            log.error("[Upload] 读取分片内容失败: taskId={}, chunkIndex={}", taskId, chunkIndex, e);
            throw new BizException(ResultCode.SERVER_ERROR);
        }

        // ── 校验分片 MD5（防传输损坏）────────────────────────────────────
        String actualMd5 = md5Hex(chunkBytes);
        if (!actualMd5.equalsIgnoreCase(chunkMd5)) {
            log.warn("[Upload] 分片 MD5 不匹配: taskId={}, chunkIndex={}, expected={}, actual={}",
                    taskId, chunkIndex, chunkMd5, actualMd5);
            throw new BizException(ResultCode.UPLOAD_CHUNK_MD5_MISMATCH);
        }

        // ── WAL：先落盘分片文件，再更新 bitmap ───────────────────────────
        Path chunkFile = Paths.get(uploadProperties.getTmpDir(), taskId,
                "chunk_" + chunkIndex);
        try {
            Files.write(chunkFile, chunkBytes, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            log.error("[Upload] 分片写入磁盘失败: taskId={}, chunkIndex={}", taskId, chunkIndex, e);
            throw new BizException(ResultCode.SERVER_ERROR);
        }

        // ── 更新 bitmap ───────────────────────────────────────────────────
        redisTemplate.opsForValue().setBit(bitmapKey, chunkIndex, true);

        log.debug("[Upload] 分片上传成功: taskId={}, chunkIndex={}/{}", taskId, chunkIndex, totalChunks - 1);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 3. 查询分片上传状态（断点续传）
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public UploadStatusVO getStatus(String taskId, Long userId) {

        Map<String, Object> meta = loadAndValidateMeta(taskId, userId);
        int totalChunks = Integer.parseInt((String) meta.get(FIELD_TOTAL_CHUNKS));
        String bitmapKey = RedisKeyConst.UPLOAD_BITMAP + taskId;

        List<Integer> missing = new ArrayList<>();
        int uploadedCount = 0;

        for (int i = 0; i < totalChunks; i++) {
            Boolean bit = redisTemplate.opsForValue().getBit(bitmapKey, i);
            if (Boolean.TRUE.equals(bit)) {
                uploadedCount++;
            } else {
                missing.add(i);
            }
        }

        UploadStatusVO vo = new UploadStatusVO();
        vo.setTaskId(taskId);
        vo.setTotalChunks(totalChunks);
        vo.setUploadedCount(uploadedCount);
        vo.setMissingChunks(missing);
        vo.setComplete(missing.isEmpty());
        return vo;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 4. 合并分片并上传 OSS
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public UploadResultVO complete(String taskId, Long userId) {

        Map<String, Object> meta = loadAndValidateMeta(taskId, userId);
        int    totalChunks  = Integer.parseInt((String) meta.get(FIELD_TOTAL_CHUNKS));
        long   fileSize     = Long.parseLong((String) meta.get(FIELD_FILE_SIZE));
        String fileMd5      = (String) meta.get(FIELD_FILE_MD5);
        String contentType  = (String) meta.get(FIELD_CONTENT_TYPE);
        String fileName     = (String) meta.get(FIELD_FILE_NAME);
        String bitmapKey    = RedisKeyConst.UPLOAD_BITMAP + taskId;

        // ── 校验所有分片已到齐 ────────────────────────────────────────────
        for (int i = 0; i < totalChunks; i++) {
            Boolean bit = redisTemplate.opsForValue().getBit(bitmapKey, i);
            if (!Boolean.TRUE.equals(bit)) {
                log.warn("[Upload] 合并失败，分片未到齐: taskId={}, missingChunk={}", taskId, i);
                throw new BizException(ResultCode.UPLOAD_NOT_COMPLETE);
            }
        }

        // ── 合并分片到临时文件 ────────────────────────────────────────────
        Path taskDir    = Paths.get(uploadProperties.getTmpDir(), taskId);
        Path mergedFile = taskDir.resolve("merged");

        try (BufferedOutputStream bos = new BufferedOutputStream(
                new FileOutputStream(mergedFile.toFile()), BUFFER_SIZE)) {

            for (int i = 0; i < totalChunks; i++) {
                Path chunkFile = taskDir.resolve("chunk_" + i);
                if (!Files.exists(chunkFile)) {
                    log.error("[Upload] 合并时分片文件缺失: taskId={}, chunkIndex={}", taskId, i);
                    throw new BizException(ResultCode.UPLOAD_NOT_COMPLETE);
                }
                try (BufferedInputStream bis = new BufferedInputStream(
                        new FileInputStream(chunkFile.toFile()), BUFFER_SIZE)) {
                    byte[] buf = new byte[BUFFER_SIZE];
                    int len;
                    while ((len = bis.read(buf)) != -1) {
                        bos.write(buf, 0, len);
                    }
                }
            }
        } catch (IOException e) {
            log.error("[Upload] 分片合并 IO 失败: taskId={}", taskId, e);
            cleanupTask(taskId);
            throw new BizException(ResultCode.SERVER_ERROR);
        }

        // ── 校验合并后文件的 MD5 ──────────────────────────────────────────
        String actualFileMd5;
        try {
            actualFileMd5 = md5HexFile(mergedFile);
        } catch (IOException e) {
            log.error("[Upload] 计算合并文件 MD5 失败: taskId={}", taskId, e);
            cleanupTask(taskId);
            throw new BizException(ResultCode.SERVER_ERROR);
        }
        if (!actualFileMd5.equalsIgnoreCase(fileMd5)) {
            log.warn("[Upload] 文件 MD5 不匹配: taskId={}, expected={}, actual={}",
                    taskId, fileMd5, actualFileMd5);
            cleanupTask(taskId);
            throw new BizException(ResultCode.UPLOAD_FILE_MD5_MISMATCH);
        }

        // ── 文件头魔数二次校验（防止伪造 contentType）────────────────────
        validateMagicBytes(mergedFile, contentType, taskId);

        // ── 生成 OSS objectKey（按日期分目录，避免单目录文件过多）─────────
        String ext = extractExtension(fileName);
        String objectKey = String.format("avatar/%s/%s%s",
                java.time.LocalDate.now().toString().replace("-", "/"),
                taskId, ext);

        // ── 上传到 OSS ────────────────────────────────────────────────────
        String url;
        try (InputStream is = new BufferedInputStream(
                new FileInputStream(mergedFile.toFile()), BUFFER_SIZE)) {
            url = ossClient.upload(objectKey, is, contentType, fileSize);
        } catch (BizException e) {
            cleanupTask(taskId);
            throw e;
        } catch (IOException e) {
            log.error("[Upload] 读取合并文件失败: taskId={}", taskId, e);
            cleanupTask(taskId);
            throw new BizException(ResultCode.SERVER_ERROR);
        }

        log.info("[Upload] 上传完成: taskId={}, userId={}, url={}", taskId, userId, url);

        // ── 清理临时目录和 Redis key ──────────────────────────────────────
        cleanupTask(taskId);

        UploadResultVO vo = new UploadResultVO();
        vo.setUrl(url);
        vo.setContentType(contentType);
        vo.setFileSize(fileSize);
        return vo;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 私有工具方法
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * 加载任务元信息并校验：任务存在 + 归属当前用户
     */
    private Map<String, Object> loadAndValidateMeta(String taskId, Long userId) {
        String taskKey = RedisKeyConst.UPLOAD_TASK + taskId;
        Map<Object, Object> raw = redisTemplate.opsForHash().entries(taskKey);
        if (raw == null || raw.isEmpty()) {
            throw new BizException(ResultCode.UPLOAD_TASK_NOT_FOUND);
        }
        // 转换为 Map<String, Object>
        Map<String, Object> meta = new HashMap<>();
        raw.forEach((k, v) -> meta.put(String.valueOf(k), v));

        // 归属校验：防止用户操作他人任务
        String ownerUserId = (String) meta.get(FIELD_USER_ID);
        if (!String.valueOf(userId).equals(ownerUserId)) {
            throw new BizException(ResultCode.FORBIDDEN);
        }
        return meta;
    }

    /**
     * 清理任务：删除临时目录（含所有分片和合并文件）+ 删除 Redis key
     */
    private void cleanupTask(String taskId) {
        // 删除临时目录
        Path taskDir = Paths.get(uploadProperties.getTmpDir(), taskId);
        try {
            deleteDirectoryRecursively(taskDir);
        } catch (IOException e) {
            // 清理失败不影响主流程，记录警告即可
            log.warn("[Upload] 清理临时目录失败: taskId={}, path={}", taskId, taskDir, e);
        }
        // 删除 Redis key（pipeline 一次往返）
        final String taskKey   = RedisKeyConst.UPLOAD_TASK   + taskId;
        final String bitmapKey = RedisKeyConst.UPLOAD_BITMAP + taskId;
        redisTemplate.executePipelined((org.springframework.data.redis.core.RedisCallback<Object>) conn -> {
            conn.del(redisTemplate.getStringSerializer().serialize(taskKey));
            conn.del(redisTemplate.getStringSerializer().serialize(bitmapKey));
            return null;
        });
        log.debug("[Upload] 任务清理完成: taskId={}", taskId);
    }

    /**
     * 递归删除目录（含所有子文件）
     */
    private void deleteDirectoryRecursively(Path dir) throws IOException {
        if (!Files.exists(dir)) return;
        try (var stream = Files.walk(dir)) {
            stream.sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException e) {
                            log.warn("[Upload] 删除文件失败: {}", path, e);
                        }
                    });
        }
    }

    /**
     * 校验文件头魔数，防止伪造 contentType
     * 仅校验已知类型，未知类型跳过（降级处理）
     */
    private void validateMagicBytes(Path file, String contentType, String taskId) {
        byte[] expected = MAGIC_BYTES.get(contentType);
        if (expected == null) return; // 未知类型，跳过魔数校验

        try (InputStream is = new FileInputStream(file.toFile())) {
            byte[] header = new byte[expected.length];
            int read = is.read(header);
            if (read < expected.length) {
                throw new BizException(ResultCode.UPLOAD_FILE_TYPE_NOT_ALLOWED);
            }
            for (int i = 0; i < expected.length; i++) {
                if (header[i] != expected[i]) {
                    log.warn("[Upload] 文件头魔数不匹配: taskId={}, contentType={}", taskId, contentType);
                    throw new BizException(ResultCode.UPLOAD_FILE_TYPE_NOT_ALLOWED);
                }
            }
        } catch (BizException e) {
            cleanupTask(taskId);
            throw e;
        } catch (IOException e) {
            log.error("[Upload] 读取文件头失败: taskId={}", taskId, e);
            cleanupTask(taskId);
            throw new BizException(ResultCode.SERVER_ERROR);
        }
    }

    /**
     * 计算字节数组的 MD5（小写十六进制）
     */
    private static String md5Hex(byte[] data) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] hash = md.digest(data);
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 not available", e);
        }
    }

    /**
     * 流式计算文件 MD5（避免大文件 OOM）
     */
    private static String md5HexFile(Path file) throws IOException {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            try (InputStream is = new BufferedInputStream(
                    new FileInputStream(file.toFile()), BUFFER_SIZE)) {
                byte[] buf = new byte[BUFFER_SIZE];
                int len;
                while ((len = is.read(buf)) != -1) {
                    md.update(buf, 0, len);
                }
            }
            return HexFormat.of().formatHex(md.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 not available", e);
        }
    }

    /**
     * 从文件名提取扩展名（含点，如 .jpg），无扩展名返回空字符串
     */
    private static String extractExtension(String fileName) {
        if (fileName == null) return "";
        int dot = fileName.lastIndexOf('.');
        return (dot >= 0 && dot < fileName.length() - 1)
                ? fileName.substring(dot).toLowerCase()
                : "";
    }
}
