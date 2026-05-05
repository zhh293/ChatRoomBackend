package com.example.chatroom.cache.helper;

import org.springframework.stereotype.Component;

import java.util.Random;

/**
 * 缓存 TTL 工具类（防雪崩：随机 TTL 打散过期时间）
 * 所有写缓存的地方统一使用此类获取 TTL，禁止硬编码固定过期时间
 */
@Component
public class CacheTtlHelper {

    private static final Random RANDOM = new Random();

    /** 用户信息缓存：基础 1h + 随机 0~10min */
    public long userInfoTtl() {
        return 3600 + RANDOM.nextInt(600);
    }

    /** 会话信息缓存：基础 5min + 随机 0~1min（会话数量多，TTL 短一点避免内存爆满） */
    public long sessionInfoTtl() {
        return 300 + RANDOM.nextInt(60);
    }

    /** 会话列表缓存：基础 5min + 随机 0~2min */
    public long sessionListTtl() {
        return 300 + RANDOM.nextInt(120);
    }

    /**
     * 群成员列表缓存：基础 7天 + 随机 0~2h 抖动
     * 成员变动（加入/退出/踢人）时主动失效，TTL 只是兜底保障
     */
    public long sessionMembersTtl() {
        return 7 * 24 * 3600 + RANDOM.nextInt(7200);
    }

    /** 通用：在基础 TTL 上叠加最多 20% 的随机偏移 */
    public long withJitter(long baseTtlSeconds) {
        long jitter = (long) (baseTtlSeconds * 0.2 * RANDOM.nextDouble());
        return baseTtlSeconds + jitter;
    }
}
