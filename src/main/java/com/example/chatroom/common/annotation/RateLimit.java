package com.example.chatroom.common.annotation;

import java.lang.annotation.*;

/**
 * 接口限流注解（Guava RateLimiter，单机令牌桶）
 *
 * 用法示例：
 * <pre>
 * // 全局限流：整个接口每秒最多 10 个请求
 * {@literal @}RateLimit(key = "login", permitsPerSecond = 10.0)
 *
 * // 按 IP 限流：同一 IP 每秒最多 2 个请求（防暴力破解）
 * {@literal @}RateLimit(key = "login", permitsPerSecond = 2.0, limitByIp = true)
 *
 * // 按用户ID限流：同一用户每秒最多 10 条消息
 * {@literal @}RateLimit(key = "sendMessage", permitsPerSecond = 10.0, limitByUserId = true)
 * </pre>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RateLimit {

    /**
     * 限流桶标识，相同 key 共享同一个 RateLimiter。
     * 开启 limitByIp 时，实际 key = key + ":" + clientIp。
     * 开启 limitByUserId 时，实际 key = key + ":u:" + userId。
     */
    String key();

    /**
     * 每秒允许的请求数（令牌生成速率）。
     * 默认 10.0，即每秒最多 10 个请求。
     */
    double permitsPerSecond() default 10.0;

    /**
     * 是否按客户端 IP 单独限流。
     * true：每个 IP 独立一个令牌桶，适合登录/注册等防暴力破解场景。
     * false：所有请求共享同一个令牌桶，适合全局接口保护。
     */
    boolean limitByIp() default false;

    /**
     * 是否按登录用户 ID 单独限流。
     * true：每个用户独立一个令牌桶，适合发消息等需要按用户控频的场景。
     * 优先级高于 limitByIp，两者同时为 true 时以 userId 为准。
     */
    boolean limitByUserId() default false;
}
