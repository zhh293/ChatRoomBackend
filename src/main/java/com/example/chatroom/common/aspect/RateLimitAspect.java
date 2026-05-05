package com.example.chatroom.common.aspect;

import com.example.chatroom.common.annotation.RateLimit;
import com.example.chatroom.common.exception.BizException;
import com.example.chatroom.common.interceptor.UserContext;
import com.example.chatroom.common.response.ResultCode;
import com.google.common.util.concurrent.RateLimiter;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 限流切面（Guava RateLimiter，令牌桶算法）
 *
 * 设计要点：
 * 1. ConcurrentHashMap 管理所有限流桶，key → RateLimiter，懒初始化
 * 2. tryAcquire() 非阻塞，获取不到令牌立即抛 TOO_MANY_REQUESTS，不阻塞线程
 * 3. limitByUserId=true 时，key = annotation.key() + ":u:" + userId，每个用户独立桶
 * 4. limitByIp=true 时，key = annotation.key() + ":" + clientIp，每个 IP 独立桶
 * 5. limitByUserId 优先级高于 limitByIp
 * 6. 单体应用内存级限流，重启后桶状态重置（符合 MVP 阶段需求）
 */
@Slf4j
@Aspect
@Component
public class RateLimitAspect {

    /**
     * 限流桶注册表：bucketKey → RateLimiter
     * ConcurrentHashMap 保证并发安全，computeIfAbsent 保证同一 key 只创建一次
     */
    private final ConcurrentHashMap<String, RateLimiter> rateLimiterMap = new ConcurrentHashMap<>();

    @Around("@annotation(rateLimit)")
    public Object around(ProceedingJoinPoint joinPoint, RateLimit rateLimit) throws Throwable {

        String bucketKey = resolveBucketKey(rateLimit);

        // 懒初始化：同一 key 只创建一个 RateLimiter
        RateLimiter limiter = rateLimiterMap.computeIfAbsent(
                bucketKey,
                k -> {
                    log.info("[RateLimit] 创建限流桶: key={}, permitsPerSecond={}",
                            k, rateLimit.permitsPerSecond());
                    return RateLimiter.create(rateLimit.permitsPerSecond());
                }
        );

        // tryAcquire() 非阻塞：拿不到令牌直接返回 false
        if (!limiter.tryAcquire()) {
            log.warn("[RateLimit] 触发限流: key={}", bucketKey);
            throw new BizException(ResultCode.TOO_MANY_REQUESTS);
        }

        return joinPoint.proceed();
    }

    /**
     * 解析最终的桶 key，优先级：userId > IP > 全局
     */
    private String resolveBucketKey(RateLimit rateLimit) {
        if (rateLimit.limitByUserId()) {
            Long userId = UserContext.get();
            if (userId != null) {
                return rateLimit.key() + ":u:" + userId;
            }
            // 未登录时降级到 IP 限流，避免匿名请求绕过
            return rateLimit.key() + ":" + getClientIp();
        }
        if (rateLimit.limitByIp()) {
            return rateLimit.key() + ":" + getClientIp();
        }
        return rateLimit.key();
    }

    /**
     * 获取客户端真实 IP（兼容反向代理）
     */
    private String getClientIp() {
        try {
            ServletRequestAttributes attrs =
                    (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
            if (attrs == null) return "unknown";
            HttpServletRequest request = attrs.getRequest();

            // 依次尝试常见代理头
            String[] headers = {
                    "X-Forwarded-For", "X-Real-IP", "Proxy-Client-IP",
                    "WL-Proxy-Client-IP", "HTTP_CLIENT_IP", "HTTP_X_FORWARDED_FOR"
            };
            for (String header : headers) {
                String ip = request.getHeader(header);
                if (ip != null && !ip.isEmpty() && !"unknown".equalsIgnoreCase(ip)) {
                    // X-Forwarded-For 可能包含多个 IP，取第一个（最原始客户端）
                    return ip.split(",")[0].trim();
                }
            }
            return request.getRemoteAddr();
        } catch (Exception e) {
            return "unknown";
        }
    }
}
