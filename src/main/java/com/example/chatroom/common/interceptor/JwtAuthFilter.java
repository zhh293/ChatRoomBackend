package com.example.chatroom.common.interceptor;

import com.example.chatroom.common.response.Result;
import com.example.chatroom.common.response.ResultCode;
import com.example.chatroom.common.util.JwtUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.util.AntPathMatcher;
import org.springframework.util.StringUtils;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;

/**
 * JWT 认证过滤器
 *
 * 流程：
 * 1. 白名单路径直接放行（登录、注册、刷新 Token、WebSocket 握手）
 * 2. 非白名单路径：提取 Bearer Token → 验签 → 查黑名单 → 注入 UserContext
 * 3. 非白名单路径且 UserContext 为空 → 直接返回 401 JSON，不继续传递
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class JwtAuthFilter extends OncePerRequestFilter {

    private final JwtUtil jwtUtil;
    private final RedisTemplate<String, Object> redisTemplate;
    private final ObjectMapper objectMapper;

    private static final String BLACKLIST_KEY_PREFIX = "token:blacklist:";
    private static final AntPathMatcher PATH_MATCHER = new AntPathMatcher();

    /**
     * 白名单：这些路径无需携带 Token，直接放行。
     * 使用 Ant 风格通配符。
     */
    private static final Set<String> WHITE_LIST = Set.of(
            "/api/v1/auth/login",
            "/api/v1/auth/register",
            "/api/v1/auth/refresh",
            "/api/v1/users/register",  // 用户注册（无需 Token）
            "/ws/**"                   // WebSocket 握手（Token 通过 query param 传递，由 WS Handler 校验）
    );

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {

        String path = request.getRequestURI();

        // ── 1. 白名单直接放行 ──────────────────────────────────────────────
        if (isWhitelisted(path)) {
            filterChain.doFilter(request, response);
            return;
        }

        // ── 2. 提取并校验 Token ───────────────────────────────────────────
        String token = extractToken(request);
        if (StringUtils.hasText(token)) {
            try {
                Claims claims = jwtUtil.parseToken(token);
                String jti = jwtUtil.getJti(claims);

                // 检查黑名单（登出后的 Token）
                Boolean inBlacklist = redisTemplate.hasKey(BLACKLIST_KEY_PREFIX + jti);
                if (Boolean.TRUE.equals(inBlacklist)) {
                    log.warn("[JwtFilter] Token 已在黑名单中, jti={}, path={}", jti, path);
                    // 不注入 UserContext，后续会被 401 拦截
                } else {
                    Long userId = jwtUtil.getUserId(claims);
                    UserContext.set(userId);
                }
            } catch (JwtException e) {
                log.debug("[JwtFilter] Token 无效: {}, path={}", e.getMessage(), path);
                // 不注入 UserContext
            }
        }

        // ── 3. 非白名单路径：UserContext 为空则返回 401 ───────────────────
        if (UserContext.get() == null) {
            writeUnauthorized(response);
            return;
        }

        // ── 4. 放行，finally 清理 ThreadLocal ────────────────────────────
        try {
            filterChain.doFilter(request, response);
        } finally {
            UserContext.clear();
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 私有工具方法
    // ─────────────────────────────────────────────────────────────────────────

    private boolean isWhitelisted(String path) {
        return WHITE_LIST.stream().anyMatch(pattern -> PATH_MATCHER.match(pattern, path));
    }

    private String extractToken(HttpServletRequest request) {
        String header = request.getHeader("Authorization");
        if (StringUtils.hasText(header) && header.startsWith("Bearer ")) {
            return header.substring(7);
        }
        return null;
    }

    /**
     * 直接写 401 JSON 响应，与 GlobalExceptionHandler 格式保持一致
     */
    private void writeUnauthorized(HttpServletResponse response) throws IOException {
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());

        Result<Void> body = Result.fail(ResultCode.UNAUTHORIZED);
        response.getWriter().write(objectMapper.writeValueAsString(body));
    }
}
