package com.example.chatroom.common.util;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

/**
 * JWT 工具类
 * 负责 Access Token 的生成、解析、校验
 */
@Component
public class JwtUtil {

    @Value("${jwt.secret}")
    private String secret;

    @Value("${jwt.access-token-expire}")
    private long accessTokenExpire; // 秒

    private SecretKey getKey() {
        return Keys.hmacShaKeyFor(secret.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 生成 Access Token
     */
    public String generateAccessToken(Long userId, String username) {
        String jti = UUID.randomUUID().toString().replace("-", "");
        return Jwts.builder()
                .id(jti)
                .subject(String.valueOf(userId))
                .claim("userId", userId)
                .claim("username", username)
                .issuedAt(new Date())
                .expiration(new Date(System.currentTimeMillis() + accessTokenExpire * 1000))
                .signWith(getKey())
                .compact();
    }

    /**
     * 解析 Token，返回 Claims
     */
    public Claims parseToken(String token) {
        return Jwts.parser()
                .verifyWith(getKey())
                .build()
                .parseSignedClaims(token)
                .getPayload();
    }

    /**
     * 获取 Token 剩余有效期（秒）
     */
    public long getRemainingExpire(Claims claims) {
        long expireMs = claims.getExpiration().getTime();
        long remaining = (expireMs - System.currentTimeMillis()) / 1000;
        return Math.max(remaining, 0);
    }

    /**
     * 获取 jti（Token 唯一标识，用于黑名单）
     */
    public String getJti(Claims claims) {
        return claims.getId();
    }

    /**
     * 获取 userId
     */
    public Long getUserId(Claims claims) {
        return claims.get("userId", Long.class);
    }
}
