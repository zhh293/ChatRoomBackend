package com.example.chatroom.common.interceptor;

/**
 * 用户上下文（ThreadLocal）
 * 在 JwtAuthFilter 中写入，在 Controller/Service 中读取
 */
public class UserContext {

    private static final ThreadLocal<Long> USER_ID_HOLDER = new ThreadLocal<>();

    public static void set(Long userId) {
        USER_ID_HOLDER.set(userId);
    }

    public static Long get() {
        return USER_ID_HOLDER.get();
    }

    public static Long getRequired() {
        Long userId = USER_ID_HOLDER.get();
        if (userId == null) {
            throw new IllegalStateException("当前请求未登录");
        }
        return userId;
    }

    public static void clear() {
        USER_ID_HOLDER.remove();
    }
}
