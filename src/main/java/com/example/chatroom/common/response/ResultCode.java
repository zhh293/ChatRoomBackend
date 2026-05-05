package com.example.chatroom.common.response;

import lombok.Getter;

/**
 * 统一错误码枚举
 */
@Getter
public enum ResultCode {

    // 通用
    SUCCESS(0, "success"),
    PARAM_ERROR(10001, "参数校验失败"),
    UNAUTHORIZED(10002, "未登录或Token无效"),
    TOKEN_EXPIRED(10003, "Token已过期"),
    FORBIDDEN(10004, "无权限"),
    NOT_FOUND(10005, "资源不存在"),
    CONFLICT(10006, "资源已存在"),
    TOO_MANY_REQUESTS(10007, "请求过于频繁"),
    SERVER_ERROR(50000, "服务器内部错误"),

    // 用户模块
    USER_NOT_FOUND(20001, "用户不存在"),
    PASSWORD_ERROR(20002, "密码错误"),
    USER_DISABLED(20003, "账号已被禁用"),
    USERNAME_DUPLICATE(20004, "用户名已被占用"),
    PHONE_DUPLICATE(20005, "手机号已被注册"),
    EMAIL_DUPLICATE(20006, "邮箱已被注册"),
    ALREADY_FRIEND(20007, "已经是好友关系"),
    NOT_FRIEND(20008, "对方不在好友列表中"),
    CANNOT_ADD_SELF(20009, "不能添加自己为好友"),

    // 会话模块
    SESSION_NOT_FOUND(30001, "会话不存在"),
    NOT_IN_SESSION(30002, "不在该会话中"),
    NO_ADMIN_PERMISSION(30003, "无管理员权限"),
    GROUP_MEMBER_LIMIT_EXCEEDED(30004, "群聊人数已达上限"),

    // 消息模块
    MSG_NOT_FOUND(40001, "消息不存在"),
    MSG_REVOKE_TIMEOUT(40002, "消息撤回超时（超过2分钟）"),
    MSG_DUPLICATE(40003, "消息重复（幂等拦截）"),
    MSG_TYPE_INVALID(40004, "消息类型不合法"),
    ILLEGAL_SESSION(40005, "非法会话，无权发送消息"),

    // 上传模块
    UPLOAD_TASK_NOT_FOUND(60001, "上传任务不存在或已过期"),
    UPLOAD_CHUNK_MD5_MISMATCH(60002, "分片 MD5 校验失败，数据已损坏"),
    UPLOAD_FILE_MD5_MISMATCH(60003, "文件 MD5 校验失败，合并结果与预期不符"),
    UPLOAD_FILE_TYPE_NOT_ALLOWED(60004, "不支持的文件类型"),
    UPLOAD_FILE_TOO_LARGE(60005, "文件超过大小限制"),
    UPLOAD_CHUNK_INDEX_OUT_OF_RANGE(60006, "分片序号越界"),
    UPLOAD_NOT_COMPLETE(60007, "尚有分片未上传，无法合并"),
    UPLOAD_OSS_FAILED(60008, "文件上传云端失败");

    private final int code;
    private final String msg;

    ResultCode(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }
}
