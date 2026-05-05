package com.example.chatroom.common.exception;

import com.example.chatroom.common.response.ResultCode;
import lombok.Getter;

/**
 * 业务异常（可预期的业务错误，不打印堆栈）
 */
@Getter
public class BizException extends RuntimeException {

    private final int code;

    public BizException(ResultCode resultCode) {
        super(resultCode.getMsg());
        this.code = resultCode.getCode();
    }

    public BizException(int code, String message) {
        super(message);
        this.code = code;
    }
}
