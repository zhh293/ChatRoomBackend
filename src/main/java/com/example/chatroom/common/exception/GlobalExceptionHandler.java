package com.example.chatroom.common.exception;

import com.example.chatroom.common.response.Result;
import com.example.chatroom.common.response.ResultCode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.validation.BindException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * 全局异常处理器
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    /** 业务异常：可预期，不打印堆栈 */
    @ExceptionHandler(BizException.class)
    public Result<Void> handleBizException(BizException e) {
        return Result.fail(e.getCode(), e.getMessage());
    }

    /** 参数校验异常（@Valid） */
    @ExceptionHandler({MethodArgumentNotValidException.class, BindException.class})
    public Result<Void> handleValidException(Exception e) {
        String msg = "参数校验失败";
        if (e instanceof MethodArgumentNotValidException ex) {
            var fieldError = ex.getBindingResult().getFieldError();
            if (fieldError != null) {
                msg = fieldError.getField() + ": " + fieldError.getDefaultMessage();
            }
        }
        return Result.fail(ResultCode.PARAM_ERROR.getCode(), msg);
    }

    /** 兜底异常：未预期错误，打印完整堆栈 */
    @ExceptionHandler(Exception.class)
    public Result<Void> handleException(Exception e) {
        log.error("[GlobalException] 未预期异常", e);
        return Result.fail(ResultCode.SERVER_ERROR);
    }
}
