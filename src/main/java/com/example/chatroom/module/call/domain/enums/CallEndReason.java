package com.example.chatroom.module.call.domain.enums;

import lombok.Getter;

/**
 * 通话结束原因
 */
@Getter
public enum CallEndReason {

    COMPLETED("正常结束"),
    MISSED("未接来电"),
    REJECTED("已拒绝"),
    CANCELED("已取消"),
    BUSY("忙线"),
    FAILED("建链失败"),
    TIMEOUT("超时未接");

    private final String desc;

    CallEndReason(String desc) {
        this.desc = desc;
    }
}
