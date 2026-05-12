package com.example.chatroom.module.call.domain.enums;

import lombok.Getter;

/**
 * 通话状态机
 *
 * <pre>
 * INIT → INVITING → RINGING → ACCEPTED → CONNECTING → IN_CALL → ENDED
 * </pre>
 */
@Getter
public enum CallStatus {

    INIT("初始化"),
    INVITING("邀请中"),
    RINGING("振铃中"),
    ACCEPTED("已接听"),
    CONNECTING("建链中"),
    IN_CALL("通话中"),
    ENDED("已结束");

    private final String desc;

    CallStatus(String desc) {
        this.desc = desc;
    }

    public boolean isTerminal() {
        return this == ENDED;
    }
}
