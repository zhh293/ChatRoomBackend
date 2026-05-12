package com.example.chatroom.module.call.domain.enums;

import lombok.Getter;

/**
 * 通话事件（驱动状态机流转的动作）
 */
@Getter
public enum CallEvent {

    // 正向流程
    INVITE("发起邀请"),
    RING("被叫振铃"),
    ACCEPT("被叫接听"),
    CONNECT("WebRTC建链完成"),
    CALL_START("通话开始"),

    // 终止流程
    CANCEL("主叫取消"),
    REJECT("被叫拒绝"),
    TIMEOUT("超时未接"),
    BUSY("对方忙线"),
    HANG_UP("挂断"),
    ERROR("异常断开");

    private final String desc;

    CallEvent(String desc) {
        this.desc = desc;
    }
}
