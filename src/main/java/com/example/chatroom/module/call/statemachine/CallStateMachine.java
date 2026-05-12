package com.example.chatroom.module.call.statemachine;

import com.example.chatroom.module.call.domain.enums.CallEvent;
import com.example.chatroom.module.call.domain.enums.CallStatus;

import java.util.EnumMap;
import java.util.Map;

/**
 * 语音通话状态机（表驱动）
 *
 * <p>内部用 {@code Map<CallStatus, Map<CallEvent, CallStatus>>} 定义状态转换表：
 * <pre>
 *   当前状态 + 触发事件 → 下一个状态
 * </pre>
 *
 * <h3>状态流转图</h3>
 * <pre>
 * INVITING ──RING──→ RINGING ──ACCEPT──→ ACCEPTED ──CONNECT──→ CONNECTING ──CALL_START──→ IN_CALL ──HANG_UP──→ ENDED
 *    │                  │                                                                   │
 *    ├──CANCEL──→ ENDED │──REJECT──→ ENDED                                                  ├──ERROR──→ ENDED
 *    ├──TIMEOUT─→ ENDED │──TIMEOUT─→ ENDED
 *    └──BUSY────→ ENDED
 * </pre>
 */
public class CallStateMachine {

    /**
     * 状态转换表：transitions.get(当前状态).get(事件) = 下一个状态
     */
    private static final Map<CallStatus, Map<CallEvent, CallStatus>> TRANSITIONS;

    static {
        TRANSITIONS = new EnumMap<>(CallStatus.class);

        // INVITING 状态：等待被叫响应
        Map<CallEvent, CallStatus> inviting = new EnumMap<>(CallEvent.class);
        inviting.put(CallEvent.RING, CallStatus.RINGING);       // 被叫收到邀请，开始振铃
        inviting.put(CallEvent.CANCEL, CallStatus.ENDED);       // 主叫取消
        inviting.put(CallEvent.TIMEOUT, CallStatus.ENDED);      // 超时无响应
        inviting.put(CallEvent.BUSY, CallStatus.ENDED);         // 被叫忙线
        TRANSITIONS.put(CallStatus.INVITING, inviting);

        // RINGING 状态：被叫正在振铃
        Map<CallEvent, CallStatus> ringing = new EnumMap<>(CallEvent.class);
        ringing.put(CallEvent.ACCEPT, CallStatus.ACCEPTED);     // 被叫接听
        ringing.put(CallEvent.REJECT, CallStatus.ENDED);        // 被叫拒绝
        ringing.put(CallEvent.CANCEL, CallStatus.ENDED);        // 主叫取消
        ringing.put(CallEvent.TIMEOUT, CallStatus.ENDED);       // 振铃超时
        TRANSITIONS.put(CallStatus.RINGING, ringing);

        // ACCEPTED 状态：被叫已接听，准备建链
        Map<CallEvent, CallStatus> accepted = new EnumMap<>(CallEvent.class);
        accepted.put(CallEvent.CONNECT, CallStatus.CONNECTING); // WebRTC 开始建链
        accepted.put(CallEvent.ERROR, CallStatus.ENDED);        // 建链前异常
        TRANSITIONS.put(CallStatus.ACCEPTED, accepted);

        // CONNECTING 状态：WebRTC 建链中
        Map<CallEvent, CallStatus> connecting = new EnumMap<>(CallEvent.class);
        connecting.put(CallEvent.CALL_START, CallStatus.IN_CALL); // 建链成功，通话开始
        connecting.put(CallEvent.ERROR, CallStatus.ENDED);        // 建链失败
        connecting.put(CallEvent.TIMEOUT, CallStatus.ENDED);      // 建链超时
        TRANSITIONS.put(CallStatus.CONNECTING, connecting);

        // IN_CALL 状态：通话中
        Map<CallEvent, CallStatus> inCall = new EnumMap<>(CallEvent.class);
        inCall.put(CallEvent.HANG_UP, CallStatus.ENDED);        // 正常挂断
        inCall.put(CallEvent.ERROR, CallStatus.ENDED);          // 异常断开
        TRANSITIONS.put(CallStatus.IN_CALL, inCall);

        // ENDED 是终态，不注册任何转换
    }

    /**
     * 执行状态转换
     *
     * @param current 当前状态
     * @param event   触发事件
     * @return 下一个状态
     * @throws IllegalStateException 非法转换（当前状态下不允许该事件）
     */
    public static CallStatus transition(CallStatus current, CallEvent event) {
        Map<CallEvent, CallStatus> eventMap = TRANSITIONS.get(current);
        if (eventMap == null) {
            throw new IllegalStateException(
                    String.format("状态[%s]为终态，不允许任何转换", current));
        }
        CallStatus next = eventMap.get(event);
        if (next == null) {
            throw new IllegalStateException(
                    String.format("非法状态转换: [%s] + 事件[%s]", current, event));
        }
        return next;
    }

    /**
     * 判断当前状态下是否允许某个事件
     */
    public static boolean canTransition(CallStatus current, CallEvent event) {
        Map<CallEvent, CallStatus> eventMap = TRANSITIONS.get(current);
        return eventMap != null && eventMap.containsKey(event);
    }
}
