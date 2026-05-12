package com.example.chatroom.module.call.service;

import com.example.chatroom.module.call.domain.dto.CallInitiateDTO;
import com.example.chatroom.module.call.domain.vo.CallInitiateVO;

/**
 * 语音通话服务
 */
public interface CallService {

    /**
     * 发起语音通话
     */
    CallInitiateVO initiateCall(Long callerId, CallInitiateDTO dto);
}
