package com.example.chatroom.module.call.controller;

import com.example.chatroom.common.interceptor.UserContext;
import com.example.chatroom.common.response.Result;
import com.example.chatroom.module.call.domain.dto.CallInitiateDTO;
import com.example.chatroom.module.call.domain.vo.CallInitiateVO;
import com.example.chatroom.module.call.service.CallService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

/**
 * 语音通话 Controller
 */
@RestController
@RequestMapping("/api/v1/calls")
@RequiredArgsConstructor
public class CallController {

    private final CallService callService;

    /**
     * 发起语音通话
     */
    @PostMapping("/initiate")
    public Result<CallInitiateVO> initiateCall(@Valid @RequestBody CallInitiateDTO dto) {
        return Result.ok(callService.initiateCall(UserContext.getRequired(), dto));
    }
}
