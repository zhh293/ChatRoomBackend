package com.example.chatroom.module.message.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.example.chatroom.module.message.domain.entity.MsgDeadLetter;
import org.apache.ibatis.annotations.Mapper;

/**
 * 消息死信 Mapper
 */
@Mapper
public interface MsgDeadLetterMapper extends BaseMapper<MsgDeadLetter> {
}
