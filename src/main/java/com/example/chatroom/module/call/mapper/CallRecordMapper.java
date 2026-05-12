package com.example.chatroom.module.call.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.example.chatroom.module.call.domain.entity.CallRecord;
import org.apache.ibatis.annotations.Mapper;

/**
 * 通话记录 Mapper
 */
@Mapper
public interface CallRecordMapper extends BaseMapper<CallRecord> {
}
