package com.example.chatroom.module.notification.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.example.chatroom.module.notification.domain.entity.SystemNotification;
import org.apache.ibatis.annotations.Mapper;

/**
 * 系统通知 Mapper
 */
@Mapper
public interface SystemNotificationMapper extends BaseMapper<SystemNotification> {
}
