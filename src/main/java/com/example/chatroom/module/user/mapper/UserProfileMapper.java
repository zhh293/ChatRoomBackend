package com.example.chatroom.module.user.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.example.chatroom.module.user.domain.entity.UserProfile;
import org.apache.ibatis.annotations.Mapper;

/**
 * 用户扩展信息 Mapper
 */
@Mapper
public interface UserProfileMapper extends BaseMapper<UserProfile> {
}
