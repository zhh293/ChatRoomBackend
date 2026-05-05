package com.example.chatroom.module.user.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.example.chatroom.module.user.domain.entity.UserFriend;
import org.apache.ibatis.annotations.Mapper;

/**
 * 好友关系 Mapper
 */
@Mapper
public interface UserFriendMapper extends BaseMapper<UserFriend> {
}
