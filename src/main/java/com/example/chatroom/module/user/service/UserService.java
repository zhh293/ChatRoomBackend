package com.example.chatroom.module.user.service;

import com.example.chatroom.module.auth.domain.vo.TokenVO;
import com.example.chatroom.module.user.domain.dto.RegisterDTO;
import com.example.chatroom.module.user.domain.entity.User;
import com.example.chatroom.module.user.domain.dto.UpdateUserDTO;
import com.example.chatroom.module.user.domain.dto.UpdateFriendDTO;
import com.example.chatroom.module.user.domain.dto.UpdateProfileDTO;
import com.example.chatroom.module.user.domain.vo.UserProfileVO;
import com.example.chatroom.module.user.domain.vo.UserVO;

import java.util.List;

/**
 * 用户服务接口
 */
public interface UserService {

    /**
     * 用户注册
     * 注册成功后直接走登录流程（创建双 Token、入库、入 Redis、布隆过滤器），
     * 返回与登录接口相同的 TokenVO，前端无需二次登录。
     *
     * @param dto      注册请求参数
     * @param clientIp 客户端 IP（用于 refreshToken 记录）
     */
    TokenVO register(RegisterDTO dto, String clientIp);

    /** 获取当前用户信息 */
    UserVO getMe(Long userId);

    /** 修改当前用户基础信息 */
    void updateMe(Long userId, UpdateUserDTO dto);

    /** 修改密码 */
    void updatePassword(Long userId, String oldPassword, String newPassword);

    /** 更新头像 */
    void updateAvatar(Long userId, String avatarUrl);

    /** 注销账号（软删除） */
    void deleteMe(Long userId);

    /** 查看指定用户公开信息 */
    UserVO getUserByNo(String userNo);

    /**
     * 批量获取用户信息（走缓存，未命中批量回源 DB）
     *
     * @param userIds 用户 ID 列表
     * @return 用户实体列表，不存在的用户自动跳过
     */
    List<User> batchGetUsers(List<Long> userIds);

    /** 搜索用户（按用户名/手机号） */
    List<UserVO> searchUsers(String keyword);

    /** 获取好友列表 */
    List<UserVO> getFriends(Long userId);

    /** 添加好友 */
    void addFriend(Long userId, String friendUserNo);

    /** 删除好友 */
    void deleteFriend(Long userId, Long friendId);

    /** 修改好友备注 / 拉黑 */
    void updateFriend(Long userId, Long friendId, UpdateFriendDTO dto);

    /** 获取用户扩展信息 */
    UserProfileVO getProfile(Long userId);

    /** 更新用户扩展信息 */
    void updateProfile(Long userId, UpdateProfileDTO dto);
}
