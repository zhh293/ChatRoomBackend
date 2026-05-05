package com.example.chatroom.module.session.service;

import com.example.chatroom.common.response.PageResult;
import com.example.chatroom.module.session.domain.dto.CreateGroupSessionDTO;
import com.example.chatroom.module.session.domain.dto.CreateSingleSessionDTO;
import com.example.chatroom.module.session.domain.dto.UpdateGroupSessionDTO;
import com.example.chatroom.module.session.domain.vo.ReadPositionVO;
import com.example.chatroom.module.session.domain.vo.SessionListVO;
import com.example.chatroom.module.session.domain.vo.SessionMemberVO;
import com.example.chatroom.module.session.domain.vo.SessionVO;

import java.util.List;
import java.util.Map;

/**
 * 会话服务接口
 */
public interface SessionService {

    /**
     * 获取当前用户的会话列表（分页，含未读数 + 总未读数）
     *
     * @param userId         当前用户 ID
     * @param page           页码（从 1 开始）
     * @param size           每页条数
     * @param lastReadMsgIds 前端传入的各会话已读位置，key=sessionNo，value=lastReadMsgId
     */
    SessionListVO listSessions(Long userId, int page, int size, Map<String, Long> lastReadMsgIds);

    /**
     * 分页拉取当前用户各会话的已读位置（lastReadMsgId）
     * <p>
     * 前端在首次加载或重新登录时调用，把本地缓存的 lastReadMsgId 与服务端对齐。
     * 每页约 15 条，按 session_id DESC 排序（最近活跃的会话优先）。
     *
     * @param userId 当前用户 ID
     * @param page   页码（从 1 开始）
     * @param size   每页条数（建议 15，最大 50）
     * @return 分页结果，每条包含 sessionNo + lastReadMsgId
     */
    PageResult<ReadPositionVO> getReadPositions(Long userId, int page, int size);

    /** 创建或获取单聊会话 */
    SessionVO getOrCreateSingleSession(Long userId, CreateSingleSessionDTO dto);

    /** 创建群聊会话 */
    SessionVO createGroupSession(Long userId, CreateGroupSessionDTO dto);

    /** 获取会话详情 */
    SessionVO getSessionDetail(Long userId, Long sessionId);

    /** 退出/解散会话 */
    void leaveSession(Long userId, Long sessionId);

    /** 邀请成员入群 */
    void inviteMembers(Long userId, Long sessionId, List<String> memberUserNos);

    /** 踢出成员 */
    void kickMember(Long operatorId, Long sessionId, Long targetUserId);

    /**
     * 修改群聊信息（群名/头像）
     * <p>仅群主或管理员可操作</p>
     *
     * @param operatorId 操作人 ID
     * @param sessionId  会话 ID
     * @param dto        修改内容，null 字段不修改
     */
    void updateGroupSession(Long operatorId, Long sessionId, UpdateGroupSessionDTO dto);

    /**
     * 获取会话成员列表
     *
     * @param userId    当前用户 ID（鉴权：必须是会话成员才能查看）
     * @param sessionId 会话 ID
     * @return 成员列表，含用户基本信息 + 角色
     */
    List<SessionMemberVO> getMembers(Long userId, Long sessionId);
}
