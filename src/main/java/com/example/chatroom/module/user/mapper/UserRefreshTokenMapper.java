package com.example.chatroom.module.user.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.example.chatroom.module.user.domain.entity.UserRefreshToken;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;

import java.time.LocalDateTime;

/**
 * Refresh Token Mapper
 */
@Mapper
public interface UserRefreshTokenMapper extends BaseMapper<UserRefreshToken> {

    /**
     * 批量删除过期或已撤销的 Token（凌晨清理任务用）
     */
    @Update("DELETE FROM user_refresh_token WHERE (expires_at < #{now} OR revoked = 1) LIMIT #{batchSize}")
    int deleteExpiredBatch(@Param("now") LocalDateTime now, @Param("batchSize") int batchSize);
}
