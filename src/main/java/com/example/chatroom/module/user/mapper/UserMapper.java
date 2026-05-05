package com.example.chatroom.module.user.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.example.chatroom.module.user.domain.entity.User;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * 用户 Mapper
 */
@Mapper
public interface UserMapper extends BaseMapper<User> {

    /**
     * 查询 ID 上界（布隆过滤器初始化用）
     */
    @Select("SELECT MAX(id) FROM user WHERE deleted_at IS NULL")
    Long selectMaxId();

    /**
     * 游标分批查询 ID（布隆过滤器初始化用，避免大偏移量分页）
     */
    @Select("SELECT id FROM user WHERE id > #{lastId} AND deleted_at IS NULL ORDER BY id ASC LIMIT #{batchSize}")
    List<Long> selectIdsBatch(@Param("lastId") long lastId, @Param("batchSize") int batchSize);

    /**
     * 批量查询用户信息（群成员列表用）
     * 一次 IN 查询替代 N 次单查，避免 N+1 问题
     */
    @Select("""
            <script>
            SELECT id, user_no, username, nickname, avatar_url, bio, gender, status
            FROM user
            WHERE id IN
            <foreach collection="ids" item="id" open="(" separator="," close=")">
                #{id}
            </foreach>
            AND deleted_at IS NULL
            </script>
            """)
    List<User> selectBatchByIds(@Param("ids") List<Long> ids);
}
