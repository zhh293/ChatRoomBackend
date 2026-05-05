package com.example.chatroom.task;

import com.example.chatroom.module.user.mapper.UserRefreshTokenMapper;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

/**
 * 过期 Refresh Token 清理任务
 *
 * Cron: 0 0 3 * * ?（每天凌晨3点）
 * 路由策略: 第一个（FIRST）
 * 超时: 120s
 *
 * 批量删除，避免一次性大事务
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TokenCleanJobHandler {

    private final UserRefreshTokenMapper refreshTokenMapper;

    @XxlJob("tokenCleanJobHandler")
    public void execute() {
        XxlJobHelper.log("[TokenClean] 开始清理过期 Refresh Token");

        int totalDeleted = 0;
        int batchSize = 500;
        int deleted;
        do {
            deleted = refreshTokenMapper.deleteExpiredBatch(LocalDateTime.now(), batchSize);
            totalDeleted += deleted;
            XxlJobHelper.log("[TokenClean] 本批删除={}", deleted);
        } while (deleted == batchSize);

        XxlJobHelper.log("[TokenClean] 执行完毕，共删除={}", totalDeleted);
    }
}
