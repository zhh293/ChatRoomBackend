package com.example.chatroom.task;

import com.example.chatroom.module.message.mapper.LocalMsgOutboxMapper;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

/**
 * 消息发件箱归档清理任务
 *
 * Cron: 0 0 4 * * ?（每天凌晨4点）
 * 路由策略: 第一个（FIRST）
 * 超时: 300s
 *
 * local_msg_outbox 中 status=2（已成功落库）的记录7天后可安全删除
 * 防止表无限膨胀
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxArchiveJobHandler {

    private final LocalMsgOutboxMapper outboxMapper;

    @XxlJob("outboxArchiveJobHandler")
    public void execute() {
        XxlJobHelper.log("[OutboxArchive] 开始归档清理");

        LocalDateTime threshold = LocalDateTime.now().minusDays(7);
        int totalDeleted = 0;
        int batchSize = 1000;
        int deleted;
        do {
            deleted = outboxMapper.deleteArchivedBatch(threshold, batchSize);
            totalDeleted += deleted;
            XxlJobHelper.log("[OutboxArchive] 本批删除={}", deleted);
        } while (deleted == batchSize);

        XxlJobHelper.log("[OutboxArchive] 执行完毕，共删除={}", totalDeleted);
    }
}
