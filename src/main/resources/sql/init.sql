-- ============================================================
-- 聊天室数据库初始化脚本
-- 数据库：chatroom
-- ============================================================

CREATE DATABASE IF NOT EXISTS chatroom DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE chatroom;

-- 用户表
CREATE TABLE IF NOT EXISTS `user` (
  `id`              BIGINT       NOT NULL AUTO_INCREMENT,
  `user_no`         VARCHAR(32)  NOT NULL,
  `username`        VARCHAR(64)  NOT NULL,
  `nickname`        VARCHAR(64)  NOT NULL,
  `password_hash`   VARCHAR(128) NOT NULL,
  `avatar_url`      VARCHAR(512) DEFAULT NULL,
  `email`           VARCHAR(128) DEFAULT NULL,
  `phone`           VARCHAR(20)  DEFAULT NULL,
  `gender`          TINYINT      DEFAULT 0,
  `bio`             VARCHAR(256) DEFAULT NULL,
  `status`          TINYINT      DEFAULT 1,
  `last_login_at`   DATETIME     DEFAULT NULL,
  `last_login_ip`   VARCHAR(64)  DEFAULT NULL,
  `created_at`      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at`      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `deleted_at`      DATETIME     DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_user_no` (`user_no`),
  UNIQUE KEY `uk_username` (`username`),
  KEY `idx_phone` (`phone`),
  KEY `idx_email` (`email`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 用户扩展信息表
CREATE TABLE IF NOT EXISTS `user_profile` (
  `id`             BIGINT       NOT NULL AUTO_INCREMENT,
  `user_id`        BIGINT       NOT NULL,
  `real_name`      VARCHAR(64)  DEFAULT NULL,
  `birthday`       DATE         DEFAULT NULL,
  `region`         VARCHAR(128) DEFAULT NULL,
  `signature`      VARCHAR(512) DEFAULT NULL,
  `background_url` VARCHAR(512) DEFAULT NULL,
  `created_at`     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at`     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_user_id` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 好友关系表
CREATE TABLE IF NOT EXISTS `user_friend` (
  `id`         BIGINT      NOT NULL AUTO_INCREMENT,
  `user_id`    BIGINT      NOT NULL,
  `friend_id`  BIGINT      NOT NULL,
  `remark`     VARCHAR(64) DEFAULT NULL,
  `status`     TINYINT     DEFAULT 1,
  `created_at` DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_user_friend` (`user_id`, `friend_id`),
  KEY `idx_friend_id` (`friend_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Refresh Token 表
CREATE TABLE IF NOT EXISTS `user_refresh_token` (
  `id`          BIGINT       NOT NULL AUTO_INCREMENT,
  `user_id`     BIGINT       NOT NULL,
  `token_hash`  VARCHAR(128) NOT NULL,
  `device_info` VARCHAR(256) DEFAULT NULL,
  `ip`          VARCHAR(64)  DEFAULT NULL,
  `expires_at`  DATETIME     NOT NULL,
  `revoked`     TINYINT      DEFAULT 0,
  `created_at`  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_user_id` (`user_id`),
  KEY `idx_token_hash` (`token_hash`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 会话表
CREATE TABLE IF NOT EXISTS `session` (
  `id`               BIGINT       NOT NULL AUTO_INCREMENT,
  `session_no`       VARCHAR(64)  NOT NULL,
  `type`             TINYINT      NOT NULL,
  `name`             VARCHAR(128) DEFAULT NULL,
  `avatar_url`       VARCHAR(512) DEFAULT NULL,
  `owner_id`         BIGINT       DEFAULT NULL,
  `last_msg_id`      BIGINT       DEFAULT NULL,
  `last_msg_content` VARCHAR(512) DEFAULT NULL,
  `last_msg_at`      DATETIME     DEFAULT NULL,
  `member_count`     INT          DEFAULT 0,
  `status`           TINYINT      DEFAULT 1,
  `created_at`       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at`       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `deleted_at`       DATETIME     DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_session_no` (`session_no`),
  KEY `idx_last_msg_at` (`last_msg_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 会话成员表
CREATE TABLE IF NOT EXISTS `session_member` (
  `id`               BIGINT   NOT NULL AUTO_INCREMENT,
  `session_id`       BIGINT   NOT NULL,
  `user_id`          BIGINT   NOT NULL,
  `role`             TINYINT  DEFAULT 1,
  `alias`            VARCHAR(64) DEFAULT NULL,
  `is_muted`         TINYINT  DEFAULT 0,
  `is_pinned`        TINYINT  DEFAULT 0,
  `is_disturb`       TINYINT  DEFAULT 0,
  `last_read_msg_id` BIGINT   DEFAULT 0,
  `joined_at`        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `left_at`          DATETIME DEFAULT NULL,
  `created_at`       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at`       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_session_user` (`session_id`, `user_id`),
  KEY `idx_user_id` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 消息分片表（chat_message_0 ~ chat_message_15）
-- 以下为生成16张分片表的存储过程
DELIMITER $$
CREATE PROCEDURE create_chat_message_tables()
BEGIN
  DECLARE i INT DEFAULT 0;
  WHILE i < 16 DO
    SET @sql = CONCAT('CREATE TABLE IF NOT EXISTS `chat_message_', i, '` (
      `id`           BIGINT      NOT NULL,
      `msg_no`       VARCHAR(64) NOT NULL,
      `session_id`   BIGINT      NOT NULL,
      `sender_id`    BIGINT      NOT NULL,
      `msg_type`     TINYINT     NOT NULL,
      `content`      TEXT        DEFAULT NULL,
      `extra`        JSON        DEFAULT NULL,
      `reply_msg_id` BIGINT      DEFAULT NULL,
      `status`       TINYINT     DEFAULT 1,
      `is_read`      TINYINT     DEFAULT 0,
      `created_at`   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
      `updated_at`   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
      PRIMARY KEY (`id`),
      UNIQUE KEY `uk_msg_no` (`msg_no`),
      KEY `idx_session_created` (`session_id`, `created_at` DESC),
      KEY `idx_sender_id` (`sender_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4');
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    SET i = i + 1;
  END WHILE;
END$$
DELIMITER ;

CALL create_chat_message_tables();
DROP PROCEDURE IF EXISTS create_chat_message_tables;

-- 本地消息发件箱表
CREATE TABLE IF NOT EXISTS `local_msg_outbox` (
  `id`            BIGINT   NOT NULL AUTO_INCREMENT,
  `msg_no`        VARCHAR(64) NOT NULL,
  `session_id`    BIGINT   NOT NULL,
  `sender_id`     BIGINT   NOT NULL,
  `payload`       TEXT     NOT NULL,
  `status`        TINYINT  DEFAULT 0,
  `retry_count`   INT      DEFAULT 0,
  `next_retry_at` DATETIME DEFAULT NULL,
  `created_at`    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at`    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_msg_no` (`msg_no`),
  KEY `idx_status_retry` (`status`, `next_retry_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 群聊消息已读回执表
CREATE TABLE IF NOT EXISTS `msg_read_receipt` (
  `id`         BIGINT   NOT NULL AUTO_INCREMENT,
  `session_id` BIGINT   NOT NULL,
  `msg_id`     BIGINT   NOT NULL,
  `user_id`    BIGINT   NOT NULL,
  `read_at`    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_msg_user` (`msg_id`, `user_id`),
  KEY `idx_session_user` (`session_id`, `user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 系统通知表
CREATE TABLE IF NOT EXISTS `system_notification` (
  `id`         BIGINT       NOT NULL AUTO_INCREMENT,
  `user_id`    BIGINT       NOT NULL,
  `type`       TINYINT      NOT NULL,
  `title`      VARCHAR(128) NOT NULL,
  `content`    VARCHAR(512) DEFAULT NULL,
  `extra`      JSON         DEFAULT NULL,
  `is_read`    TINYINT      DEFAULT 0,
  `created_at` DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_user_read` (`user_id`, `is_read`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
