-- N9E数据库初始化脚本
-- 确保数据库和表结构正确创建

-- 创建数据库
CREATE DATABASE IF NOT EXISTS n9e_v6 CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE n9e_v6;

-- 创建用户表
CREATE TABLE IF NOT EXISTS `users` (
    `id` bigint unsigned not null auto_increment,
    `username` varchar(64) not null comment 'login name, cannot rename',
    `nickname` varchar(64) not null comment 'display name, chinese name',
    `password` varchar(128) not null default '',
    `phone` varchar(16) not null default '',
    `email` varchar(64) not null default '',
    `portrait` varchar(255) not null default '' comment 'portrait image url',
    `roles` varchar(255) not null comment 'Admin | Standard | Guest, split by space',
    `contacts` varchar(1024) comment 'json e.g. {wecom:xx, dingtalk_robot_token:yy}',
    `maintainer` tinyint(1) not null default 0,
    `belong` varchar(191) DEFAULT '' COMMENT 'belong',
    `last_active_time` bigint DEFAULT 0 COMMENT 'last_active_time',
    `create_at` bigint not null default 0,
    `create_by` varchar(64) not null default '',
    `update_at` bigint not null default 0,
    `update_by` varchar(64) not null default '',
    PRIMARY KEY (`id`),
    UNIQUE KEY (`username`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

-- 插入默认用户（如果不存在）
INSERT IGNORE INTO `users`(id, username, nickname, password, roles, create_at, create_by, update_at, update_by)
VALUES(1, 'root', '超管', 'root.2020', 'Admin', unix_timestamp(now()), 'system', unix_timestamp(now()), 'system');

-- 创建角色操作表
CREATE TABLE IF NOT EXISTS `role_operation` (
    `id` bigint unsigned not null auto_increment,
    `role_name` varchar(128) not null,
    `operation` varchar(191) not null,
    PRIMARY KEY (`id`),
    KEY `idx_role_name` (`role_name`),
    KEY `idx_operation` (`operation`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

-- 插入默认角色操作
INSERT IGNORE INTO `role_operation` (role_name, operation) VALUES
('Admin', '/event-pipelines/del'),
('Standard', '/event-pipelines/del');

-- 确保权限正确
FLUSH PRIVILEGES;