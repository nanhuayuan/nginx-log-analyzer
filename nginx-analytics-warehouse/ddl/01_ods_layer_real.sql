-- ==========================================
-- 基于实际nginx日志格式的ODS层设计
-- 参考: sample_nginx_logs\2025-04-23\access186.log
-- ==========================================

-- 创建数据库
CREATE DATABASE IF NOT EXISTS nginx_analytics;

-- ODS原始日志表 - 保持与现有schema_design_v2_fixed.sql一致
CREATE TABLE IF NOT EXISTS nginx_analytics.ods_nginx_raw (
    id UInt64,                                    -- ID主键
    log_time DateTime64(3) CODEC(Delta, ZSTD(1)), -- 日志时间
    server_name LowCardinality(String),           -- 服务器名称(http_host)
    client_ip String CODEC(ZSTD(1)),              -- 客户端IP(remote_addr)
    client_port UInt32,                           -- 客户端端口(remote_port)
    xff_ip String CODEC(ZSTD(1)),                 -- 真实IP(RealIp)
    remote_user String CODEC(ZSTD(1)),            -- 远程用户(remote_user)
    request_method LowCardinality(String),        -- 请求方法(从request字段解析)
    request_uri String CODEC(ZSTD(1)),            -- 请求URI(从request字段解析)
    request_full_uri String CODEC(ZSTD(1)),       -- 完整请求URI
    http_protocol LowCardinality(String),         -- HTTP协议版本
    response_status_code LowCardinality(String),  -- 响应状态码
    response_body_size UInt64,                    -- 响应体大小
    response_referer String CODEC(ZSTD(1)),       -- 来源页面
    user_agent String CODEC(ZSTD(1)),             -- 用户代理
    upstream_addr String CODEC(ZSTD(1)),          -- 上游服务地址
    upstream_connect_time Float64,                -- 上游连接时间
    upstream_header_time Float64,                 -- 上游响应头时间
    upstream_response_time Float64,               -- 上游响应时间
    total_request_time Float64,                   -- 总请求时间(ar_time主要性能指标)
    total_bytes_sent UInt64,                      -- 发送字节总数
    query_string String CODEC(ZSTD(1)),           -- 查询字符串
    connection_requests UInt32,                   -- 连接请求数
    trace_id String CODEC(ZSTD(1)),               -- 跟踪ID
    business_sign LowCardinality(String),         -- 业务标识
    application_name LowCardinality(String),      -- 应用名称
    service_name LowCardinality(String),          -- 服务名称
    cache_status LowCardinality(String),          -- 缓存状态
    cluster_node LowCardinality(String),          -- 集群节点
    log_source_file LowCardinality(String),       -- 日志源文件
    created_at DateTime DEFAULT now(),            -- 创建时间
    date_partition Date MATERIALIZED toDate(log_time), -- 日期分区
    hour_partition UInt8 MATERIALIZED toHour(log_time) -- 小时分区
) ENGINE = MergeTree()
PARTITION BY date_partition
ORDER BY (date_partition, hour_partition, server_name, client_ip, log_time)
SETTINGS index_granularity = 8192;

-- 样例数据格式参考:
-- http_host:zgtapp.zwfw.gxzf.gov.cn remote_addr:"100.100.8.44" remote_port:"10305" 
-- remote_user:"-" time:"2025-04-23T00:00:02+08:00" 
-- request:"GET /group1/M00/06/B3/rBAWN2f-ZIKAJI2vAAIkLKrgt-I560.png HTTP/1.1" 
-- code:"200" body:"140332" http_referer:"-" ar_time:"0.325" RealIp:"100.100.8.44" 
-- agent:"zgt-ios/1.4.1 (iPhone; iOS 15.4.1; Scale/3.00)"

-- 表注释：Nginx原始日志ODS层，基于实际日志格式设计，保持数据完整性