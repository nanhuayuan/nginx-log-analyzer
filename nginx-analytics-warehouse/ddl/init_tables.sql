-- ==========================================
-- Nginx Analytics Data Warehouse
-- 数据库和表初始化DDL脚本
-- ==========================================

-- 创建数据库
CREATE DATABASE IF NOT EXISTS nginx_analytics;
USE nginx_analytics;

-- 删除现有表（重新初始化时使用）
-- DROP TABLE IF EXISTS ads_status_stats;
-- DROP TABLE IF EXISTS ads_platform_stats;
-- DROP TABLE IF EXISTS ads_top_hot_apis;
-- DROP TABLE IF EXISTS dwd_nginx_enriched;
-- DROP TABLE IF EXISTS ods_nginx_raw;

-- ==========================================
-- ODS层：原始数据层
-- ==========================================

-- ODS原始日志表
CREATE TABLE IF NOT EXISTS ods_nginx_raw (
    id UUID DEFAULT generateUUIDv4(),
    log_time DateTime64(3) CODEC(Delta, ZSTD(1)),
    server_name LowCardinality(String),              -- http_host
    client_ip String CODEC(ZSTD(1)),                 -- remote_addr  
    client_port UInt32,                              -- remote_port
    xff_ip String CODEC(ZSTD(1)),                    -- RealIp
    remote_user String CODEC(ZSTD(1)),               -- remote_user
    request_method LowCardinality(String),           -- 从request字段解析
    request_uri String CODEC(ZSTD(1)),               -- 从request字段解析
    request_full_uri String CODEC(ZSTD(1)),          -- 完整request
    http_protocol LowCardinality(String),            -- HTTP/1.1
    response_status_code LowCardinality(String),     -- code
    response_body_size UInt64,                       -- body
    response_referer String CODEC(ZSTD(1)),          -- http_referer
    user_agent String CODEC(ZSTD(1)),                -- agent
    upstream_addr String CODEC(ZSTD(1)),
    upstream_connect_time Float64,
    upstream_header_time Float64, 
    upstream_response_time Float64,
    total_request_time Float64,                      -- ar_time (主要性能指标)
    total_bytes_sent UInt64,
    query_string String CODEC(ZSTD(1)),
    connection_requests UInt32,
    trace_id String CODEC(ZSTD(1)),
    business_sign LowCardinality(String),
    application_name LowCardinality(String),
    service_name LowCardinality(String),
    cache_status LowCardinality(String),
    cluster_node LowCardinality(String),
    log_source_file LowCardinality(String),
    created_at DateTime DEFAULT now(),
    date_partition Date MATERIALIZED toDate(log_time),
    hour_partition UInt8 MATERIALIZED toHour(log_time)
) ENGINE = MergeTree()
PARTITION BY date_partition
ORDER BY (date_partition, hour_partition, server_name, client_ip, log_time)
SETTINGS index_granularity = 8192;

-- ==========================================
-- DWD层：明细数据层
-- ==========================================

-- DWD增强明细表
CREATE TABLE IF NOT EXISTS dwd_nginx_enriched (
    id UUID,
    log_time DateTime64(3),
    server_name LowCardinality(String),
    client_ip String,
    client_port UInt32,
    xff_ip String,
    remote_user String,
    request_method LowCardinality(String),
    request_uri String,
    request_full_uri String,
    http_protocol LowCardinality(String),
    response_status_code LowCardinality(String),
    response_body_size UInt64,
    response_referer String,
    user_agent String,
    upstream_addr String,
    upstream_connect_time Float64,
    upstream_header_time Float64,
    upstream_response_time Float64,
    total_request_time Float64,
    total_bytes_sent UInt64,
    query_string String,
    connection_requests UInt32,
    trace_id String,
    business_sign LowCardinality(String),
    application_name LowCardinality(String),
    service_name LowCardinality(String),
    cache_status LowCardinality(String),
    cluster_node LowCardinality(String),
    log_source_file LowCardinality(String),
    
    -- 增强字段
    platform LowCardinality(String),                -- iOS/Android/Web
    api_category LowCardinality(String),             -- API类型分类
    is_success UInt8,                                -- 是否成功请求
    is_slow_request UInt8,                           -- 是否慢请求
    is_mobile UInt8,                                 -- 是否移动端
    browser_family LowCardinality(String),          -- 浏览器类型
    os_family LowCardinality(String),               -- 操作系统
    device_family LowCardinality(String),           -- 设备类型
    geo_country LowCardinality(String),             -- 地理位置-国家
    geo_region String,                               -- 地理位置-区域
    
    created_at DateTime DEFAULT now(),
    date_partition Date MATERIALIZED toDate(log_time),
    hour_partition UInt8 MATERIALIZED toHour(log_time)
) ENGINE = MergeTree()
PARTITION BY date_partition
ORDER BY (date_partition, hour_partition, platform, api_category, server_name)
SETTINGS index_granularity = 8192;

-- ==========================================
-- ADS层：应用数据层
-- ==========================================

-- ADS热门API统计
CREATE TABLE IF NOT EXISTS ads_top_hot_apis (
    stat_date Date,
    request_uri String,
    platform LowCardinality(String),
    request_count UInt64,
    success_count UInt64,
    avg_response_time Float64,
    p95_response_time Float64,
    total_bytes UInt64,
    unique_visitors UInt64,
    created_at DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree()
ORDER BY (stat_date, request_uri, platform)
SETTINGS index_granularity = 8192;

-- ADS平台统计
CREATE TABLE IF NOT EXISTS ads_platform_stats (
    stat_date Date,
    platform LowCardinality(String),
    total_requests UInt64,
    success_requests UInt64,
    failed_requests UInt64,
    success_rate Float64,
    avg_response_time Float64,
    p95_response_time Float64,
    total_bytes UInt64,
    unique_visitors UInt64,
    created_at DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree()
ORDER BY (stat_date, platform)
SETTINGS index_granularity = 8192;

-- ADS状态码统计
CREATE TABLE IF NOT EXISTS ads_status_stats (
    stat_date Date,
    status_code LowCardinality(String),
    platform LowCardinality(String),
    request_count UInt64,
    percentage Float64,
    avg_response_time Float64,
    created_at DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree()
ORDER BY (stat_date, status_code, platform)
SETTINGS index_granularity = 8192;

-- ==========================================
-- 索引优化
-- ==========================================

-- 为常用查询添加索引
ALTER TABLE dwd_nginx_enriched ADD INDEX IF NOT EXISTS idx_response_time total_request_time TYPE minmax GRANULARITY 1;
ALTER TABLE dwd_nginx_enriched ADD INDEX IF NOT EXISTS idx_status_code response_status_code TYPE set(100) GRANULARITY 1;
ALTER TABLE dwd_nginx_enriched ADD INDEX IF NOT EXISTS idx_platform platform TYPE set(50) GRANULARITY 1;

-- ==========================================
-- 表注释
-- ==========================================

-- 添加表注释（如果ClickHouse支持）
-- COMMENT ON TABLE ods_nginx_raw IS 'Nginx原始日志ODS层，基于实际日志格式设计';
-- COMMENT ON TABLE dwd_nginx_enriched IS 'Nginx增强明细数据DWD层，包含平台识别和性能分析';
-- COMMENT ON TABLE ads_top_hot_apis IS 'ADS应用层 - 热门API统计表';
-- COMMENT ON TABLE ads_platform_stats IS 'ADS应用层 - 平台统计表';
-- COMMENT ON TABLE ads_status_stats IS 'ADS应用层 - HTTP状态码统计表';