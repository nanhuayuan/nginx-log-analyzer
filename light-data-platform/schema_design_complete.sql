-- =====================================================
-- 全新ClickHouse表结构设计
-- 支持Self功能 + 核心监控指标 + 实时分析
-- 设计理念：全量存储 + 智能分层 + 高效查询
-- =====================================================

-- 创建数据库
CREATE DATABASE IF NOT EXISTS nginx_analytics;
USE nginx_analytics;

-- =====================================================
-- ODS层：原始数据全量存储
-- =====================================================
CREATE TABLE IF NOT EXISTS ods_nginx_raw (
    -- 基础标识
    id UInt64,
    log_time DateTime64(3) CODEC(Delta, ZSTD(1)),
    server_name LowCardinality(String),
    
    -- 请求信息（完整保留）
    client_ip String CODEC(ZSTD(1)),
    client_port UInt32,
    xff_ip String CODEC(ZSTD(1)),                -- X-Forwarded-For
    request_method LowCardinality(String),
    request_uri String CODEC(ZSTD(1)),
    request_uri_path String CODEC(ZSTD(1)),      -- URI路径部分
    request_query_string String CODEC(ZSTD(1)),   -- 查询参数（?后面部分）
    http_protocol LowCardinality(String),
    http_host String CODEC(ZSTD(1)),
    
    -- 响应信息
    response_status_code UInt16,
    response_body_size UInt64 CODEC(Delta, ZSTD(1)),
    response_content_type LowCardinality(String),
    
    -- 时间性能指标（核心）
    request_time Float64 CODEC(ZSTD(1)),                    -- 总请求时间
    upstream_response_time Float64 CODEC(ZSTD(1)),          -- 上游响应时间
    upstream_connect_time Float64 CODEC(ZSTD(1)),           -- 上游连接时间
    upstream_header_time Float64 CODEC(ZSTD(1)),            -- 上游头部时间
    upstream_status_code UInt16,                            -- 上游状态码
    upstream_cache_status LowCardinality(String),           -- 缓存命中状态
    upstream_addr String CODEC(ZSTD(1)),                    -- 上游地址
    
    -- 连接和传输
    connection_requests UInt32,                             -- 连接复用次数
    connection_id UInt64,                                   -- 连接ID
    request_length UInt64 CODEC(Delta, ZSTD(1)),           -- 请求大小
    bytes_sent UInt64 CODEC(Delta, ZSTD(1)),               -- 发送字节数
    
    -- 客户端信息
    user_agent String CODEC(ZSTD(1)),
    referer String CODEC(ZSTD(1)),
    
    -- 业务标识
    trace_id String CODEC(ZSTD(1)),                        -- 链路追踪ID
    business_sign LowCardinality(String),                   -- 业务标识
    cluster_name LowCardinality(String),                    -- 集群名称
    
    -- 时间维度（预计算）
    log_date Date MATERIALIZED toDate(log_time),
    log_hour UInt8 MATERIALIZED toHour(log_time),
    log_minute UInt8 MATERIALIZED toMinute(log_time),
    
    -- 元数据
    source_file String,
    created_at DateTime DEFAULT now()
    
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(log_date)
ORDER BY (log_date, log_hour, server_name, request_uri_path, log_time)
SETTINGS 
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

-- 创建TTL策略（可选：180天后删除）
-- ALTER TABLE ods_nginx_raw MODIFY TTL log_date + INTERVAL 180 DAY;

-- =====================================================
-- DWD层：清洗enriched数据，添加业务维度
-- =====================================================
CREATE TABLE IF NOT EXISTS dwd_nginx_enriched (
    -- 基础信息
    id UInt64,
    ods_id UInt64,
    log_time DateTime64(3) CODEC(Delta, ZSTD(1)),
    
    -- 维度信息（enriched）
    platform LowCardinality(String),               -- 平台：iOS/Android/Web等
    platform_version String,                       -- 平台版本
    entry_source LowCardinality(String),           -- 入口来源
    api_category LowCardinality(String),           -- API分类
    business_category LowCardinality(String),      -- 业务分类
    
    -- 请求信息（标准化）
    client_ip String CODEC(ZSTD(1)),
    client_region LowCardinality(String),          -- 客户端地理位置
    client_isp LowCardinality(String),             -- 客户端ISP
    request_method LowCardinality(String),
    request_uri_normalized String CODEC(ZSTD(1)),  -- 标准化URI（去参数）
    request_uri_pattern String CODEC(ZSTD(1)),     -- URI模式（/api/{id}/info）
    query_param_count UInt16,                      -- 查询参数个数
    has_sensitive_params Bool,                     -- 是否包含敏感参数
    
    -- 性能指标
    request_time Float64 CODEC(ZSTD(1)),
    response_status_code UInt16,
    response_size_kb Float64 CODEC(ZSTD(1)),
    
    -- 上游服务
    upstream_response_time Float64 CODEC(ZSTD(1)),
    upstream_status_code UInt16,
    upstream_cache_status LowCardinality(String),
    upstream_cluster LowCardinality(String),
    
    -- 连接信息
    connection_reused Bool,                        -- 连接是否复用
    connection_requests UInt32,
    
    -- 用户代理解析
    browser_name LowCardinality(String),
    browser_version String,
    os_name LowCardinality(String),
    os_version String,
    device_type LowCardinality(String),           -- mobile/desktop/tablet/bot
    is_bot Bool,
    bot_name LowCardinality(String),
    
    -- 业务标识
    trace_id String CODEC(ZSTD(1)),
    business_sign LowCardinality(String),
    user_id String CODEC(ZSTD(1)),                -- 用户ID（脱敏）
    session_id String CODEC(ZSTD(1)),             -- 会话ID
    
    -- 质量评估
    is_success Bool MATERIALIZED response_status_code < 400,
    is_client_error Bool MATERIALIZED response_status_code >= 400 AND response_status_code < 500,
    is_server_error Bool MATERIALIZED response_status_code >= 500,
    is_slow Bool MATERIALIZED request_time > 3.0,
    is_very_slow Bool MATERIALIZED request_time > 10.0,
    cache_hit Bool MATERIALIZED upstream_cache_status IN ('HIT', 'hit'),
    
    -- 异常检测
    has_anomaly Bool DEFAULT false,
    anomaly_score Float64 DEFAULT 0.0,
    anomaly_type LowCardinality(String) DEFAULT '',
    
    -- 时间维度
    log_date Date MATERIALIZED toDate(log_time),
    log_hour UInt8 MATERIALIZED toHour(log_time),
    log_minute UInt8 MATERIALIZED toMinute(log_time),
    log_weekday UInt8 MATERIALIZED toDayOfWeek(log_time),
    
    -- 元数据
    data_quality_score Float64 DEFAULT 1.0,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
    
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(log_date)
ORDER BY (log_date, log_hour, platform, api_category, request_uri_normalized, log_time, id)
SETTINGS index_granularity = 8192;

-- =====================================================
-- DWS层：聚合数据表，支持快速查询
-- =====================================================

-- 1. API性能聚合表（分钟级）
CREATE TABLE IF NOT EXISTS dws_api_metrics_minute (
    log_time DateTime CODEC(Delta, ZSTD(1)),
    request_uri_pattern String CODEC(ZSTD(1)),
    platform LowCardinality(String),
    api_category LowCardinality(String),
    
    -- 请求统计
    total_requests UInt64,
    success_requests UInt64,
    client_error_requests UInt64,
    server_error_requests UInt64,
    
    -- 性能统计（支持T-Digest精度）
    avg_request_time Float64,
    p50_request_time Float64,
    p90_request_time Float64,
    p95_request_time Float64,
    p99_request_time Float64,
    max_request_time Float64,
    
    -- 上游性能
    avg_upstream_time Float64,
    upstream_error_rate Float64,
    cache_hit_rate Float64,
    
    -- 连接统计
    avg_connection_reuse_rate Float64,
    unique_clients UInt64,
    
    -- 流量统计
    total_bytes_sent UInt64,
    avg_response_size Float64,
    
    created_at DateTime DEFAULT now()
    
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(toDate(log_time))
ORDER BY (toDate(log_time), toHour(log_time), toMinute(log_time), request_uri_pattern, platform)
SETTINGS index_granularity = 8192;

-- 2. 客户端行为聚合表（小时级）
CREATE TABLE IF NOT EXISTS dws_client_behavior_hour (
    log_time DateTime CODEC(Delta, ZSTD(1)),
    client_ip_hash UInt64,                         -- IP哈希保护隐私
    platform LowCardinality(String),
    client_region LowCardinality(String),
    device_type LowCardinality(String),
    
    -- 行为统计
    total_requests UInt64,
    unique_uris UInt64,
    unique_sessions UInt64,
    avg_request_interval Float64,                  -- 请求间隔
    
    -- 性能体验
    avg_response_time Float64,
    error_rate Float64,
    bounce_rate Float64,                           -- 跳出率
    
    -- 业务指标
    api_coverage_rate Float64,                     -- API覆盖率
    business_conversion_rate Float64,              -- 业务转化率
    
    -- 安全指标
    security_score Float64,                        -- 安全评分
    suspicious_behavior_count UInt32,
    
    created_at DateTime DEFAULT now()
    
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(toDate(log_time))
ORDER BY (toDate(log_time), toHour(log_time), client_region, device_type, client_ip_hash)
SETTINGS index_granularity = 8192;

-- 3. 业务链路追踪聚合表
CREATE TABLE IF NOT EXISTS dws_trace_analysis (
    log_date Date,
    trace_id String CODEC(ZSTD(1)),
    business_sign LowCardinality(String),
    
    -- 链路统计
    total_spans UInt32,                            -- 总调用次数
    service_count UInt16,                          -- 服务数量
    total_duration Float64,                        -- 总耗时
    critical_path_duration Float64,               -- 关键路径耗时
    
    -- 性能分析
    avg_span_duration Float64,
    max_span_duration Float64,
    bottleneck_service LowCardinality(String),
    
    -- 错误分析
    error_count UInt32,
    error_services Array(String),
    error_rate Float64,
    
    -- 业务指标
    business_success Bool,
    user_experience_score Float64,
    
    created_at DateTime DEFAULT now()
    
) ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(log_date)
ORDER BY (log_date, business_sign, trace_id)
SETTINGS index_granularity = 4096;

-- =====================================================
-- ADS层：应用数据服务，支持实时Dashboard
-- =====================================================

-- 1. 实时性能监控表
CREATE TABLE IF NOT EXISTS ads_realtime_metrics (
    metric_time DateTime CODEC(Delta, ZSTD(1)),
    metric_interval UInt16,                        -- 统计间隔（秒）
    
    -- 全局性能指标
    total_qps Float64,
    avg_response_time Float64,
    p95_response_time Float64,
    success_rate Float64,
    error_rate Float64,
    
    -- TOP榜单（JSON格式存储）
    top_slow_apis String,                          -- TOP 5最慢接口
    top_hot_apis String,                           -- TOP 5热点接口  
    top_error_apis String,                         -- TOP 5错误接口
    
    -- 集群对比
    cluster_performance_comparison String,         -- 集群性能对比
    
    -- 上游健康度
    upstream_health_score Float64,
    upstream_availability Float64,
    
    -- 缓存效率
    cache_hit_rate Float64,
    cache_performance_gain Float64,
    
    -- 告警状态
    alert_level LowCardinality(String),           -- normal/warning/critical
    active_alerts String,                         -- 活跃告警列表
    
    created_at DateTime DEFAULT now()
    
) ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(toDate(metric_time))
ORDER BY (toDate(metric_time), toHour(metric_time), metric_time)
SETTINGS index_granularity = 1024;

-- 2. 异常检测结果表
CREATE TABLE IF NOT EXISTS ads_anomaly_detection (
    detection_time DateTime CODEC(Delta, ZSTD(1)),
    anomaly_type LowCardinality(String),          -- response_time/error_rate/qps等
    
    -- 异常标识
    resource_type LowCardinality(String),         -- api/client/upstream
    resource_id String CODEC(ZSTD(1)),
    
    -- 异常详情
    anomaly_score Float64,
    severity_level LowCardinality(String),
    baseline_value Float64,
    current_value Float64,
    deviation_percentage Float64,
    
    -- 影响分析
    affected_requests UInt64,
    affected_users UInt64,
    business_impact_score Float64,
    
    -- 处理状态
    status LowCardinality(String) DEFAULT 'new',  -- new/investigating/resolved
    resolution_time Nullable(DateTime),
    resolution_note String DEFAULT '',
    
    created_at DateTime DEFAULT now()
    
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDate(detection_time))
ORDER BY (toDate(detection_time), toHour(detection_time), anomaly_type, severity_level, detection_time)
SETTINGS index_granularity = 4096;

-- =====================================================
-- 物化视图：实时数据流处理
-- =====================================================

-- 1. 实时API性能监控视图
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_realtime_api_performance
TO dws_api_metrics_minute
AS SELECT
    toStartOfMinute(log_time) as log_time,
    request_uri_normalized as request_uri_pattern,
    platform,
    api_category,
    
    count() as total_requests,
    countIf(is_success) as success_requests,
    countIf(is_client_error) as client_error_requests,
    countIf(is_server_error) as server_error_requests,
    
    avg(request_time) as avg_request_time,
    quantile(0.5)(request_time) as p50_request_time,
    quantile(0.9)(request_time) as p90_request_time,
    quantile(0.95)(request_time) as p95_request_time,
    quantile(0.99)(request_time) as p99_request_time,
    max(request_time) as max_request_time,
    
    avg(upstream_response_time) as avg_upstream_time,
    countIf(upstream_status_code >= 500) * 100.0 / count() as upstream_error_rate,
    countIf(cache_hit) * 100.0 / count() as cache_hit_rate,
    
    avg(if(connection_reused, 1.0, 0.0)) as avg_connection_reuse_rate,
    uniq(client_ip) as unique_clients,
    
    sum(response_size_kb * 1024) as total_bytes_sent,
    avg(response_size_kb * 1024) as avg_response_size,
    
    now() as created_at
    
FROM dwd_nginx_enriched
GROUP BY 
    toStartOfMinute(log_time),
    request_uri_normalized,
    platform,
    api_category;

-- 2. 实时客户端行为分析视图
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_realtime_client_behavior  
TO dws_client_behavior_hour
AS SELECT
    toStartOfHour(log_time) as log_time,
    cityHash64(client_ip) as client_ip_hash,
    platform,
    client_region,
    device_type,
    
    count() as total_requests,
    uniq(request_uri_normalized) as unique_uris,
    uniq(session_id) as unique_sessions,
    avg(lagInFrame(log_time) OVER (PARTITION BY client_ip ORDER BY log_time)) as avg_request_interval,
    
    avg(request_time) as avg_response_time,
    countIf(NOT is_success) * 100.0 / count() as error_rate,
    -- 简化跳出率计算
    if(count() = 1, 100.0, 0.0) as bounce_rate,
    
    uniq(api_category) * 100.0 / (SELECT uniq(api_category) FROM dwd_nginx_enriched WHERE log_date = today()) as api_coverage_rate,
    -- 简化业务转化率
    countIf(api_category = 'Business_Core') * 100.0 / count() as business_conversion_rate,
    
    -- 简化安全评分
    if(countIf(is_bot) > 0, 20.0, if(error_rate > 50, 40.0, 80.0)) as security_score,
    countIf(is_bot OR has_anomaly) as suspicious_behavior_count,
    
    now() as created_at
    
FROM dwd_nginx_enriched  
GROUP BY
    toStartOfHour(log_time),
    cityHash64(client_ip),
    platform,
    client_region,
    device_type;

-- =====================================================
-- 索引优化
-- =====================================================

-- 为高频查询创建索引
-- 1. API性能查询索引
ALTER TABLE dwd_nginx_enriched ADD INDEX idx_api_perf (request_uri_normalized, request_time) TYPE minmax GRANULARITY 4;

-- 2. 客户端行为索引  
ALTER TABLE dwd_nginx_enriched ADD INDEX idx_client (client_ip, platform, device_type) TYPE bloom_filter GRANULARITY 4;

-- 3. 时间范围查询索引
ALTER TABLE dwd_nginx_enriched ADD INDEX idx_time_range (log_time) TYPE minmax GRANULARITY 1;

-- 4. 错误查询索引
ALTER TABLE dwd_nginx_enriched ADD INDEX idx_errors (response_status_code, is_success) TYPE set(0) GRANULARITY 4;

COMMENT ON TABLE ods_nginx_raw IS '原始nginx日志全量存储，支持所有分析需求';
COMMENT ON TABLE dwd_nginx_enriched IS '清洗enriched数据，添加业务维度和质量评估';  
COMMENT ON TABLE dws_api_metrics_minute IS 'API性能分钟级聚合，支持实时监控';
COMMENT ON TABLE ads_realtime_metrics IS '实时性能监控，支持Dashboard展示';