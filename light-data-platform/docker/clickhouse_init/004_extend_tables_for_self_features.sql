-- 扩展表结构以支持Self目录高级功能
-- 添加Self分析器需要的所有关键字段

-- 1. 扩展ODS表（原始数据层）
ALTER TABLE nginx_analytics.ods_nginx_log 
ADD COLUMN IF NOT EXISTS request_full_uri String DEFAULT '',
ADD COLUMN IF NOT EXISTS query_parameters String DEFAULT '',
ADD COLUMN IF NOT EXISTS http_protocol_version LowCardinality(String) DEFAULT '',
ADD COLUMN IF NOT EXISTS referer_url String DEFAULT '',
ADD COLUMN IF NOT EXISTS total_request_duration Float64 DEFAULT 0.0,
ADD COLUMN IF NOT EXISTS upstream_connect_time Float64 DEFAULT 0.0,
ADD COLUMN IF NOT EXISTS upstream_header_time Float64 DEFAULT 0.0,
ADD COLUMN IF NOT EXISTS response_body_size_kb Float64 DEFAULT 0.0,
ADD COLUMN IF NOT EXISTS total_bytes_sent_kb Float64 DEFAULT 0.0;

-- 2. 扩展DWD表（明细数据层）- 添加Self功能需要的所有字段
ALTER TABLE nginx_analytics.dwd_nginx_enriched
-- HTTP生命周期时间指标
ADD COLUMN IF NOT EXISTS total_request_duration Float64 DEFAULT 0.0,
ADD COLUMN IF NOT EXISTS upstream_response_time Float64 DEFAULT 0.0,
ADD COLUMN IF NOT EXISTS upstream_header_time Float64 DEFAULT 0.0,
ADD COLUMN IF NOT EXISTS upstream_connect_time Float64 DEFAULT 0.0,
ADD COLUMN IF NOT EXISTS total_bytes_sent_kb Float64 DEFAULT 0.0,

-- 请求详细信息
ADD COLUMN IF NOT EXISTS request_full_uri String DEFAULT '',
ADD COLUMN IF NOT EXISTS query_parameters String DEFAULT '',
ADD COLUMN IF NOT EXISTS http_protocol_version LowCardinality(String) DEFAULT 'HTTP/1.1',
ADD COLUMN IF NOT EXISTS referer_url String DEFAULT '',
ADD COLUMN IF NOT EXISTS user_agent_string String DEFAULT '',

-- 时间维度分析字段
ADD COLUMN IF NOT EXISTS date Date DEFAULT toDate(timestamp),
ADD COLUMN IF NOT EXISTS hour UInt8 DEFAULT toHour(timestamp),
ADD COLUMN IF NOT EXISTS minute UInt8 DEFAULT toMinute(timestamp),
ADD COLUMN IF NOT EXISTS second UInt8 DEFAULT toSecond(timestamp),
ADD COLUMN IF NOT EXISTS date_hour String DEFAULT formatDateTime(timestamp, '%Y-%m-%d %H'),
ADD COLUMN IF NOT EXISTS date_hour_minute String DEFAULT formatDateTime(timestamp, '%Y-%m-%d %H:%M'),

-- 到达时间维度（用于连接数统计）
ADD COLUMN IF NOT EXISTS arrival_timestamp DateTime DEFAULT timestamp,
ADD COLUMN IF NOT EXISTS arrival_date Date DEFAULT toDate(timestamp),
ADD COLUMN IF NOT EXISTS arrival_hour UInt8 DEFAULT toHour(timestamp),

-- HTTP生命周期阶段时间（用于性能分析）
ADD COLUMN IF NOT EXISTS phase_upstream_connect Float64 DEFAULT 0.0,
ADD COLUMN IF NOT EXISTS phase_upstream_header Float64 DEFAULT 0.0,
ADD COLUMN IF NOT EXISTS phase_upstream_body Float64 DEFAULT 0.0,
ADD COLUMN IF NOT EXISTS phase_client_transfer Float64 DEFAULT 0.0,

-- 高级时间指标（Self高级分析需要）
ADD COLUMN IF NOT EXISTS backend_connect_phase Float64 DEFAULT 0.0,
ADD COLUMN IF NOT EXISTS backend_process_phase Float64 DEFAULT 0.0,
ADD COLUMN IF NOT EXISTS backend_transfer_phase Float64 DEFAULT 0.0,
ADD COLUMN IF NOT EXISTS nginx_transfer_phase Float64 DEFAULT 0.0,
ADD COLUMN IF NOT EXISTS backend_total_phase Float64 DEFAULT 0.0,
ADD COLUMN IF NOT EXISTS network_phase Float64 DEFAULT 0.0;

-- 3. 创建Self功能专用的高级DWS层表

-- API时间维度聚合表（支持连接数统计）
CREATE TABLE IF NOT EXISTS nginx_analytics.dws_api_time_dimension (
    date_partition Date,
    hour_partition UInt8,
    minute_partition UInt8,
    request_uri String,
    platform LowCardinality(String),
    api_category LowCardinality(String),
    
    -- 基础请求统计
    total_requests UInt64,
    success_requests UInt64,
    error_requests UInt64,
    slow_requests UInt64,
    
    -- 连接数统计（Self高级功能）
    new_connections UInt64,
    concurrent_connections UInt64,
    active_connections UInt64,
    
    -- 响应时间统计
    avg_response_time Float64,
    p50_response_time Float64,
    p95_response_time Float64,
    p99_response_time Float64,
    max_response_time Float64,
    min_response_time Float64,
    
    -- HTTP生命周期阶段统计
    avg_upstream_connect_time Float64,
    avg_upstream_header_time Float64,
    avg_upstream_response_time Float64,
    avg_backend_total_time Float64,
    avg_network_transfer_time Float64,
    
    -- 大小统计
    avg_response_size_kb Float64,
    total_bytes_sent_mb Float64,
    
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
    
) ENGINE = SummingMergeTree()
ORDER BY (date_partition, hour_partition, minute_partition, request_uri, platform)
PARTITION BY toYYYYMM(date_partition);

-- IP分析聚合表
CREATE TABLE IF NOT EXISTS nginx_analytics.dws_ip_analysis (
    date_partition Date,
    hour_partition UInt8,
    client_ip String,
    platform LowCardinality(String),
    
    -- 请求统计
    total_requests UInt64,
    success_requests UInt64,
    error_requests UInt64,
    slow_requests UInt64,
    anomaly_requests UInt64,
    
    -- 响应时间统计
    avg_response_time Float64,
    max_response_time Float64,
    
    -- 流量统计
    total_bytes_sent_mb Float64,
    
    -- 行为分析
    unique_apis UInt64,
    unique_user_agents UInt64,
    
    -- 风险评分（基于请求模式）
    risk_score Float64,
    risk_level LowCardinality(String), -- 'low', 'medium', 'high'
    
    created_at DateTime DEFAULT now()
    
) ENGINE = SummingMergeTree()
ORDER BY (date_partition, hour_partition, client_ip, platform)
PARTITION BY toYYYYMM(date_partition);

-- User-Agent分析聚合表
CREATE TABLE IF NOT EXISTS nginx_analytics.dws_user_agent_analysis (
    date_partition Date,
    user_agent_hash UInt64,
    user_agent_string String,
    
    -- 解析结果
    browser LowCardinality(String),
    browser_version String,
    os_name LowCardinality(String),
    os_version String,
    device_type LowCardinality(String),
    is_bot Bool,
    bot_name String,
    
    -- 统计信息
    total_requests UInt64,
    unique_ips UInt64,
    success_rate Float64,
    avg_response_time Float64,
    
    created_at DateTime DEFAULT now()
    
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (date_partition, user_agent_hash)
PARTITION BY toYYYYMM(date_partition);

-- 4. 创建Self功能专用ADS层表

-- 性能稳定性指标表
CREATE TABLE IF NOT EXISTS nginx_analytics.ads_performance_stability (
    metric_time DateTime,
    platform LowCardinality(String),
    api_category LowCardinality(String),
    
    -- 稳定性指标
    availability_rate Float64,
    reliability_score Float64,
    performance_consistency Float64,
    error_diversity Float64,
    
    -- 趋势指标
    request_growth_rate Float64,
    response_time_trend Float64,
    error_rate_trend Float64,
    
    -- 异常检测
    anomaly_count UInt64,
    anomaly_severity LowCardinality(String),
    anomaly_types Array(String),
    
    created_at DateTime DEFAULT now()
    
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (metric_time, platform, api_category)
PARTITION BY toYYYYMM(metric_time);

-- 接口错误分析表
CREATE TABLE IF NOT EXISTS nginx_analytics.ads_interface_errors (
    error_time DateTime,
    request_uri String,
    platform LowCardinality(String),
    response_status_code LowCardinality(String),
    
    -- 错误模式
    error_pattern LowCardinality(String),
    error_frequency UInt64,
    error_burst_detected Bool,
    
    -- 影响分析
    affected_users UInt64,
    business_impact_level LowCardinality(String),
    
    -- 根因分析线索
    upstream_error_rate Float64,
    network_latency_spike Bool,
    resource_bottleneck Bool,
    
    created_at DateTime DEFAULT now()
    
) ENGINE = MergeTree()
ORDER BY (error_time, request_uri, platform)
PARTITION BY toYYYYMM(error_time);

-- 5. 创建支持Self功能的物化视图

-- 实时时间维度分析视图
CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_time_dimension_realtime
TO nginx_analytics.dws_api_time_dimension
AS SELECT
    toDate(timestamp) as date_partition,
    toHour(timestamp) as hour_partition,
    toMinute(timestamp) as minute_partition,
    request_uri,
    platform,
    api_category,
    
    count() as total_requests,
    countIf(is_success) as success_requests,
    countIf(not is_success) as error_requests,
    countIf(is_slow) as slow_requests,
    
    -- 连接数计算（简化版）
    count() as new_connections,
    count() as concurrent_connections,
    count() as active_connections,
    
    -- 响应时间统计
    avg(response_time) as avg_response_time,
    quantile(0.5)(response_time) as p50_response_time,
    quantile(0.95)(response_time) as p95_response_time,
    quantile(0.99)(response_time) as p99_response_time,
    max(response_time) as max_response_time,
    min(response_time) as min_response_time,
    
    -- HTTP生命周期统计
    avg(upstream_connect_time) as avg_upstream_connect_time,
    avg(upstream_header_time) as avg_upstream_header_time,
    avg(upstream_response_time) as avg_upstream_response_time,
    avg(backend_total_phase) as avg_backend_total_time,
    avg(network_phase) as avg_network_transfer_time,
    
    -- 大小统计
    avg(response_size_kb) as avg_response_size_kb,
    sum(total_bytes_sent_kb) / 1024 as total_bytes_sent_mb,
    
    now() as created_at,
    now() as updated_at
    
FROM nginx_analytics.dwd_nginx_enriched
GROUP BY 
    toDate(timestamp),
    toHour(timestamp),
    toMinute(timestamp),
    request_uri,
    platform,
    api_category;

-- IP行为分析视图
CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_ip_behavior_analysis
TO nginx_analytics.dws_ip_analysis
AS SELECT
    toDate(timestamp) as date_partition,
    toHour(timestamp) as hour_partition,
    client_ip,
    platform,
    
    count() as total_requests,
    countIf(is_success) as success_requests,
    countIf(not is_success) as error_requests,
    countIf(is_slow) as slow_requests,
    countIf(has_anomaly) as anomaly_requests,
    
    avg(response_time) as avg_response_time,
    max(response_time) as max_response_time,
    
    sum(total_bytes_sent_kb) / 1024 as total_bytes_sent_mb,
    
    uniq(request_uri) as unique_apis,
    uniq(user_agent_string) as unique_user_agents,
    
    -- 简化风险评分
    if(countIf(not is_success) * 100.0 / count() > 20, 80.0,
       if(count() > 1000, 60.0, 20.0)) as risk_score,
    if(countIf(not is_success) * 100.0 / count() > 20, 'high',
       if(count() > 1000, 'medium', 'low')) as risk_level,
    
    now() as created_at
    
FROM nginx_analytics.dwd_nginx_enriched
GROUP BY 
    toDate(timestamp),
    toHour(timestamp),
    client_ip,
    platform;

-- COMMENT ON TABLE nginx_analytics.dws_api_time_dimension IS 'API time dimension analysis table for Self features';
-- COMMENT ON TABLE nginx_analytics.dws_ip_analysis IS 'IP behavior analysis table for Self features';
-- COMMENT ON TABLE nginx_analytics.ads_performance_stability IS 'Performance stability metrics for Self features';