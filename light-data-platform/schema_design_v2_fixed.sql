-- ============================================================
-- Self功能完整支持的ClickHouse表结构设计 V2.0 - 修复版
-- 解决ClickHouse语法问题
-- ============================================================

-- 创建数据库
CREATE DATABASE IF NOT EXISTS nginx_analytics;

-- ============================================================
-- ODS层：原始数据存储层
-- ============================================================
CREATE TABLE IF NOT EXISTS ods_nginx_raw (
    id UInt64,
    log_time DateTime64(3) CODEC(Delta, ZSTD(1)),
    server_name LowCardinality(String),
    client_ip String CODEC(ZSTD(1)),
    client_port UInt32,
    xff_ip String CODEC(ZSTD(1)),
    remote_user String CODEC(ZSTD(1)),
    request_method LowCardinality(String),
    request_uri String CODEC(ZSTD(1)),
    request_full_uri String CODEC(ZSTD(1)),
    http_protocol LowCardinality(String),
    response_status_code LowCardinality(String),
    response_body_size UInt64,
    response_referer String CODEC(ZSTD(1)),
    user_agent String CODEC(ZSTD(1)),
    upstream_addr String CODEC(ZSTD(1)),
    upstream_connect_time Float64,
    upstream_header_time Float64,
    upstream_response_time Float64,
    total_request_time Float64,
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

-- ============================================================
-- DWD层：数据仓库明细层（65字段完整支持）
-- ============================================================
CREATE TABLE IF NOT EXISTS dwd_nginx_enriched (
    -- 主键和基础信息
    id UInt64,
    ods_id UInt64,
    log_time DateTime64(3) CODEC(Delta, ZSTD(1)),
    date_partition Date,
    hour_partition UInt8,
    minute_partition UInt8,
    second_partition UInt8,
    
    -- 请求基础信息
    client_ip String CODEC(ZSTD(1)),
    client_port UInt32,
    xff_ip String CODEC(ZSTD(1)),
    server_name LowCardinality(String),
    request_method LowCardinality(String),
    request_uri String CODEC(ZSTD(1)),
    request_uri_normalized String CODEC(ZSTD(1)),
    request_full_uri String CODEC(ZSTD(1)),
    query_parameters String CODEC(ZSTD(1)),
    http_protocol_version LowCardinality(String),
    
    -- 响应信息
    response_status_code LowCardinality(String),
    response_body_size UInt64,
    response_body_size_kb Float64,
    total_bytes_sent UInt64,
    total_bytes_sent_kb Float64,
    
    -- 核心时间字段
    total_request_duration Float64,
    upstream_connect_time Float64,
    upstream_header_time Float64,
    upstream_response_time Float64,
    
    -- 阶段时间字段
    backend_connect_phase Float64,
    backend_process_phase Float64,
    backend_transfer_phase Float64,
    nginx_transfer_phase Float64,
    backend_total_phase Float64,
    network_phase Float64,
    processing_phase Float64,
    transfer_phase Float64,
    
    -- 性能效率指标
    response_transfer_speed Float64,
    total_transfer_speed Float64,
    nginx_transfer_speed Float64,
    backend_efficiency Float64,
    network_overhead Float64,
    transfer_ratio Float64,
    connection_cost_ratio Float64,
    processing_efficiency_index Float64,
    
    -- 业务维度
    platform LowCardinality(String),
    platform_version String CODEC(ZSTD(1)),
    device_type LowCardinality(String),
    browser_type LowCardinality(String),
    os_type LowCardinality(String),
    bot_type LowCardinality(String),
    entry_source LowCardinality(String),
    referer_domain String CODEC(ZSTD(1)),
    search_engine LowCardinality(String),
    social_media LowCardinality(String),
    api_category LowCardinality(String),
    application_name LowCardinality(String),
    service_name LowCardinality(String),
    
    -- 链路和集群
    trace_id String CODEC(ZSTD(1)),
    business_sign LowCardinality(String),
    cluster_node LowCardinality(String),
    upstream_server String CODEC(ZSTD(1)),
    connection_requests UInt32,
    cache_status LowCardinality(String),
    
    -- 原始字段
    referer_url String CODEC(ZSTD(1)),
    user_agent_string String CODEC(ZSTD(1)),
    log_source_file LowCardinality(String),
    
    -- 状态标识
    is_success Bool,
    is_slow Bool,
    is_error Bool,
    has_anomaly Bool,
    anomaly_type LowCardinality(String),
    data_quality_score Float64,
    
    -- 地理网络
    client_region LowCardinality(String),
    client_isp LowCardinality(String),
    ip_risk_level LowCardinality(String),
    is_internal_ip Bool,
    
    -- 时间维度字段
    date Date MATERIALIZED toDate(log_time),
    hour UInt8 MATERIALIZED toHour(log_time),
    minute UInt8 MATERIALIZED toMinute(log_time),
    second UInt8 MATERIALIZED toSecond(log_time),
    date_hour String MATERIALIZED concat(toString(date), '_', toString(hour)),
    date_hour_minute String MATERIALIZED concat(toString(date), '_', toString(hour), '_', toString(minute)),
    
    -- 元数据
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY date_partition  
ORDER BY (date_partition, hour_partition, api_category, platform, log_time)
SETTINGS index_granularity = 8192;

-- ============================================================
-- DWS层：数据仓库汇总层
-- ============================================================

-- 1. API性能分位数统计表
CREATE TABLE IF NOT EXISTS dws_api_performance_percentiles (
    log_date Date,
    hour_partition UInt8,
    request_uri_normalized String,
    platform LowCardinality(String),
    api_category LowCardinality(String),
    cluster_node LowCardinality(String),
    
    -- 请求统计
    total_requests UInt64,
    success_requests UInt64,
    error_requests UInt64,
    slow_requests UInt64,
    success_rate Float64,
    error_rate Float64,
    slow_rate Float64,
    
    -- 响应时间分位数
    response_time_avg Float64,
    response_time_p50 Float64,
    response_time_p90 Float64,
    response_time_p95 Float64,
    response_time_p99 Float64,
    response_time_max Float64,
    
    -- 阶段时间分位数
    backend_connect_p50 Float64,
    backend_connect_p95 Float64,
    backend_process_p50 Float64,
    backend_process_p95 Float64,
    backend_transfer_p50 Float64,
    backend_transfer_p95 Float64,
    nginx_transfer_p50 Float64,
    nginx_transfer_p95 Float64,
    
    -- 传输性能分位数
    transfer_speed_p50 Float64,
    transfer_speed_p95 Float64,
    response_size_p50 Float64,
    response_size_p95 Float64,
    
    -- 效率指标
    backend_efficiency_avg Float64,
    processing_efficiency_avg Float64,
    network_overhead_avg Float64,
    connection_reuse_rate Float64,
    cache_hit_rate Float64,
    
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY log_date
ORDER BY (log_date, hour_partition, request_uri_normalized, platform)
SETTINGS index_granularity = 8192;

-- 2. 实时QPS排行表
CREATE TABLE IF NOT EXISTS dws_realtime_qps_ranking (
    log_time DateTime,
    time_window UInt16,
    request_uri_normalized String,
    platform LowCardinality(String),
    api_category LowCardinality(String),
    
    -- QPS指标
    qps Float64,
    requests_count UInt64,
    avg_response_time Float64,
    p95_response_time Float64,
    error_rate Float64,
    
    -- 排名信息
    qps_rank UInt16,
    response_time_rank UInt16,
    is_hot_api Bool,
    is_slow_api Bool,
    
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toDate(log_time)
ORDER BY (log_time, time_window, qps_rank, request_uri_normalized)
TTL log_time + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;

-- 3. 错误监控汇总表
CREATE TABLE IF NOT EXISTS dws_error_monitoring (
    log_date Date,
    hour_partition UInt8,
    request_uri_normalized String,
    response_status_code LowCardinality(String),
    upstream_server String,
    platform LowCardinality(String),
    cluster_node LowCardinality(String),
    
    -- 错误统计
    error_count UInt64,
    total_requests UInt64,
    error_rate Float64,
    
    -- 错误时间分析
    first_error_time DateTime,
    last_error_time DateTime,
    error_duration_minutes UInt32,
    peak_error_time DateTime,
    
    -- 影响范围
    affected_clients_count UInt64,
    affected_apis_count UInt64,
    
    -- 错误特征
    avg_error_response_time Float64,
    most_common_error_code LowCardinality(String),
    upstream_error_rate Float64,
    
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY log_date
ORDER BY (log_date, hour_partition, error_rate, request_uri_normalized)
SETTINGS index_granularity = 8192;

-- 4. 上游服务健康监控表
CREATE TABLE IF NOT EXISTS dws_upstream_health_monitoring (
    log_date Date,
    hour_partition UInt8,
    upstream_server String,
    service_name LowCardinality(String),
    cluster_node LowCardinality(String),
    
    -- 健康指标
    total_requests UInt64,
    success_requests UInt64,
    error_requests UInt64,
    timeout_requests UInt64,
    
    -- 连接健康
    connect_success_rate Float64,
    avg_connect_time Float64,
    p95_connect_time Float64,
    connect_error_count UInt64,
    
    -- 响应健康  
    avg_response_time Float64,
    p95_response_time Float64,
    p99_response_time Float64,
    response_timeout_rate Float64,
    
    -- 服务状态评估
    health_score Float64,
    availability Float64,
    is_healthy Bool,
    alert_level LowCardinality(String),
    
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY log_date
ORDER BY (log_date, hour_partition, health_score, upstream_server)
SETTINGS index_granularity = 8192;

-- 5. 客户端行为分析表
CREATE TABLE IF NOT EXISTS dws_client_behavior_analysis (
    log_date Date,
    hour_partition UInt8,
    client_ip String,
    platform LowCardinality(String),
    device_type LowCardinality(String),
    browser_type LowCardinality(String),
    os_type LowCardinality(String),
    entry_source LowCardinality(String),
    
    -- 行为统计
    session_requests UInt64,
    unique_apis_accessed UInt64,
    avg_session_duration Float64,
    total_data_consumed_kb Float64,
    
    -- 性能表现
    avg_response_time Float64,
    p95_response_time Float64,
    slow_requests_count UInt64,
    error_requests_count UInt64,
    
    -- 访问模式
    peak_hour UInt8,
    request_frequency Float64,
    most_accessed_api String,
    
    -- 风险评估
    risk_score Float64,
    is_suspicious Bool,
    bot_probability Float64,
    
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY log_date
ORDER BY (log_date, hour_partition, risk_score, client_ip)
SETTINGS index_granularity = 8192;

-- 6. 业务链路追踪汇总表
CREATE TABLE IF NOT EXISTS dws_trace_analysis (
    log_date Date,
    trace_id String,
    business_sign LowCardinality(String),
    
    -- 链路统计
    total_spans UInt32,
    service_count UInt16,
    error_spans UInt32,
    
    -- 链路时间分析
    total_trace_duration Float64,
    critical_path_duration Float64,
    avg_span_duration Float64,
    p95_span_duration Float64,
    
    -- 链路健康
    success_rate Float64,
    error_rate Float64,
    bottleneck_service LowCardinality(String),
    slowest_api String,
    
    -- 业务标识
    start_time DateTime,
    end_time DateTime,
    entry_api String,
    
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY log_date
ORDER BY (log_date, business_sign, total_trace_duration)
SETTINGS index_granularity = 8192;

-- ============================================================
-- ADS层：应用数据服务层
-- ============================================================

-- 1. TOP慢接口排行榜
CREATE TABLE IF NOT EXISTS ads_top_slow_apis (
    ranking_time DateTime,
    ranking_period LowCardinality(String),
    rank_position UInt16,
    request_uri_normalized String,
    platform LowCardinality(String),
    
    -- 性能指标
    avg_response_time Float64,
    p95_response_time Float64,
    p99_response_time Float64,
    requests_count UInt64,
    slow_requests_ratio Float64,
    
    -- 变化趋势
    response_time_trend Float64,
    rank_change Int16,
    
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (ranking_time, ranking_period, rank_position)
TTL ranking_time + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- 2. TOP热点接口排行榜
CREATE TABLE IF NOT EXISTS ads_top_hot_apis (
    ranking_time DateTime,
    ranking_period LowCardinality(String),
    rank_position UInt16,
    request_uri_normalized String,
    platform LowCardinality(String),
    
    -- 热度指标
    qps Float64,
    requests_count UInt64,
    traffic_share Float64,
    
    -- 性能保障
    avg_response_time Float64,
    error_rate Float64,
    availability Float64,
    
    -- 变化趋势
    qps_trend Float64,
    rank_change Int16,
    
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (ranking_time, ranking_period, rank_position)
TTL ranking_time + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;

-- 3. 集群性能对比表
CREATE TABLE IF NOT EXISTS ads_cluster_performance_comparison (
    comparison_time DateTime,
    time_period LowCardinality(String),
    cluster_node LowCardinality(String),
    
    -- 性能指标
    total_qps Float64,
    avg_response_time Float64,
    p95_response_time Float64,
    error_rate Float64,
    success_rate Float64,
    
    -- 资源指标
    cpu_usage Float64,
    memory_usage Float64,
    connection_pool_usage Float64,
    cache_hit_rate Float64,
    
    -- 排名和对比
    performance_rank UInt16,
    performance_score Float64,
    vs_avg_performance Float64,
    
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (comparison_time, time_period, performance_rank)
TTL comparison_time + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- 4. 缓存命中率分析表
CREATE TABLE IF NOT EXISTS ads_cache_hit_analysis (
    analysis_time DateTime,
    time_period LowCardinality(String),
    cache_layer LowCardinality(String),
    api_category LowCardinality(String),
    
    -- 缓存统计
    total_requests UInt64,
    cache_hits UInt64,
    cache_misses UInt64,
    cache_bypasses UInt64,
    hit_rate Float64,
    miss_rate Float64,
    
    -- 性能影响
    hit_avg_response_time Float64,
    miss_avg_response_time Float64,
    performance_improvement Float64,
    
    -- 缓存效率
    cache_size_mb Float64,
    cache_utilization Float64,
    eviction_rate Float64,
    
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (analysis_time, time_period, hit_rate)
TTL analysis_time + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- ============================================================
-- 简化索引（避免字段不存在错误）
-- ============================================================

-- DWD表基础索引（使用确定存在的字段）
ALTER TABLE dwd_nginx_enriched ADD INDEX idx_api_category (api_category) TYPE minmax GRANULARITY 3;
ALTER TABLE dwd_nginx_enriched ADD INDEX idx_platform (platform) TYPE minmax GRANULARITY 3;
ALTER TABLE dwd_nginx_enriched ADD INDEX idx_response_status (response_status_code) TYPE minmax GRANULARITY 3;

-- ============================================================
-- TTL策略
-- ============================================================

-- ODS层数据保留90天
ALTER TABLE ods_nginx_raw MODIFY TTL date_partition + INTERVAL 90 DAY;

-- DWD层数据保留365天
ALTER TABLE dwd_nginx_enriched MODIFY TTL date_partition + INTERVAL 365 DAY;

-- DWS层数据保留策略
ALTER TABLE dws_api_performance_percentiles MODIFY TTL log_date + INTERVAL 1095 DAY;
ALTER TABLE dws_error_monitoring MODIFY TTL log_date + INTERVAL 365 DAY;