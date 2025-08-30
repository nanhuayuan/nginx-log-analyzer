-- ClickHouse完整分层架构表结构
-- 创建处理状态表
CREATE TABLE IF NOT EXISTS nginx_analytics.processing_status (
    process_id String,
    log_date Date,
    log_file_path String,
    file_hash String,
    file_size UInt64,
    processed_records UInt64,
    processing_start_time DateTime,
    processing_end_time DateTime,
    status LowCardinality(String), -- 'processing', 'completed', 'failed'
    error_message String,
    processor_version String DEFAULT 'v1.0.0',
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (log_date, process_id)
PARTITION BY toYYYYMM(log_date)
SETTINGS index_granularity = 8192;

-- 创建DWS层按小时聚合表
CREATE TABLE IF NOT EXISTS nginx_analytics.dws_nginx_hourly (
    date_partition String,
    hour_partition UInt8,
    platform LowCardinality(String),
    entry_source LowCardinality(String),
    api_category LowCardinality(String),
    total_requests UInt64,
    success_requests UInt64,
    error_requests UInt64,
    slow_requests UInt64,
    anomaly_requests UInt64,
    avg_response_time Float64,
    p50_response_time Float64,
    p95_response_time Float64,
    p99_response_time Float64,
    max_response_time Float64,
    min_response_time Float64,
    success_rate Float64,
    error_rate Float64,
    slow_rate Float64,
    anomaly_rate Float64,
    unique_ips UInt64,
    total_response_size_mb Float64,
    avg_response_size_kb Float64,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
ORDER BY (date_partition, hour_partition, platform, entry_source, api_category)
PARTITION BY toYYYYMM(toDate(date_partition));

-- 创建DWS层按天聚合表
CREATE TABLE IF NOT EXISTS nginx_analytics.dws_nginx_daily (
    log_date Date,
    platform LowCardinality(String),
    entry_source LowCardinality(String),
    api_category LowCardinality(String),
    total_requests UInt64,
    success_requests UInt64,
    error_requests UInt64,
    slow_requests UInt64,
    anomaly_requests UInt64,
    avg_response_time Float64,
    p50_response_time Float64,
    p95_response_time Float64,
    p99_response_time Float64,
    max_response_time Float64,
    min_response_time Float64,
    success_rate Float64,
    error_rate Float64,
    slow_rate Float64,
    anomaly_rate Float64,
    unique_ips UInt64,
    total_response_size_mb Float64,
    avg_response_size_kb Float64,
    top_apis Array(String),
    top_error_apis Array(String),
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
ORDER BY (log_date, platform, entry_source, api_category)
PARTITION BY toYYYYMM(log_date);

-- 创建ADS层性能指标表
CREATE TABLE IF NOT EXISTS nginx_analytics.ads_performance_metrics (
    metric_time DateTime,
    metric_type LowCardinality(String), -- 'realtime', 'hourly', 'daily'
    platform LowCardinality(String),
    total_requests UInt64,
    success_rate Float64,
    avg_response_time Float64,
    p95_response_time Float64,
    p99_response_time Float64,
    slow_request_rate Float64,
    error_rate Float64,
    anomaly_score Float64,
    alert_level LowCardinality(String), -- 'normal', 'warning', 'critical'
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (metric_time, metric_type, platform)
PARTITION BY toYYYYMM(metric_time);

-- 创建ADS层异常检测表(扩展原有)
ALTER TABLE nginx_analytics.ads_anomaly_log 
ADD COLUMN IF NOT EXISTS detection_algorithm LowCardinality(String) DEFAULT 'threshold',
ADD COLUMN IF NOT EXISTS confidence_score Float64 DEFAULT 0.0,
ADD COLUMN IF NOT EXISTS impact_level LowCardinality(String) DEFAULT 'low';

-- 创建实时指标物化视图
CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_realtime_metrics
TO nginx_analytics.ads_performance_metrics
AS SELECT
    toStartOfHour(timestamp) as metric_time,
    'hourly' as metric_type,
    platform,
    count() as total_requests,
    avg(if(is_success, 1, 0)) * 100 as success_rate,
    avg(response_time) as avg_response_time,
    quantile(0.95)(response_time) as p95_response_time,
    quantile(0.99)(response_time) as p99_response_time,
    avg(if(is_slow, 1, 0)) * 100 as slow_request_rate,
    avg(if(is_success, 0, 1)) * 100 as error_rate,
    avg(if(has_anomaly, 1, 0)) * 100 as anomaly_score,
    if(avg(response_time) > 1.0, 'warning', 
       if(avg(response_time) > 3.0, 'critical', 'normal')) as alert_level,
    now() as created_at
FROM nginx_analytics.dwd_nginx_enriched
GROUP BY toStartOfHour(timestamp), platform;

-- 创建API性能视图
CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_api_performance
TO nginx_analytics.dws_api_hourly
AS SELECT
    formatDateTime(toStartOfHour(timestamp), '%Y-%m-%d') as date_partition,
    toHour(timestamp) as hour_partition,
    request_uri,
    platform,
    api_category,
    count() as total_requests,
    countIf(is_success) as success_requests,
    countIf(not is_success) as error_requests,
    countIf(is_slow) as slow_requests,
    avg(response_time) as avg_response_time,
    quantile(0.95)(response_time) as p95_response_time,
    max(response_time) as max_response_time,
    if(count() > 0, success_requests * 100.0 / count(), 0) as success_rate,
    if(count() > 0, error_requests * 100.0 / count(), 0) as error_rate,
    if(count() > 0, slow_requests * 100.0 / count(), 0) as slow_rate,
    application_name,
    service_name,
    now() as created_at,
    now() as updated_at
FROM nginx_analytics.dwd_nginx_enriched
GROUP BY 
    formatDateTime(toStartOfHour(timestamp), '%Y-%m-%d'),
    toHour(timestamp),
    request_uri,
    platform,
    api_category,
    application_name,
    service_name;

-- 创建平台分析视图
CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_platform_analysis
TO nginx_analytics.dws_nginx_hourly
AS SELECT
    formatDateTime(toStartOfHour(timestamp), '%Y-%m-%d') as date_partition,
    toHour(timestamp) as hour_partition,
    platform,
    entry_source,
    api_category,
    count() as total_requests,
    countIf(is_success) as success_requests,
    countIf(not is_success) as error_requests,
    countIf(is_slow) as slow_requests,
    countIf(has_anomaly) as anomaly_requests,
    avg(response_time) as avg_response_time,
    quantile(0.5)(response_time) as p50_response_time,
    quantile(0.95)(response_time) as p95_response_time,
    quantile(0.99)(response_time) as p99_response_time,
    max(response_time) as max_response_time,
    min(response_time) as min_response_time,
    if(count() > 0, success_requests * 100.0 / count(), 0) as success_rate,
    if(count() > 0, error_requests * 100.0 / count(), 0) as error_rate,
    if(count() > 0, slow_requests * 100.0 / count(), 0) as slow_rate,
    if(count() > 0, anomaly_requests * 100.0 / count(), 0) as anomaly_rate,
    uniq(client_ip) as unique_ips,
    sum(response_size_kb) / 1024 as total_response_size_mb,
    avg(response_size_kb) as avg_response_size_kb,
    now() as created_at,
    now() as updated_at
FROM nginx_analytics.dwd_nginx_enriched
GROUP BY 
    formatDateTime(toStartOfHour(timestamp), '%Y-%m-%d'),
    toHour(timestamp),
    platform,
    entry_source,
    api_category;