-- 创建ODS层表结构
CREATE TABLE IF NOT EXISTS nginx_analytics.ods_nginx_log (
    id UInt64,
    timestamp DateTime,
    client_ip String,
    request_method LowCardinality(String),
    request_full_uri String,
    request_protocol LowCardinality(String),
    response_status_code LowCardinality(String),
    response_body_size_kb Float64,
    total_bytes_sent_kb Float64,
    referer String,
    user_agent String,
    total_request_duration Float64,
    upstream_response_time Float64,
    upstream_connect_time Float64,
    upstream_header_time Float64,
    application_name LowCardinality(String),
    service_name LowCardinality(String),
    source_file String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (timestamp, client_ip)
PARTITION BY toYYYYMM(timestamp)
SETTINGS index_granularity = 8192;

-- 创建DWD层表结构
CREATE TABLE IF NOT EXISTS nginx_analytics.dwd_nginx_enriched (
    id UInt64,
    ods_id UInt64,
    timestamp DateTime,
    date_partition String,
    hour_partition UInt8,
    client_ip String,
    request_uri String,
    request_method LowCardinality(String),
    response_status_code LowCardinality(String),
    response_time Float64,
    response_size_kb Float64,
    platform LowCardinality(String),
    platform_version String,
    entry_source LowCardinality(String),
    api_category LowCardinality(String),
    application_name LowCardinality(String),
    service_name LowCardinality(String),
    is_success Bool,
    is_slow Bool,
    data_quality_score Float64,
    has_anomaly Bool,
    anomaly_type LowCardinality(String),
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (timestamp, platform, api_category)
PARTITION BY toYYYYMM(timestamp)
SETTINGS index_granularity = 8192;

-- 创建DWS层聚合表(按小时聚合)
CREATE TABLE IF NOT EXISTS nginx_analytics.dws_platform_hourly (
    date_partition String,
    hour_partition UInt8,
    platform LowCardinality(String),
    total_requests UInt64,
    success_requests UInt64,
    error_requests UInt64,
    slow_requests UInt64,
    avg_response_time Float64,
    p50_response_time Float64,
    p95_response_time Float64,
    p99_response_time Float64,
    max_response_time Float64,
    success_rate Float64,
    error_rate Float64,
    slow_rate Float64,
    unique_ips UInt64,
    total_response_size_mb Float64,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
ORDER BY (date_partition, hour_partition, platform)
PARTITION BY toYYYYMM(toDate(date_partition));

-- 创建API维度聚合表
CREATE TABLE IF NOT EXISTS nginx_analytics.dws_api_hourly (
    date_partition String,
    hour_partition UInt8,
    request_uri String,
    platform LowCardinality(String),
    api_category LowCardinality(String),
    total_requests UInt64,
    success_requests UInt64,
    error_requests UInt64,
    slow_requests UInt64,
    avg_response_time Float64,
    p95_response_time Float64,
    max_response_time Float64,
    success_rate Float64,
    error_rate Float64,
    slow_rate Float64,
    application_name LowCardinality(String),
    service_name LowCardinality(String),
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
ORDER BY (date_partition, hour_partition, request_uri, platform)
PARTITION BY toYYYYMM(toDate(date_partition));

-- 创建ADS层异常检测表
CREATE TABLE IF NOT EXISTS nginx_analytics.ads_anomaly_log (
    id UInt64,
    anomaly_time DateTime,
    anomaly_type LowCardinality(String),
    severity LowCardinality(String),
    platform LowCardinality(String),
    request_uri String,
    current_value Float64,
    baseline_value Float64,
    deviation_ratio Float64,
    description String,
    suggestion String,
    status LowCardinality(String) DEFAULT 'open',
    resolved_at DateTime,
    resolved_by String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (anomaly_time, severity, anomaly_type)
PARTITION BY toYYYYMM(anomaly_time)
SETTINGS index_granularity = 8192;