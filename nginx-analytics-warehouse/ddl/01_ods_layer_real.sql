-- ==========================================
-- 基于实际nginx日志格式的ODS层设计
-- 参考: sample_nginx_logs\2025-04-23\access186.log
-- ==========================================

-- ODS原始日志表 - 保持与现有schema_design_v2_fixed.sql一致
CREATE TABLE IF NOT EXISTS ods_nginx_raw (
    id UInt64,
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

-- 样例数据格式参考:
-- http_host:zgtapp.zwfw.gxzf.gov.cn remote_addr:"100.100.8.44" remote_port:"10305" 
-- remote_user:"-" time:"2025-04-23T00:00:02+08:00" 
-- request:"GET /group1/M00/06/B3/rBAWN2f-ZIKAJI2vAAIkLKrgt-I560.png HTTP/1.1" 
-- code:"200" body:"140332" http_referer:"-" ar_time:"0.325" RealIp:"100.100.8.44" 
-- agent:"zgt-ios/1.4.1 (iPhone; iOS 15.4.1; Scale/3.00)"

COMMENT ON TABLE ods_nginx_raw IS 'Nginx原始日志ODS层，基于实际日志格式设计，保持数据完整性';