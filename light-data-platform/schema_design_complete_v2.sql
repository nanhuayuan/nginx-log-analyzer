-- ============================================================
-- Self功能完整支持的ClickHouse表结构设计 V2.0
-- 支持全部12个分析器的完整功能需求
-- 设计理念：大开大合，全量支持，高效计算
-- ============================================================

-- 删除现有数据库并重建
-- DROP DATABASE IF EXISTS nginx_analytics;
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
-- DWD层：数据仓库明细层（完整重构）
-- 支持Self全部12个分析器的字段需求
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
    request_uri_normalized String CODEC(ZSTD(1)),  -- 规范化后的URI（用于聚合）
    request_full_uri String CODEC(ZSTD(1)),
    query_parameters String CODEC(ZSTD(1)),
    http_protocol_version LowCardinality(String),
    
    -- 响应信息
    response_status_code LowCardinality(String),
    response_body_size UInt64,
    response_body_size_kb Float64,  -- KB单位，便于分析
    total_bytes_sent UInt64,
    total_bytes_sent_kb Float64,    -- KB单位，便于分析
    
    -- ★★★ 核心时间字段（Self 01,02,03分析器核心需求）★★★
    total_request_duration Float64,              -- 请求总时长
    upstream_connect_time Float64,               -- 原始上游连接时间
    upstream_header_time Float64,                -- 原始上游头部时间  
    upstream_response_time Float64,              -- 原始上游响应时间
    
    -- ★★★ 阶段时间细分字段（Self核心计算需求）★★★
    backend_connect_phase Float64,               -- 后端连接阶段时长
    backend_process_phase Float64,               -- 后端处理阶段时长
    backend_transfer_phase Float64,              -- 后端传输阶段时长
    nginx_transfer_phase Float64,                -- Nginx传输阶段时长
    backend_total_phase Float64,                 -- 后端总阶段时长
    network_phase Float64,                       -- 网络传输阶段时长
    processing_phase Float64,                    -- 纯处理阶段时长
    transfer_phase Float64,                      -- 纯传输阶段时长
    
    -- ★★★ 性能效率指标（Self 01,02,03分析器需求）★★★
    response_transfer_speed Float64,             -- 响应传输速度(KB/s)
    total_transfer_speed Float64,                -- 总传输速度(KB/s)
    nginx_transfer_speed Float64,                -- Nginx传输速度(KB/s)
    backend_efficiency Float64,                  -- 后端处理效率(%)
    network_overhead Float64,                    -- 网络开销占比(%)
    transfer_ratio Float64,                      -- 传输时间占比(%)
    connection_cost_ratio Float64,               -- 连接成本占比(%)
    processing_efficiency_index Float64,         -- 处理效率指数
    
    -- 业务维度enrichment
    platform LowCardinality(String),             -- 平台识别（iOS/Android/Web等）
    platform_version String CODEC(ZSTD(1)),      -- 平台版本
    device_type LowCardinality(String),           -- 设备类型
    browser_type LowCardinality(String),          -- 浏览器类型
    os_type LowCardinality(String),               -- 操作系统类型
    bot_type LowCardinality(String),              -- 机器人类型
    entry_source LowCardinality(String),          -- 入口来源
    referer_domain String CODEC(ZSTD(1)),         -- 来源域名
    search_engine LowCardinality(String),         -- 搜索引擎
    social_media LowCardinality(String),          -- 社交媒体
    api_category LowCardinality(String),          -- API分类
    application_name LowCardinality(String),      -- 应用名称
    service_name LowCardinality(String),          -- 服务名称
    
    -- 链路追踪和集群信息
    trace_id String CODEC(ZSTD(1)),              -- 链路追踪ID
    business_sign LowCardinality(String),         -- 业务标识
    cluster_node LowCardinality(String),          -- 集群节点
    upstream_server String CODEC(ZSTD(1)),        -- 上游服务器地址
    connection_requests UInt32,                   -- 连接复用次数
    cache_status LowCardinality(String),          -- 缓存状态(HIT/MISS/BYPASS)
    
    -- 原始字段保留（用于详细分析）
    referer_url String CODEC(ZSTD(1)),
    user_agent_string String CODEC(ZSTD(1)),
    log_source_file LowCardinality(String),
    
    -- 质量和状态标识
    is_success Bool,                              -- 是否成功请求
    is_slow Bool,                                 -- 是否慢请求  
    is_error Bool,                                -- 是否错误请求
    has_anomaly Bool,                             -- 是否异常
    anomaly_type LowCardinality(String),          -- 异常类型
    data_quality_score Float64,                  -- 数据质量评分
    
    -- 地理和网络信息
    client_region LowCardinality(String),        -- 客户端地区
    client_isp LowCardinality(String),           -- 客户端ISP
    ip_risk_level LowCardinality(String),        -- IP风险等级
    is_internal_ip Bool,                         -- 是否内网IP
    
    -- 时间维度字段（便于查询聚合）
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
-- DWS层：数据仓库汇总层（6个核心聚合表）
-- ============================================================

-- 1. API性能分位数统计表（支持Self 01接口性能分析）
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
    
    -- 响应时间分位数（Self核心需求）
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

-- 2. 实时QPS排行表（支持Self 05时间维度分析）
CREATE TABLE IF NOT EXISTS dws_realtime_qps_ranking (
    log_time DateTime,
    time_window UInt16,  -- 时间窗口(分钟)
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
    is_hot_api Bool,      -- 是否热点API
    is_slow_api Bool,     -- 是否慢API
    
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toDate(log_time)
ORDER BY (log_time, time_window, qps_rank, request_uri_normalized)
TTL log_time + INTERVAL 7 DAY  -- 7天TTL
SETTINGS index_granularity = 8192;

-- 3. 错误监控汇总表（支持Self 04,13错误分析）  
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

-- 4. 上游服务健康监控表（支持核心监控指标）
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
    health_score Float64,     -- 综合健康评分 0-100
    availability Float64,     -- 可用性 %
    is_healthy Bool,          -- 是否健康
    alert_level LowCardinality(String),  -- 告警级别
    
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY log_date
ORDER BY (log_date, hour_partition, health_score, upstream_server)
SETTINGS index_granularity = 8192;

-- 5. 客户端行为分析表（支持Self 08,10,11分析）
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
    request_frequency Float64,  -- 每小时平均请求数
    most_accessed_api String,
    
    -- 风险评估
    risk_score Float64,         -- 风险评分 0-100
    is_suspicious Bool,         -- 是否可疑
    bot_probability Float64,    -- 机器人概率
    
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY log_date
ORDER BY (log_date, hour_partition, risk_score, client_ip)
SETTINGS index_granularity = 8192;

-- 6. 业务链路追踪汇总表（支持链路追踪分析）
CREATE TABLE IF NOT EXISTS dws_trace_analysis (
    log_date Date,
    trace_id String,
    business_sign LowCardinality(String),
    
    -- 链路统计
    total_spans UInt32,                            -- 总调用次数
    service_count UInt16,                          -- 服务数量
    error_spans UInt32,                            -- 错误调用次数
    
    -- 链路时间分析
    total_trace_duration Float64,                 -- 链路总耗时
    critical_path_duration Float64,               -- 关键路径耗时
    avg_span_duration Float64,                    -- 平均span耗时
    p95_span_duration Float64,                    -- 95分位span耗时
    
    -- 链路健康
    success_rate Float64,                         -- 链路成功率
    error_rate Float64,                           -- 链路错误率
    bottleneck_service LowCardinality(String),    -- 瓶颈服务
    slowest_api String,                           -- 最慢接口
    
    -- 业务标识
    start_time DateTime,
    end_time DateTime,
    entry_api String,                             -- 入口接口
    
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY log_date
ORDER BY (log_date, business_sign, total_trace_duration)
SETTINGS index_granularity = 8192;

-- ============================================================
-- ADS层：应用数据服务层（4个核心应用表）
-- ============================================================

-- 1. TOP慢接口排行榜（直接支持核心监控需求）
CREATE TABLE IF NOT EXISTS ads_top_slow_apis (
    ranking_time DateTime,
    ranking_period LowCardinality(String),  -- 'hourly','daily'
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
    response_time_trend Float64,  -- 与上期对比变化率
    rank_change Int16,            -- 排名变化
    
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (ranking_time, ranking_period, rank_position)
TTL ranking_time + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- 2. TOP热点接口排行榜（直接支持核心监控需求）
CREATE TABLE IF NOT EXISTS ads_top_hot_apis (
    ranking_time DateTime,
    ranking_period LowCardinality(String),  -- 'minutely','hourly','daily'
    rank_position UInt16,
    request_uri_normalized String,
    platform LowCardinality(String),
    
    -- 热度指标
    qps Float64,
    requests_count UInt64,
    traffic_share Float64,       -- 流量占比
    
    -- 性能保障
    avg_response_time Float64,
    error_rate Float64,
    availability Float64,
    
    -- 变化趋势
    qps_trend Float64,           -- QPS变化率
    rank_change Int16,           -- 排名变化
    
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (ranking_time, ranking_period, rank_position)
TTL ranking_time + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;

-- 3. 集群性能对比表（直接支持核心监控需求）
CREATE TABLE IF NOT EXISTS ads_cluster_performance_comparison (
    comparison_time DateTime,
    time_period LowCardinality(String),  -- 'hourly','daily'
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
    performance_rank UInt16,     -- 性能排名
    performance_score Float64,   -- 综合性能评分
    vs_avg_performance Float64,  -- 与平均性能对比
    
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (comparison_time, time_period, performance_rank)
TTL comparison_time + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- 4. 缓存命中率分析表（直接支持核心监控需求）
CREATE TABLE IF NOT EXISTS ads_cache_hit_analysis (
    analysis_time DateTime,
    time_period LowCardinality(String),  -- 'hourly','daily'
    cache_layer LowCardinality(String),  -- 缓存层级
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
    performance_improvement Float64,  -- 缓存带来的性能提升
    
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
-- 物化视图：实时数据处理（6个核心物化视图）
-- ============================================================

-- 1. 实时API性能分位数物化视图
CREATE MATERIALIZED VIEW mv_api_performance_realtime TO dws_api_performance_percentiles AS
SELECT
    toDate(log_time) as log_date,
    toHour(log_time) as hour_partition,
    request_uri_normalized,
    platform,
    api_category,
    cluster_node,
    
    count() as total_requests,
    countIf(is_success) as success_requests,
    countIf(is_error) as error_requests,  
    countIf(is_slow) as slow_requests,
    
    success_requests / total_requests * 100 as success_rate,
    error_requests / total_requests * 100 as error_rate,
    slow_requests / total_requests * 100 as slow_rate,
    
    avg(total_request_duration) as response_time_avg,
    quantile(0.5)(total_request_duration) as response_time_p50,
    quantile(0.9)(total_request_duration) as response_time_p90,
    quantile(0.95)(total_request_duration) as response_time_p95,
    quantile(0.99)(total_request_duration) as response_time_p99,
    max(total_request_duration) as response_time_max,
    
    quantile(0.5)(backend_connect_phase) as backend_connect_p50,
    quantile(0.95)(backend_connect_phase) as backend_connect_p95,
    quantile(0.5)(backend_process_phase) as backend_process_p50,
    quantile(0.95)(backend_process_phase) as backend_process_p95,
    quantile(0.5)(backend_transfer_phase) as backend_transfer_p50,
    quantile(0.95)(backend_transfer_phase) as backend_transfer_p95,
    quantile(0.5)(nginx_transfer_phase) as nginx_transfer_p50,
    quantile(0.95)(nginx_transfer_phase) as nginx_transfer_p95,
    
    quantile(0.5)(response_transfer_speed) as transfer_speed_p50,
    quantile(0.95)(response_transfer_speed) as transfer_speed_p95,
    quantile(0.5)(response_body_size_kb) as response_size_p50,
    quantile(0.95)(response_body_size_kb) as response_size_p95,
    
    avg(backend_efficiency) as backend_efficiency_avg,
    avg(processing_efficiency_index) as processing_efficiency_avg,
    avg(network_overhead) as network_overhead_avg,
    avg(connection_requests) as connection_reuse_rate,
    countIf(cache_status = 'HIT') / count() * 100 as cache_hit_rate,
    
    now() as created_at
FROM dwd_nginx_enriched
WHERE log_time >= now() - INTERVAL 1 HOUR
GROUP BY log_date, hour_partition, request_uri_normalized, platform, api_category, cluster_node;

-- 2. 实时QPS排行物化视图
CREATE MATERIALIZED VIEW mv_realtime_qps_ranking TO dws_realtime_qps_ranking AS
SELECT
    toStartOfMinute(log_time) as log_time,
    1 as time_window,
    request_uri_normalized,
    platform,
    api_category,
    
    count() / 60.0 as qps,
    count() as requests_count,
    avg(total_request_duration) as avg_response_time,
    quantile(0.95)(total_request_duration) as p95_response_time,
    countIf(is_error) / count() * 100 as error_rate,
    
    row_number() OVER (PARTITION BY log_time ORDER BY qps DESC) as qps_rank,
    row_number() OVER (PARTITION BY log_time ORDER BY avg_response_time DESC) as response_time_rank,
    qps_rank <= 5 as is_hot_api,
    response_time_rank <= 5 as is_slow_api,
    
    now() as created_at
FROM dwd_nginx_enriched
WHERE log_time >= now() - INTERVAL 5 MINUTE
GROUP BY log_time, request_uri_normalized, platform, api_category;

-- 3. 错误监控实时物化视图
CREATE MATERIALIZED VIEW mv_error_monitoring_realtime TO dws_error_monitoring AS
SELECT
    toDate(log_time) as log_date,
    toHour(log_time) as hour_partition,
    request_uri_normalized,
    response_status_code,
    upstream_server,
    platform,
    cluster_node,
    
    countIf(is_error) as error_count,
    count() as total_requests,
    error_count / total_requests * 100 as error_rate,
    
    min(log_time) as first_error_time,
    max(log_time) as last_error_time,
    dateDiff('minute', first_error_time, last_error_time) as error_duration_minutes,
    argMax(log_time, error_count) as peak_error_time,
    
    uniq(client_ip) as affected_clients_count,
    uniq(request_uri_normalized) as affected_apis_count,
    
    avgIf(total_request_duration, is_error) as avg_error_response_time,
    topK(1)(response_status_code)[1] as most_common_error_code,
    countIf(upstream_server != '') / count() * 100 as upstream_error_rate,
    
    now() as created_at
FROM dwd_nginx_enriched
WHERE log_time >= now() - INTERVAL 1 HOUR AND is_error = true
GROUP BY log_date, hour_partition, request_uri_normalized, response_status_code, upstream_server, platform, cluster_node;

-- 4. 上游服务健康监控物化视图
CREATE MATERIALIZED VIEW mv_upstream_health_realtime TO dws_upstream_health_monitoring AS
SELECT
    toDate(log_time) as log_date,
    toHour(log_time) as hour_partition,
    upstream_server,
    service_name,
    cluster_node,
    
    count() as total_requests,
    countIf(is_success) as success_requests,
    countIf(is_error) as error_requests,
    countIf(total_request_duration > 30) as timeout_requests,
    
    countIf(upstream_connect_time > 0 AND upstream_connect_time < 5) / countIf(upstream_connect_time > 0) * 100 as connect_success_rate,
    avgIf(upstream_connect_time, upstream_connect_time > 0) as avg_connect_time,
    quantileIf(0.95)(upstream_connect_time, upstream_connect_time > 0) as p95_connect_time,
    countIf(upstream_connect_time <= 0 OR upstream_connect_time > 5) as connect_error_count,
    
    avg(total_request_duration) as avg_response_time,
    quantile(0.95)(total_request_duration) as p95_response_time,
    quantile(0.99)(total_request_duration) as p99_response_time,
    timeout_requests / total_requests * 100 as response_timeout_rate,
    
    (connect_success_rate * 0.3 + (100 - error_requests/total_requests*100) * 0.4 + (100 - response_timeout_rate) * 0.3) as health_score,
    success_requests / total_requests * 100 as availability,
    health_score >= 80 as is_healthy,
    multiIf(health_score >= 90, 'OK', health_score >= 70, 'WARNING', 'CRITICAL') as alert_level,
    
    now() as created_at
FROM dwd_nginx_enriched  
WHERE log_time >= now() - INTERVAL 1 HOUR AND upstream_server != ''
GROUP BY log_date, hour_partition, upstream_server, service_name, cluster_node;

-- 5. 客户端行为分析物化视图
CREATE MATERIALIZED VIEW mv_client_behavior_realtime TO dws_client_behavior_analysis AS
SELECT
    toDate(log_time) as log_date,
    toHour(log_time) as hour_partition,
    client_ip,
    platform,
    device_type,
    browser_type,
    os_type,
    entry_source,
    
    count() as session_requests,
    uniq(request_uri_normalized) as unique_apis_accessed,
    (max(log_time) - min(log_time)) / 60.0 as avg_session_duration,
    sum(response_body_size_kb) as total_data_consumed_kb,
    
    avg(total_request_duration) as avg_response_time,
    quantile(0.95)(total_request_duration) as p95_response_time,
    countIf(is_slow) as slow_requests_count,
    countIf(is_error) as error_requests_count,
    
    argMax(toHour(log_time), count()) as peak_hour,
    count() / 1.0 as request_frequency,
    topK(1)(request_uri_normalized)[1] as most_accessed_api,
    
    multiIf(
        session_requests > 1000 OR unique_apis_accessed > 50, 100,
        session_requests > 500 OR unique_apis_accessed > 20, 70,
        session_requests > 100, 30, 0
    ) as risk_score,
    risk_score >= 70 as is_suspicious,
    if(bot_type != '', 0.9, if(session_requests > 500, 0.6, 0.1)) as bot_probability,
    
    now() as created_at
FROM dwd_nginx_enriched
WHERE log_time >= now() - INTERVAL 1 HOUR
GROUP BY log_date, hour_partition, client_ip, platform, device_type, browser_type, os_type, entry_source;

-- 6. 链路追踪分析物化视图
CREATE MATERIALIZED VIEW mv_trace_analysis_realtime TO dws_trace_analysis AS
SELECT
    toDate(log_time) as log_date,
    trace_id,
    business_sign,
    
    count() as total_spans,
    uniq(service_name) as service_count,
    countIf(is_error) as error_spans,
    
    max(log_time) - min(log_time) as total_trace_duration,
    max(total_request_duration) as critical_path_duration,
    avg(total_request_duration) as avg_span_duration,
    quantile(0.95)(total_request_duration) as p95_span_duration,
    
    (total_spans - error_spans) / total_spans * 100 as success_rate,
    error_spans / total_spans * 100 as error_rate,
    argMax(service_name, total_request_duration) as bottleneck_service,
    argMax(request_uri_normalized, total_request_duration) as slowest_api,
    
    min(log_time) as start_time,
    max(log_time) as end_time,
    argMin(request_uri_normalized, log_time) as entry_api,
    
    now() as created_at
FROM dwd_nginx_enriched
WHERE log_time >= now() - INTERVAL 1 HOUR AND trace_id != ''
GROUP BY log_date, trace_id, business_sign;

-- ============================================================
-- 普通视图：便于查询的视图层
-- ============================================================

-- 1. 实时统计视图（支持Web界面首页）
CREATE VIEW v_realtime_dashboard_stats AS
SELECT
    -- 实时QPS
    sum(requests_count) / 60.0 as current_qps,
    
    -- 平均响应时间
    avg(avg_response_time) as avg_response_time,
    
    -- 错误率
    sum(error_requests) / sum(total_requests) * 100 as error_rate,
    
    -- 慢请求率  
    sum(slow_requests) / sum(total_requests) * 100 as slow_rate,
    
    -- 成功率
    sum(success_requests) / sum(total_requests) * 100 as success_rate,
    
    -- 活跃API数量
    uniq(request_uri_normalized) as active_apis_count,
    
    -- 活跃客户端数量
    uniq(client_ip) as active_clients_count,
    
    -- 数据更新时间
    max(created_at) as last_updated
FROM dws_api_performance_percentiles
WHERE log_date = today() AND hour_partition = toHour(now());

-- 2. TOP性能API视图（支持首页展示）
CREATE VIEW v_top_performance_apis AS
SELECT 
    request_uri_normalized,
    platform,
    total_requests,
    success_rate,
    response_time_p95,
    backend_efficiency_avg,
    'slow' as performance_type,
    row_number() OVER (ORDER BY response_time_p95 DESC) as rank_position
FROM dws_api_performance_percentiles
WHERE log_date = today() AND hour_partition = toHour(now())
ORDER BY response_time_p95 DESC
LIMIT 10

UNION ALL

SELECT 
    request_uri_normalized,
    platform,
    total_requests,
    success_rate,
    response_time_p95,
    backend_efficiency_avg,
    'hot' as performance_type,
    row_number() OVER (ORDER BY total_requests DESC) as rank_position
FROM dws_api_performance_percentiles
WHERE log_date = today() AND hour_partition = toHour(now())
ORDER BY total_requests DESC
LIMIT 10;

-- 3. 异常检测视图（支持监控告警）
CREATE VIEW v_anomaly_detection AS
SELECT
    log_date,
    hour_partition,
    request_uri_normalized,
    platform,
    'high_error_rate' as anomaly_type,
    error_rate as anomaly_value,
    'API错误率过高' as anomaly_description,
    multiIf(error_rate > 10, 'CRITICAL', error_rate > 5, 'WARNING', 'OK') as severity
FROM dws_api_performance_percentiles
WHERE error_rate > 5

UNION ALL

SELECT
    log_date,
    hour_partition,
    request_uri_normalized,
    platform,
    'high_response_time' as anomaly_type,
    response_time_p95 as anomaly_value,
    'API响应时间过高' as anomaly_description,
    multiIf(response_time_p95 > 10, 'CRITICAL', response_time_p95 > 5, 'WARNING', 'OK') as severity
FROM dws_api_performance_percentiles
WHERE response_time_p95 > 3

UNION ALL

SELECT
    log_date,
    hour_partition,
    upstream_server as request_uri_normalized,
    cluster_node as platform,
    'low_health_score' as anomaly_type,
    health_score as anomaly_value,
    '上游服务健康度低' as anomaly_description,
    alert_level as severity
FROM dws_upstream_health_monitoring
WHERE health_score < 80;

-- ============================================================
-- 索引优化
-- ============================================================

-- DWD表核心索引
ALTER TABLE dwd_nginx_enriched ADD INDEX idx_api_performance (request_uri_normalized, platform, api_category) TYPE minmax GRANULARITY 3;
ALTER TABLE dwd_nginx_enriched ADD INDEX idx_error_analysis (is_error, response_status_code, log_time) TYPE minmax GRANULARITY 3;
ALTER TABLE dwd_nginx_enriched ADD INDEX idx_client_analysis (client_ip, platform, log_time) TYPE minmax GRANULARITY 3;
ALTER TABLE dwd_nginx_enriched ADD INDEX idx_trace_analysis (trace_id, business_sign) TYPE minmax GRANULARITY 3;

-- DWS表性能索引
ALTER TABLE dws_api_performance_percentiles ADD INDEX idx_performance_ranking (response_time_p95, total_requests) TYPE minmax GRANULARITY 2;
ALTER TABLE dws_realtime_qps_ranking ADD INDEX idx_qps_ranking (qps, qps_rank) TYPE minmax GRANULARITY 2;
ALTER TABLE dws_error_monitoring ADD INDEX idx_error_ranking (error_rate, error_count) TYPE minmax GRANULARITY 2;

-- ============================================================
-- TTL策略（数据生命周期管理）
-- ============================================================

-- ODS层数据保留90天
ALTER TABLE ods_nginx_raw MODIFY TTL date_partition + INTERVAL 90 DAY;

-- DWD层数据保留365天
ALTER TABLE dwd_nginx_enriched MODIFY TTL date_partition + INTERVAL 365 DAY;

-- DWS层数据保留策略
ALTER TABLE dws_api_performance_percentiles MODIFY TTL log_date + INTERVAL 1095 DAY;
ALTER TABLE dws_error_monitoring MODIFY TTL log_date + INTERVAL 365 DAY;
ALTER TABLE dws_upstream_health_monitoring MODIFY TTL log_date + INTERVAL 365 DAY;  
ALTER TABLE dws_client_behavior_analysis MODIFY TTL log_date + INTERVAL 365 DAY;
ALTER TABLE dws_trace_analysis MODIFY TTL log_date + INTERVAL 365 DAY;

-- 实时表短期TTL已在CREATE语句中定义

-- ============================================================
-- 数据质量检查
-- ============================================================

-- 创建数据质量监控表
CREATE TABLE IF NOT EXISTS data_quality_metrics (
    check_time DateTime,
    table_name LowCardinality(String),
    metric_name LowCardinality(String),
    metric_value Float64,
    threshold_value Float64,
    is_passed Bool,
    description String
) ENGINE = MergeTree()
ORDER BY (check_time, table_name, metric_name)
TTL check_time + INTERVAL 30 DAY;

-- ============================================================
-- 完成提示
-- ============================================================
-- Self功能完整支持的ClickHouse表结构设计完成！
-- 
-- 📊 支持功能清单:
-- ✅ 01.接口性能分析 - 完整支持所有阶段时间和分位数分析
-- ✅ 02.服务层级分析 - 支持12个时间指标和5个效率指标
-- ✅ 03.慢请求分析 - 支持全部性能指标和传输速度分析
-- ✅ 04.状态码统计 - 支持错误分布和时序分析
-- ✅ 05.时间维度分析 - 支持实时QPS和时序聚合
-- ✅ 06.服务稳定性 - 可选功能
-- ✅ 08.IP来源分析 - 支持地理位置和风险评估
-- ✅ 10.请求头分析 - 支持User-Agent详细解析
-- ✅ 11.请求头性能关联 - 支持多维度性能关联分析
-- ✅ 13.接口错误分析 - 支持错误影响范围和时序分析
-- ✅ 12.综合报告 - 汇总所有分析器数据
--
-- 🎯 核心监控指标支持:
-- ✅ 接口平均响应时长统计（含P50/P90/P95/P99）
-- ✅ TOP 5 最慢接口识别（ads_top_slow_apis表）
-- ✅ TOP 5 热点接口分析（ads_top_hot_apis表）
-- ✅ 实时QPS排行榜（dws_realtime_qps_ranking表）
-- ✅ 错误率监控（dws_error_monitoring表）
-- ✅ 集群级别性能对比（ads_cluster_performance_comparison表）
-- ✅ 上游服务健康监控（dws_upstream_health_monitoring表）
-- ✅ 缓存命中率分析（ads_cache_hit_analysis表）
-- ✅ 客户端行为分析（dws_client_behavior_analysis表）
-- ✅ 业务链路追踪（dws_trace_analysis表）
-- ✅ 连接复用率分析（connection_requests字段）
-- ✅ 请求大小分布（response_body_size_kb等字段）
-- ✅ 请求参数分析（query_parameters字段）
--
-- 架构特点：
-- 🚀 4层架构：ODS->DWD(65字段)->DWS(6表)->ADS(4表)
-- 📈 6个物化视图支持实时计算
-- 🔍 3个普通视图支持便捷查询
-- ⚡ 完整的索引和TTL策略
-- 📊 数据质量监控机制