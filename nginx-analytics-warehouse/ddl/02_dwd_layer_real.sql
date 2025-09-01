-- ==========================================
-- 基于现有DWD设计的改进版 - 面向业务分析
-- 参考: schema_design_v2_fixed.sql 中的 dwd_nginx_enriched
-- ==========================================

-- DWD层：在现有基础上增强业务维度
CREATE TABLE IF NOT EXISTS dwd_nginx_enriched_v2 (
    -- 保持现有基础字段结构
    id UInt64,
    ods_id UInt64,
    log_time DateTime64(3) CODEC(Delta, ZSTD(1)),
    date_partition Date,
    hour_partition UInt8,
    minute_partition UInt8,
    second_partition UInt8,
    
    -- 请求基础信息（保持原有）
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
    
    -- 响应信息（保持原有）
    response_status_code LowCardinality(String),
    response_body_size UInt64,
    response_body_size_kb Float64,
    total_bytes_sent UInt64,
    total_bytes_sent_kb Float64,
    
    -- 性能时间字段（保持原有65字段结构）
    total_request_duration Float64,
    upstream_connect_time Float64,
    upstream_header_time Float64,
    upstream_response_time Float64,
    backend_connect_phase Float64,
    backend_process_phase Float64,
    backend_transfer_phase Float64,
    nginx_transfer_phase Float64,
    backend_total_phase Float64,
    network_phase Float64,
    processing_phase Float64,
    transfer_phase Float64,
    response_transfer_speed Float64,
    total_transfer_speed Float64,
    nginx_transfer_speed Float64,
    backend_efficiency Float64,
    network_overhead Float64,
    transfer_ratio Float64,
    connection_cost_ratio Float64,
    processing_efficiency_index Float64,
    
    -- ===== 业务维度强化 =====
    -- 平台维度（核心！基于user_agent解析）
    platform LowCardinality(String),                -- Android/iOS/HarmonyOS/Web/SDK_Android/SDK_iOS/Other
    platform_version String CODEC(ZSTD(1)),         -- 平台版本
    app_version String CODEC(ZSTD(1)),               -- 应用版本
    device_type LowCardinality(String),              -- Mobile/Tablet/Desktop/Bot
    browser_type LowCardinality(String),             -- Chrome/Safari/WebView等
    os_type LowCardinality(String),                  -- iOS/Android/HarmonyOS/Windows
    os_version String CODEC(ZSTD(1)),                -- 系统版本
    
    -- SDK类型识别（基于user_agent中的SDK标识）
    sdk_type LowCardinality(String),                 -- WST-SDK-iOS/WST-SDK-ANDROID/zgt-ios等
    sdk_version String CODEC(ZSTD(1)),               -- SDK版本
    
    -- 业务来源分析
    bot_type LowCardinality(String),
    entry_source LowCardinality(String),             -- Direct/Search/Social/External/Internal
    referer_domain String CODEC(ZSTD(1)),
    search_engine LowCardinality(String),
    social_media LowCardinality(String),
    
    -- API业务分类（重要！）
    api_category LowCardinality(String),             -- 基于URI路径的业务分类
    api_module LowCardinality(String),               -- 功能模块：gxrz/zgt/search/calendar等
    api_version LowCardinality(String),              -- API版本：rest/v1/v2等
    business_domain LowCardinality(String),          -- 业务域：用户中心/搜索/日历等
    
    -- 接入方式分类（新增）
    access_type LowCardinality(String),              -- APP_Native/H5_WebView/Browser/OpenAPI/Internal
    client_category LowCardinality(String),          -- Mobile_App/Desktop_Web/Mini_Program/API_Client
    
    -- 链路和服务（保持原有）
    application_name LowCardinality(String),
    service_name LowCardinality(String),
    trace_id String CODEC(ZSTD(1)),
    business_sign LowCardinality(String),
    cluster_node LowCardinality(String),
    upstream_server String CODEC(ZSTD(1)),
    connection_requests UInt32,
    cache_status LowCardinality(String),
    
    -- 原始数据保留
    referer_url String CODEC(ZSTD(1)),
    user_agent_string String CODEC(ZSTD(1)),
    log_source_file LowCardinality(String),
    
    -- ===== 业务标识强化 =====
    -- 成功/失败判断（业务维度）
    is_success Bool,                                 -- HTTP 2xx
    is_business_success Bool,                        -- 业务逻辑成功（可基于返回内容判断）
    is_slow Bool,                                    -- >3s慢请求
    is_very_slow Bool,                               -- >10s超慢请求
    is_error Bool,                                   -- 4xx/5xx错误
    is_client_error Bool,                            -- 4xx客户端错误
    is_server_error Bool,                            -- 5xx服务端错误
    has_anomaly Bool,
    anomaly_type LowCardinality(String),
    
    -- 用户体验分级
    user_experience_level LowCardinality(String),   -- Excellent/Good/Fair/Poor/Unacceptable
    apdex_classification LowCardinality(String),    -- Satisfied/Tolerating/Frustrated
    
    -- 业务重要性权重
    api_importance LowCardinality(String),           -- Critical/High/Medium/Low
    business_value_score UInt8 DEFAULT 5,           -- 1-10业务价值评分
    
    -- 数据质量
    data_quality_score Float64,
    parsing_errors Array(String),                    -- 解析错误记录
    
    -- 地理和网络（保持原有）
    client_region LowCardinality(String),
    client_isp LowCardinality(String),
    ip_risk_level LowCardinality(String),
    is_internal_ip Bool,
    
    -- 时间维度字段（保持原有）
    date Date MATERIALIZED toDate(log_time),
    hour UInt8 MATERIALIZED toHour(log_time),
    minute UInt8 MATERIALIZED toMinute(log_time),
    second UInt8 MATERIALIZED toSecond(log_time),
    date_hour String MATERIALIZED concat(toString(date), '_', toString(hour)),
    date_hour_minute String MATERIALIZED concat(toString(date), '_', toString(hour), '_', toString(minute)),
    weekday UInt8 MATERIALIZED toDayOfWeek(log_time),      -- 星期几
    is_weekend Bool MATERIALIZED weekday IN (6, 7),        -- 是否周末
    time_period LowCardinality(String) MATERIALIZED        -- 时间段分类
        multiIf(
            hour < 6, 'Dawn',           -- 凌晨 0-6
            hour < 12, 'Morning',       -- 上午 6-12  
            hour < 14, 'Noon',          -- 中午 12-14
            hour < 18, 'Afternoon',     -- 下午 14-18
            hour < 22, 'Evening',       -- 晚上 18-22
            'Night'                     -- 夜间 22-24
        ),
    
    -- 元数据
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY (date_partition, platform)              -- 按日期+平台双分区
ORDER BY (date_partition, platform, api_category, hour_partition, log_time)
SETTINGS index_granularity = 8192;

-- ===== 高性能索引设计 =====
-- 业务维度索引
ALTER TABLE dwd_nginx_enriched_v2 ADD INDEX idx_platform (platform) TYPE set(0) GRANULARITY 3;
ALTER TABLE dwd_nginx_enriched_v2 ADD INDEX idx_api_category (api_category) TYPE set(0) GRANULARITY 3;
ALTER TABLE dwd_nginx_enriched_v2 ADD INDEX idx_api_module (api_module) TYPE set(0) GRANULARITY 3;
ALTER TABLE dwd_nginx_enriched_v2 ADD INDEX idx_access_type (access_type) TYPE set(0) GRANULARITY 3;
ALTER TABLE dwd_nginx_enriched_v2 ADD INDEX idx_sdk_type (sdk_type) TYPE set(0) GRANULARITY 3;

-- 性能维度索引
ALTER TABLE dwd_nginx_enriched_v2 ADD INDEX idx_response_time (total_request_duration) TYPE minmax GRANULARITY 4;
ALTER TABLE dwd_nginx_enriched_v2 ADD INDEX idx_status_code (response_status_code) TYPE set(0) GRANULARITY 3;
ALTER TABLE dwd_nginx_enriched_v2 ADD INDEX idx_user_experience (user_experience_level) TYPE set(0) GRANULARITY 3;

-- 时间维度索引
ALTER TABLE dwd_nginx_enriched_v2 ADD INDEX idx_time_period (time_period) TYPE set(0) GRANULARITY 3;
ALTER TABLE dwd_nginx_enriched_v2 ADD INDEX idx_weekday (weekday) TYPE set(0) GRANULARITY 4;

-- URI检索优化
ALTER TABLE dwd_nginx_enriched_v2 ADD INDEX idx_uri_normalized (request_uri_normalized) TYPE bloom_filter GRANULARITY 4;

-- ===== 投影索引 - 预聚合高频查询 =====
-- 平台性能分析投影
ALTER TABLE dwd_nginx_enriched_v2 ADD PROJECTION proj_platform_hourly_performance
(
    SELECT 
        toStartOfHour(log_time) as hour,
        platform,
        api_category,
        count() as requests,
        avg(total_request_duration) as avg_response_time,
        quantile(0.95)(total_request_duration) as p95_response_time,
        countIf(is_success) as success_count,
        countIf(is_slow) as slow_count,
        countIf(is_error) as error_count,
        uniq(client_ip) as unique_ips
    GROUP BY hour, platform, api_category
);

-- 业务模块分析投影
ALTER TABLE dwd_nginx_enriched_v2 ADD PROJECTION proj_api_module_stats
(
    SELECT
        date_partition,
        api_module,
        platform, 
        access_type,
        count() as requests,
        avg(total_request_duration) as avg_response_time,
        countIf(is_success) * 100.0 / count() as success_rate,
        countIf(is_business_success) * 100.0 / count() as business_success_rate
    GROUP BY date_partition, api_module, platform, access_type
);

COMMENT ON TABLE dwd_nginx_enriched_v2 IS 'DWD明细层-业务强化版，支持多平台多维度分析，保持Self功能兼容性';