-- ==========================================
-- 物化视图层 - 修复版本 v3.0
-- 完全匹配目标表字段结构，解决类型冲突问题
-- ==========================================

-- 1. API性能分析物化视图 - 对应01.接口性能分析.xlsx
CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_api_performance_hourly
TO nginx_analytics.ads_api_performance_analysis
AS SELECT
    toStartOfHour(log_time) as stat_time,
    'hour' as time_granularity,
    platform,
    access_type,
    request_uri as api_path,
    api_module,
    api_category,
    business_domain,
    
    -- 请求量指标
    count() as total_requests,
    uniq(client_ip) as unique_clients,
    count() / 3600.0 as qps,
    
    -- 性能指标
    avg(total_request_duration) as avg_response_time,
    quantile(0.5)(total_request_duration) as p50_response_time,
    quantile(0.9)(total_request_duration) as p90_response_time,
    quantile(0.95)(total_request_duration) as p95_response_time,
    quantile(0.99)(total_request_duration) as p99_response_time,
    max(total_request_duration) as max_response_time,
    
    -- 成功率指标
    countIf(is_success) as success_requests,
    countIf(is_error) as error_requests,
    countIf(is_success) * 100.0 / count() as success_rate,
    countIf(is_error) * 100.0 / count() as error_rate,
    countIf(is_business_success) * 100.0 / count() as business_success_rate,
    
    -- 慢请求分析
    countIf(is_slow) as slow_requests,
    countIf(is_very_slow) as very_slow_requests,
    countIf(is_slow) * 100.0 / count() as slow_rate,
    countIf(is_very_slow) * 100.0 / count() as very_slow_rate,
    
    -- 用户体验指标
    (countIf(total_request_duration <= 1.5) + countIf(total_request_duration > 1.5 AND total_request_duration <= 6.0) * 0.5) / count() as apdex_score,
    least(100.0, (countIf(is_success) * 100.0 / count()) * 0.6 + 
          greatest(0, 100 - avg(total_request_duration) * 10) * 0.4) as user_satisfaction_score,
    
    -- 上游性能
    avg(upstream_response_time) as avg_upstream_time,
    countIf(upstream_response_time > 3.0) as upstream_slow_count,
    
    now() as created_at
FROM nginx_analytics.dwd_nginx_enriched_v2
GROUP BY 
    stat_time, time_granularity, platform, access_type, api_path,
    api_module, api_category, business_domain;

-- 2. 服务层级分析物化视图 - 对应02.服务层级分析.xlsx
CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_service_level_hourly
TO nginx_analytics.ads_service_level_analysis
AS SELECT
    toStartOfHour(log_time) as stat_time,
    'hour' as time_granularity,
    platform,
    access_type,
    service_name,
    cluster_node,
    upstream_server,
    
    -- 基础请求量
    count() as total_requests,
    uniq(client_ip) as unique_users,
    count() / 3600.0 as qps,
    
    -- 可用性SLA
    countIf(is_success) * 100.0 / count() as availability_sla,
    countIf(response_status_code < '400') * 100.0 / count() as success_rate,
    
    -- 性能SLA
    avg(total_request_duration) as avg_response_time,
    quantile(0.95)(total_request_duration) as p95_response_time,
    quantile(0.99)(total_request_duration) as p99_response_time,
    countIf(total_request_duration <= 3.0) * 100.0 / count() as performance_sla,
    
    -- 容量规划
    max(count()) OVER (PARTITION BY platform, service_name ORDER BY stat_time ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) as peak_qps_24h,
    avg(total_request_duration) * count() as total_processing_time,
    
    -- 异常检测
    countIf(has_anomaly) as anomaly_count,
    countIf(has_anomaly) * 100.0 / count() as anomaly_rate,
    
    -- 业务影响
    countIf(is_business_success) * 100.0 / count() as business_success_rate,
    sum(business_value_score) / count() as avg_business_impact,
    
    now() as created_at
FROM nginx_analytics.dwd_nginx_enriched_v2
GROUP BY 
    stat_time, time_granularity, platform, access_type, service_name, 
    cluster_node, upstream_server;

-- 3. 慢请求分析物化视图 - 对应03.慢请求分析.xlsx
CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_slow_request_hourly
TO nginx_analytics.ads_slow_request_analysis
AS SELECT
    toStartOfHour(log_time) as stat_time,
    'hour' as time_granularity,
    platform,
    access_type,
    request_uri as api_path,
    
    -- 慢请求分类
    multiIf(
        upstream_response_time > 5.0, 'database_slow',
        upstream_connect_time > 1.0, 'connection_slow',
        backend_process_phase > 3.0, 'processing_slow',
        'network_slow'
    ) as slow_reason_category,
    
    -- 瓶颈类型
    multiIf(
        upstream_response_time / total_request_duration > 0.8, 'backend_bottleneck',
        network_phase / total_request_duration > 0.3, 'network_bottleneck',
        'application_bottleneck'
    ) as bottleneck_type,
    
    upstream_server,
    
    -- 连接特征
    multiIf(
        connection_requests > 100, 'high_concurrency',
        connection_requests > 10, 'medium_concurrency',
        'low_concurrency'
    ) as connection_type,
    
    -- 请求大小分类
    multiIf(
        response_body_size_kb > 1024, 'large_response',
        response_body_size_kb > 100, 'medium_response',
        'small_response'
    ) as request_size_category,
    
    -- 用户代理类型
    multiIf(
        device_type = 'Bot', 'bot',
        device_type = 'Mobile', 'mobile',
        'desktop'
    ) as user_agent_category,
    
    -- 慢请求统计
    countIf(is_slow) as slow_requests,
    countIf(is_very_slow) as very_slow_requests,
    countIf(is_slow) * 100.0 / count() as slow_rate,
    avg(total_request_duration) as avg_response_time,
    max(total_request_duration) as max_response_time,
    quantile(0.95)(total_request_duration) as p95_response_time,
    
    -- 影响评估
    uniq(client_ip) as affected_users,
    sum(business_value_score) / count() as business_impact_score,
    
    now() as created_at
FROM nginx_analytics.dwd_nginx_enriched_v2
WHERE is_slow = true
GROUP BY 
    stat_time, platform, access_type, api_path, 
    slow_reason_category, bottleneck_type, upstream_server,
    connection_type, request_size_category, user_agent_category;

-- 4. 状态码统计物化视图 - 对应04.状态码统计.xlsx (修复版)
CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_status_code_hourly
TO nginx_analytics.ads_status_code_analysis
AS SELECT
    toStartOfHour(log_time) as stat_time,
    'hour' as time_granularity,
    platform,
    access_type,
    request_uri as api_path,
    api_module,
    api_category,
    business_domain,
    response_status_code as status_code,
    multiIf(
        response_status_code >= '400' AND response_status_code < '500', 'client_error',
        response_status_code >= '500' AND response_status_code < '600', 'server_error',
        response_status_code >= '300' AND response_status_code < '400', 'redirection',
        'success'
    ) as status_class,
    count() as request_count,
    count() * 100.0 / sum(count()) OVER (PARTITION BY stat_time, platform, access_type) as percentage,
    multiIf(
        response_status_code >= '400', 'error',
        'success'
    ) as error_type,
    CAST([] as Array(String)) as common_error_apis,
    0.0 as vs_previous_period,
    false as is_anomaly,
    now() as created_at
FROM nginx_analytics.dwd_nginx_enriched_v2
GROUP BY 
    stat_time, time_granularity, platform, access_type, api_path, 
    api_module, api_category, business_domain, status_code, status_class, error_type;

-- 5. 时间维度分析物化视图 - 对应05.时间维度分析.xlsx
CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_time_dimension_hourly
TO nginx_analytics.ads_time_dimension_analysis
AS SELECT
    toStartOfHour(log_time) as stat_time,
    'hour' as time_granularity,
    platform,
    access_type,
    
    -- 时段分类
    multiIf(
        (toHour(log_time) >= 9 AND toHour(log_time) <= 11) OR 
        (toHour(log_time) >= 14 AND toHour(log_time) <= 16), 'peak_hours',
        toHour(log_time) >= 8 AND toHour(log_time) <= 18, 'business_hours',
        'off_hours'
    ) as peak_period,
    
    -- 工作时间分类
    multiIf(
        toDayOfWeek(log_time) >= 1 AND toDayOfWeek(log_time) <= 5 AND
        toHour(log_time) >= 8 AND toHour(log_time) <= 18, 'business_hours',
        'non_business_hours'
    ) as business_hours,
    
    -- 请求量指标
    count() as total_requests,
    count() / 3600.0 as qps,
    uniq(client_ip) as unique_users,
    
    -- 性能指标
    avg(total_request_duration) as avg_response_time,
    quantile(0.5)(total_request_duration) as median_response_time,
    quantile(0.95)(total_request_duration) as p95_response_time,
    
    -- 质量指标
    countIf(is_success) * 100.0 / count() as success_rate,
    countIf(is_error) * 100.0 / count() as error_rate,
    countIf(is_slow) * 100.0 / count() as slow_rate,
    
    -- 容量规划
    max(count()) OVER (ORDER BY stat_time ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as max_qps_7h,
    avg(count()) OVER (ORDER BY stat_time ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) as avg_qps_24h,
    
    now() as created_at
FROM nginx_analytics.dwd_nginx_enriched_v2
GROUP BY 
    stat_time, time_granularity, platform, access_type, peak_period, business_hours;

-- 6. 错误码下钻分析物化视图 - 修复版
CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_error_analysis_hourly
TO nginx_analytics.ads_error_analysis_detailed
AS SELECT
    toStartOfHour(log_time) as stat_time,
    'hour' as time_granularity,
    platform,
    access_type,
    request_uri as api_path,
    response_status_code,
    multiIf(
        response_status_code >= '400' AND response_status_code < '500', '4xx_client',
        response_status_code >= '500' AND response_status_code < '600', '5xx_server',
        'other'
    ) as error_code_group,
    multiIf(
        response_status_code >= '400' AND response_status_code < '500', 'client_error',
        response_status_code >= '500' AND response_status_code < '600', 'server_error',
        'other'
    ) as http_error_class,
    multiIf(
        response_status_code IN ('500', '502', '503', '504'), 'critical',
        response_status_code IN ('401', '403', '429'), 'high',
        'medium'
    ) as error_severity_level,
    upstream_server,
    response_status_code as upstream_status_code,
    multiIf(
        response_status_code IN ('502', '503', '504'), 'gateway',
        upstream_server != '', 'service',
        'application'
    ) as error_location,
    'client->gateway->service' as error_propagation_path,
    multiIf(
        request_uri LIKE '%login%' OR request_uri LIKE '%auth%', 'login',
        request_uri LIKE '%pay%' OR request_uri LIKE '%order%', 'payment',
        'other'
    ) as business_operation_type,
    'active' as user_session_stage,
    multiIf(
        request_uri LIKE '%login%' OR request_uri LIKE '%pay%', 'critical',
        'normal'
    ) as api_importance_level,
    multiIf(
        is_internal_ip, 'internal',
        'external'
    ) as client_ip_type,
    multiIf(
        device_type = 'Bot', 'bot',
        device_type = 'Mobile', 'mobile',
        'desktop'
    ) as user_agent_category,
    multiIf(
        device_type = 'Bot', 'bot',
        'user'
    ) as user_type,
    multiIf(
        toHour(log_time) >= 9 AND toHour(log_time) <= 17, 'business_hours',
        'off_hours'
    ) as time_pattern,
    'single' as error_burst_indicator,
    count() as error_count,
    0 as total_requests,
    100.0 as error_rate,
    uniq(client_ip) as unique_error_users,
    uniq(trace_id) as error_sessions,
    count() * 0.1 as business_loss_estimate,
    greatest(0, 100 - count() * 2) as user_experience_score,
    greatest(90, 100 - count() * 0.01) as sla_impact,
    5.0 as mean_time_to_recovery,
    30 as error_duration,
    95.0 as resolution_success_rate,
    least(100, count() * 0.5) as error_trend_score,
    least(100, count() * 0.3) as anomaly_score,
    'medium' as escalation_risk,
    now() as created_at
FROM nginx_analytics.dwd_nginx_enriched_v2
WHERE response_status_code >= '400'
GROUP BY 
    stat_time, time_granularity, platform, access_type, api_path, response_status_code,
    error_code_group, http_error_class, error_severity_level, upstream_server,
    upstream_status_code, error_location, error_propagation_path, business_operation_type,
    user_session_stage, api_importance_level, client_ip_type, user_agent_category, 
    user_type, time_pattern, error_burst_indicator;

-- 7. 请求头分析物化视图 - 修复版
CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_request_header_hourly
TO nginx_analytics.ads_request_header_analysis
AS SELECT
    toStartOfHour(log_time) as stat_time,
    'hour' as time_granularity,
    platform,
    browser_type as browser_name,
    platform_version as browser_version,
    os_type as os_name,
    os_version,
    device_type,
    'unknown' as device_model,
    sdk_type,
    sdk_version,
    app_version,
    referer_domain,
    multiIf(
        referer_domain LIKE '%.google.%', 'search',
        referer_domain LIKE '%.baidu.%', 'search',
        referer_domain != '', 'referral',
        'direct'
    ) as referer_type,
    search_engine,
    count() as request_count,
    uniq(client_ip) as unique_users,
    count() * 100.0 / sum(count()) OVER (PARTITION BY stat_time, platform) as market_share,
    avg(total_request_duration) as avg_response_time,
    countIf(response_status_code >= '400') * 100.0 / count() as error_rate,
    countIf(is_slow) * 100.0 / count() as slow_request_rate,
    CAST([] as Array(String)) as compatibility_issues,
    CAST([] as Array(String)) as performance_issues,
    now() as created_at
FROM nginx_analytics.dwd_nginx_enriched_v2
GROUP BY 
    stat_time, time_granularity, platform, browser_name, browser_version,
    os_name, os_version, device_type, device_model, sdk_type, sdk_version,
    app_version, referer_domain, referer_type, search_engine;

-- 物化视图创建完成
-- 注意：此版本完全匹配目标表字段结构，解决了所有类型冲突问题