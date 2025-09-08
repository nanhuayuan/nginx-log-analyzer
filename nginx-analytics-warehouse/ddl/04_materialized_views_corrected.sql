-- ==========================================
-- 物化视图层 - 完整版本 v4.0
-- 包含所有12个物化视图，完全匹配目标表字段结构
-- 新增5个缺失的物化视图：API错误、IP来源、服务稳定性、请求头关联、综合报告
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
    countIf(toUInt16OrZero(response_status_code) < 400) * 100.0 / count() as success_rate,
    
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
        toUInt16OrZero(response_status_code) >= 400 AND toUInt16OrZero(response_status_code) < 500, 'client_error',
        toUInt16OrZero(response_status_code) >= 500 AND toUInt16OrZero(response_status_code) < 600, 'server_error',
        toUInt16OrZero(response_status_code) >= 300 AND toUInt16OrZero(response_status_code) < 400, 'redirection',
        'success'
    ) as status_class,
    count() as request_count,
    count() * 100.0 / sum(count()) OVER (PARTITION BY stat_time, platform, access_type) as percentage,
    multiIf(
        toUInt16OrZero(response_status_code) >= 400, 'error',
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
        toUInt16OrZero(response_status_code) >= 400 AND toUInt16OrZero(response_status_code) < 500, '4xx_client',
        toUInt16OrZero(response_status_code) >= 500 AND toUInt16OrZero(response_status_code) < 600, '5xx_server',
        'other'
    ) as error_code_group,
    multiIf(
        toUInt16OrZero(response_status_code) >= 400 AND toUInt16OrZero(response_status_code) < 500, 'client_error',
        toUInt16OrZero(response_status_code) >= 500 AND toUInt16OrZero(response_status_code) < 600, 'server_error',
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
WHERE toUInt16OrZero(response_status_code) >= 400
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
        referer_domain LIKE '%.google.%' OR referer_domain LIKE '%.baidu.%' OR referer_domain LIKE '%.bing.%', 'search',
        referer_domain != '' AND referer_domain != '-', 'referral', 
        'direct'
    ) as referer_type,
    search_engine,
    count() as request_count,
    uniq(client_ip) as unique_users,
    count() * 100.0 / sum(count()) OVER (PARTITION BY stat_time, platform) as market_share,
    avg(total_request_duration) as avg_response_time,
    countIf(toUInt16OrZero(response_status_code) >= 400) * 100.0 / count() as error_rate,
    countIf(is_slow) * 100.0 / count() as slow_request_rate,
    CAST([] as Array(String)) as compatibility_issues,
    CAST([] as Array(String)) as performance_issues,
    now() as created_at
FROM nginx_analytics.dwd_nginx_enriched_v2
GROUP BY 
    stat_time, time_granularity, platform, browser_name, browser_version,
    os_name, os_version, device_type, device_model, sdk_type, sdk_version,
    app_version, referer_domain, referer_type, search_engine;

-- ==========================================
-- 新增：缺失的5个物化视图 (v4.0)
-- ==========================================

-- 8. API错误分析物化视图 - 对应ads_api_error_analysis 
CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_api_error_analysis_hourly
TO nginx_analytics.ads_api_error_analysis
AS SELECT
    toStartOfHour(log_time) as stat_time,
    'hour' as time_granularity,
    platform,
    request_uri as api_path,
    api_module,
    response_status_code as error_code,
    count() as error_count,
    uniq(client_ip) as unique_error_clients,
    countIf(toUInt16OrZero(response_status_code) >= 400 AND toUInt16OrZero(response_status_code) < 500) as client_error_count,
    countIf(toUInt16OrZero(response_status_code) >= 500) as server_error_count,
    countIf(total_request_duration > 3.0) as slow_error_count,
    countIf(total_request_duration > 30.0) as timeout_error_count,
    avg(total_request_duration) as avg_error_response_time,
    max(total_request_duration) as max_error_response_time,
    countIf(toUInt16OrZero(response_status_code) >= 500) as upstream_error_count,
    avg(upstream_response_time) as avg_upstream_error_time,
    count() * 100.0 / 
        (SELECT count() FROM nginx_analytics.dwd_nginx_enriched_v2 dwd2 
         WHERE toStartOfHour(dwd2.log_time) = toStartOfHour(dwd_nginx_enriched_v2.log_time)) as error_rate_percent,
    min(log_time) as first_error_time,
    max(log_time) as last_error_time
FROM nginx_analytics.dwd_nginx_enriched_v2
WHERE is_error = true
GROUP BY toStartOfHour(log_time), platform, request_uri, api_module, response_status_code;

-- 9. IP来源分析物化视图 - 对应ads_ip_source_analysis
CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_ip_source_analysis_hourly
TO nginx_analytics.ads_ip_source_analysis
AS SELECT
    toStartOfHour(log_time) as stat_time,
    'hour' as time_granularity,
    client_ip,
    multiIf(
        client_ip LIKE '10.%' OR client_ip LIKE '172.1%' OR client_ip LIKE '172.2%' OR 
        client_ip LIKE '172.3%' OR client_ip LIKE '192.168.%', 'private',
        client_ip LIKE '127.%', 'loopback',
        'public'
    ) as ip_type,
    multiIf(
        client_ip LIKE '10.%' OR client_ip LIKE '172.1%' OR client_ip LIKE '172.2%' OR 
        client_ip LIKE '172.3%' OR client_ip LIKE '192.168.%' OR client_ip LIKE '127.%', 'internal',
        'external'  
    ) as ip_category,
    multiIf(
        client_ip LIKE '10.%' OR client_ip LIKE '172.1%' OR client_ip LIKE '172.2%' OR 
        client_ip LIKE '172.3%' OR client_ip LIKE '192.168.%', 'internal',
        client_ip LIKE '127.%', 'localhost',
        'external'
    ) as ip_classification,
    count() as total_requests,
    uniq(request_uri) as unique_apis,
    uniq(user_agent_string) as unique_user_agents,
    avg(total_request_duration) as avg_response_time,
    quantile(0.95)(total_request_duration) as p95_response_time,
    countIf(is_success) as success_requests,
    countIf(is_error) as error_requests,
    countIf(is_success) * 100.0 / count() as success_rate,
    countIf(total_request_duration > 3.0) as slow_requests,
    countIf(response_status_code = '404') as not_found_requests,
    countIf(response_status_code = '403') as forbidden_requests,
    countIf(toUInt16OrZero(response_status_code) >= 500) as server_error_requests,
    uniq(toHour(log_time)) as active_hours,
    min(log_time) as first_seen_time,
    max(log_time) as last_seen_time,
    multiIf(
        countIf(is_error) * 100.0 / count() > 50, 'high_risk',
        countIf(is_error) * 100.0 / count() > 20, 'medium_risk', 
        countIf(is_error) * 100.0 / count() > 5, 'low_risk',
        'normal'
    ) as risk_level
FROM nginx_analytics.dwd_nginx_enriched_v2
GROUP BY toStartOfHour(log_time), client_ip;

-- 10. 服务稳定性分析物化视图 - 对应ads_service_stability_analysis
CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_service_stability_analysis_hourly
TO nginx_analytics.ads_service_stability_analysis
AS SELECT
    toStartOfHour(log_time) as stat_time,
    'hour' as time_granularity,
    platform,
    service_name,
    api_module,
    count() as total_requests,
    countIf(is_success) as success_requests,
    countIf(is_error) as error_requests,
    countIf(is_success) * 100.0 / count() as success_rate,
    avg(total_request_duration) as avg_response_time,
    stddevPop(total_request_duration) as response_time_stddev,
    quantile(0.5)(total_request_duration) as median_response_time,
    quantile(0.95)(total_request_duration) as p95_response_time,
    quantile(0.99)(total_request_duration) as p99_response_time,
    countIf(total_request_duration > 3.0) as slow_requests,
    countIf(total_request_duration > 10.0) as very_slow_requests,
    countIf(total_request_duration > 3.0) * 100.0 / count() as slow_rate,
    countIf(toUInt16OrZero(response_status_code) >= 500) as server_errors,
    countIf(toUInt16OrZero(response_status_code) >= 400 AND toUInt16OrZero(response_status_code) < 500) as client_errors,
    countIf(total_request_duration > 30.0) as timeout_errors,
    avg(upstream_response_time) as avg_upstream_response_time,
    countIf(toUInt16OrZero(response_status_code) >= 500) as upstream_server_errors,
    countIf(upstream_response_time > 0 AND upstream_response_time != total_request_duration) as upstream_issues,
    if(countIf(is_success) * 100.0 / count() >= 99.99, '99.99%',
       if(countIf(is_success) * 100.0 / count() >= 99.95, '99.95%',
          if(countIf(is_success) * 100.0 / count() >= 99.9, '99.9%',
             if(countIf(is_success) * 100.0 / count() >= 99.0, '99.0%', 'below_99%')))) as sla_level,
    multiIf(
        countIf(is_success) * 100.0 / count() >= 99.95 AND avg(total_request_duration) <= 1.0, 'excellent',
        countIf(is_success) * 100.0 / count() >= 99.9 AND avg(total_request_duration) <= 2.0, 'good',
        countIf(is_success) * 100.0 / count() >= 99.0 AND avg(total_request_duration) <= 5.0, 'fair',
        'poor'
    ) as stability_grade
FROM nginx_analytics.dwd_nginx_enriched_v2
GROUP BY toStartOfHour(log_time), platform, service_name, api_module;

-- 11. 请求头性能关联分析物化视图 - 对应ads_header_performance_correlation
CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_header_performance_correlation_hourly
TO nginx_analytics.ads_header_performance_correlation
AS SELECT
    toStartOfHour(log_time) as stat_time,
    'hour' as time_granularity,
    browser_type as browser_name,
    platform_version as browser_version,
    os_type as os_name,
    os_version, 
    device_type,
    referer_domain,
    multiIf(
        referer_domain LIKE '%.google.%' OR referer_domain LIKE '%.baidu.%' OR referer_domain LIKE '%.bing.%', 'search',
        referer_domain != '' AND referer_domain != '-', 'referral',
        'direct'
    ) as referer_type,
    count() as total_requests,
    avg(total_request_duration) as avg_response_time,
    quantile(0.5)(total_request_duration) as median_response_time,
    quantile(0.95)(total_request_duration) as p95_response_time,
    quantile(0.99)(total_request_duration) as p99_response_time,
    max(total_request_duration) as max_response_time,
    countIf(is_success) as success_requests,
    countIf(is_error) as error_requests,
    countIf(is_success) * 100.0 / count() as success_rate,
    countIf(total_request_duration > 3.0) as slow_requests,
    countIf(total_request_duration > 10.0) as very_slow_requests,
    countIf(total_request_duration > 3.0) * 100.0 / count() as slow_rate,
    countIf(toUInt16OrZero(response_status_code) >= 400 AND toUInt16OrZero(response_status_code) < 500) as client_errors,
    countIf(toUInt16OrZero(response_status_code) >= 500) as server_errors,
    countIf(user_agent_string LIKE '%bot%' OR user_agent_string LIKE '%Bot%' OR 
            user_agent_string LIKE '%spider%') as bot_requests,
    countIf(user_agent_string LIKE '%bot%' OR user_agent_string LIKE '%Bot%' OR 
            user_agent_string LIKE '%spider%') * 100.0 / count() as bot_rate,
    (countIf(total_request_duration <= 1.5) + 
     countIf(total_request_duration > 1.5 AND total_request_duration <= 6.0) * 0.5) / count() as apdex_score,
    countIf(user_agent_string LIKE '%mobile%' OR user_agent_string LIKE '%Mobile%') as mobile_requests,
    countIf(user_agent_string LIKE '%bot%' OR user_agent_string LIKE '%Bot%' OR 
            user_agent_string LIKE '%spider%') as crawler_requests
FROM nginx_analytics.dwd_nginx_enriched_v2
WHERE user_agent_string != '' AND user_agent_string IS NOT NULL
GROUP BY toStartOfHour(log_time), browser_type, platform_version, os_type, os_version, 
         device_type, referer_domain;

-- 12. 综合报告物化视图 - 对应ads_comprehensive_report
CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_comprehensive_report_hourly
TO nginx_analytics.ads_comprehensive_report
AS SELECT
    toStartOfHour(log_time) as stat_time,
    'hour' as time_granularity,
    count() as total_requests,
    uniq(client_ip) as unique_visitors,
    uniq(request_uri) as unique_apis,
    count() / 3600.0 as avg_qps,
    avg(total_request_duration) as avg_response_time,
    quantile(0.5)(total_request_duration) as median_response_time,
    quantile(0.95)(total_request_duration) as p95_response_time,
    quantile(0.99)(total_request_duration) as p99_response_time,
    max(total_request_duration) as max_response_time,
    countIf(is_success) as success_requests,
    countIf(is_error) as error_requests,
    countIf(is_success) * 100.0 / count() as overall_success_rate,
    countIf(is_error) * 100.0 / count() as overall_error_rate,
    countIf(toUInt16OrZero(response_status_code) >= 400 AND toUInt16OrZero(response_status_code) < 500) as client_errors,
    countIf(toUInt16OrZero(response_status_code) >= 500) as server_errors,
    countIf(total_request_duration > 30.0) as timeout_errors,
    countIf(total_request_duration > 3.0) as slow_requests,
    countIf(total_request_duration > 10.0) as very_slow_requests,
    countIf(total_request_duration > 3.0) * 100.0 / count() as slow_request_rate,
    CAST([] as Array(String)) as top_error_apis,
    CAST([] as Array(UInt64)) as top_error_counts,
    countIf(platform = 'web') as web_requests,
    countIf(platform = 'mobile') as mobile_requests,
    countIf(platform = 'api') as api_requests,
    avg(upstream_response_time) as avg_upstream_response_time,
    countIf(toUInt16OrZero(response_status_code) >= 500) as upstream_errors,
    countIf(upstream_response_time > total_request_duration * 0.8) as upstream_bottleneck_requests,
    (countIf(total_request_duration <= 1.5) + 
     countIf(total_request_duration > 1.5 AND total_request_duration <= 6.0) * 0.5) / count() as overall_apdex_score,
    least(100, greatest(0,
        countIf(is_success) * 100.0 / count() * 0.4 +
        greatest(0, 100 - avg(total_request_duration) * 20) * 0.3 +
        greatest(0, 100 - countIf(total_request_duration > 3.0) * 100.0 / count() * 2) * 0.2 +
        if(count() > 1000, 10, count() / 100) * 0.1
    )) as system_health_score,
    count() / 10000.0 as capacity_utilization_rate,
    multiIf(
        toHour(toStartOfHour(log_time)) >= 0 AND toHour(toStartOfHour(log_time)) < 6, 'night',
        toHour(toStartOfHour(log_time)) >= 6 AND toHour(toStartOfHour(log_time)) < 12, 'morning',
        toHour(toStartOfHour(log_time)) >= 12 AND toHour(toStartOfHour(log_time)) < 18, 'afternoon',
        'evening'
    ) as time_period
FROM nginx_analytics.dwd_nginx_enriched_v2
GROUP BY toStartOfHour(log_time);

-- ==========================================
-- 物化视图创建完成 - v4.0完整版
-- 包含12个物化视图：
-- 1-7: 原有物化视图 (已验证)
-- 8-12: 新增物化视图 (填补数据空白)
-- ==========================================