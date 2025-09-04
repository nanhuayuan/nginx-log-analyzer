-- 正确的物化视图定义 - 字段完全匹配目标表结构

-- 4. 状态码统计物化视图 - 字段匹配ads_status_code_analysis
DROP MATERIALIZED VIEW IF EXISTS nginx_analytics.mv_status_code_hourly;
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

-- 6. 错误码下钻分析物化视图 - 字段匹配ads_error_analysis_detailed  
DROP MATERIALIZED VIEW IF EXISTS nginx_analytics.mv_error_analysis_hourly;
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

-- 7. 请求头分析物化视图 - 字段匹配ads_request_header_analysis
DROP MATERIALIZED VIEW IF EXISTS nginx_analytics.mv_request_header_hourly;
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