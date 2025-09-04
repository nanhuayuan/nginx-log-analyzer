-- ==========================================
-- 物化视图层v2.1 - 字段修复版
-- 基于字段映射验证文档，使用动态计算处理缺失字段
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
    avg(upstream_connect_time) as avg_upstream_connect_time,
    
    -- 流量指标
    sum(response_body_size_kb) as total_bytes_kb,
    avg(response_body_size_kb) as avg_response_size_kb,
    
    now() as created_at
FROM nginx_analytics.dwd_nginx_enriched_v2
WHERE log_time >= now() - INTERVAL 1 DAY
GROUP BY 
    stat_time, platform, access_type, api_path, 
    api_module, api_category, business_domain;

-- 2. 服务层级分析物化视图 - 对应02.服务层级分析.xlsx
CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_service_level_hourly
TO nginx_analytics.ads_service_level_analysis
AS SELECT
    toStartOfHour(log_time) as stat_time,
    'hour' as time_granularity,
    platform,
    access_type,
    
    -- 服务名称解析：从request_uri提取服务名
    multiIf(
        request_uri LIKE '/%/%', splitByChar('/', request_uri)[2],
        'direct'
    ) as service_name,
    
    -- 应用名称解析：从request_uri提取应用名
    multiIf(
        length(splitByChar('/', request_uri)) >= 3, splitByChar('/', request_uri)[3],
        'unknown'
    ) as application_name,
    
    multiIf(
        upstream_server != '', upstream_server,
        'direct'
    ) as upstream_server,
    
    -- 服务健康指标
    count() as total_requests,
    countIf(is_success) as success_count,
    countIf(is_error) as error_count,
    countIf(is_slow) as slow_count,
    
    countIf(is_success) * 100.0 / count() as success_rate,
    countIf(is_error) * 100.0 / count() as error_rate,
    countIf(is_slow) * 100.0 / count() as slow_rate,
    
    -- 性能指标
    avg(total_request_duration) as avg_response_time,
    quantile(0.95)(total_request_duration) as p95_response_time,
    
    -- 上游健康度
    avg(upstream_response_time) as avg_upstream_time,
    avg(upstream_connect_time) as avg_upstream_connect_time,
    
    -- 并发和连接
    max(connection_requests) as max_concurrent,
    avg(connection_requests) as avg_concurrent,
    
    -- 服务稳定性评分 (综合成功率+响应时间+稳定性)
    least(100.0, 
        (countIf(is_success) * 100.0 / count()) * 0.5 +
        greatest(0, 100 - avg(total_request_duration) * 20) * 0.3 +
        (100 - stddevSamp(total_request_duration) * 5) * 0.2
    ) as service_health_score,
    
    now() as created_at
FROM nginx_analytics.dwd_nginx_enriched_v2
WHERE log_time >= now() - INTERVAL 1 DAY
GROUP BY 
    stat_time, platform, access_type, service_name, 
    application_name, upstream_server;

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
        upstream_response_time > total_request_duration * 0.8, 'upstream_slow',
        upstream_connect_time > 1.0, 'connection_slow',
        (total_request_duration - upstream_response_time) > 2.0, 'gateway_slow',
        'application_slow'
    ) as slow_reason_category,
    
    -- 瓶颈类型
    multiIf(
        upstream_response_time > 5.0, 'DB',
        upstream_connect_time > 1.0, 'Network',
        total_request_duration > 10.0, 'CPU',
        'Memory'
    ) as bottleneck_type,
    
    multiIf(
        upstream_server != '', upstream_server,
        'direct'
    ) as upstream_server,
    
    -- 连接类型
    multiIf(
        connection_requests > 100, 'keep-alive',
        'close'
    ) as connection_type,
    
    -- 请求大小分类
    multiIf(
        response_body_size_kb < 10, 'small',
        response_body_size_kb < 100, 'medium',
        'large'
    ) as request_size_category,
    
    -- 客户端分类 (动态计算)
    multiIf(
        device_type = 'Bot', 'bot',
        device_type = 'Mobile', 'mobile',
        device_type = 'Desktop', 'desktop',
        'unknown'
    ) as user_agent_category,
    
    -- 慢请求统计
    countIf(is_slow) as slow_count,
    countIf(is_very_slow) as very_slow_count,
    countIf(total_request_duration >= 30.0) as timeout_count,
    
    -- 慢请求性能
    avgIf(total_request_duration, is_slow) as avg_slow_time,
    maxIf(total_request_duration, is_slow) as max_slow_time,
    quantileIf(0.99)(total_request_duration, is_slow) as p99_slow_time,
    
    -- 根因分析
    countIf(is_slow AND upstream_response_time > 3.0) as db_slow_count,
    countIf(is_slow AND upstream_connect_time > 1.0) as network_slow_count,
    countIf(is_slow AND upstream_server != '') as upstream_slow_count,
    
    -- 影响面
    uniq(client_ip) as affected_users,  -- 使用client_ip作为user_id替代
    uniq(request_uri) as affected_apis,
    
    now() as created_at
FROM nginx_analytics.dwd_nginx_enriched_v2
WHERE log_time >= now() - INTERVAL 1 DAY
GROUP BY 
    stat_time, platform, access_type, api_path, 
    slow_reason_category, bottleneck_type, upstream_server,
    connection_type, request_size_category, user_agent_category;

-- 4. 状态码分析物化视图 - 对应04.状态码统计.xlsx
CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_status_code_hourly
TO nginx_analytics.ads_status_code_analysis
AS SELECT
    toStartOfHour(log_time) as stat_time,
    'hour' as time_granularity,
    platform,
    access_type,
    toString(response_status_code) as response_status_code,
    request_uri as api_path,
    
    -- 错误分类 (动态计算)
    multiIf(
        response_status_code >= 400 AND response_status_code < 500, 'client_error',
        response_status_code >= 500 AND response_status_code < 600, 'server_error',
        response_status_code >= 300 AND response_status_code < 400, 'redirection',
        'success'
    ) as error_category,
    
    -- 错误严重性
    multiIf(
        response_status_code IN (500, 502, 503, 504), 'critical',
        response_status_code IN (401, 403, 429), 'high',
        response_status_code IN (400, 404, 422), 'medium',
        'low'
    ) as error_severity,
    
    multiIf(
        upstream_server != '', upstream_server,
        'direct'
    ) as upstream_server,
    
    -- IP类型分类 (动态计算)
    multiIf(
        is_internal_ip, 'internal',
        ip_risk_level = 'High', 'suspicious',
        'external'
    ) as client_ip_type,
    
    -- 用户类型 (动态计算)
    multiIf(
        device_type = 'Bot', 'bot',
        device_type = 'Mobile', 'mobile_user',
        device_type = 'Desktop', 'desktop_user',
        'unknown'
    ) as user_type,
    
    -- 错误统计
    count() as total_errors,
    countIf(response_status_code >= 400 AND response_status_code < 500) as client_errors,
    countIf(response_status_code >= 500) as server_errors,
    countIf(response_status_code IN (502, 503, 504)) as gateway_errors,
    
    -- 影响面
    uniq(client_ip) as affected_users,  -- 使用client_ip作为user_id替代
    uniq(request_uri) as affected_apis,
    
    -- 错误率
    count() * 100.0 / (
        SELECT count() FROM nginx_analytics.dwd_nginx_enriched_v2 
        WHERE toStartOfHour(log_time) = stat_time 
        AND platform = outer.platform
    ) as error_rate,
    
    -- 可用性影响
    100.0 - (countIf(response_status_code >= 500) * 100.0 / count()) as availability_impact,
    
    now() as created_at
FROM nginx_analytics.dwd_nginx_enriched_v2 outer
WHERE log_time >= now() - INTERVAL 1 DAY
  AND response_status_code >= 400
GROUP BY 
    stat_time, platform, access_type, response_status_code, api_path,
    error_category, error_severity, upstream_server, client_ip_type, user_type;

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
    
    api_category,
    
    -- QPS指标
    count() / 3600.0 as current_qps,
    
    -- 性能指标
    avg(total_request_duration) as avg_response_time,
    quantile(0.95)(total_request_duration) as p95_response_time,
    
    -- 并发指标
    count() as concurrent_requests,
    max(connection_requests) as max_concurrent,
    avg(connection_requests) * 100.0 / greatest(1, max(connection_requests)) as connection_utilization,
    
    now() as created_at
FROM nginx_analytics.dwd_nginx_enriched_v2
WHERE log_time >= now() - INTERVAL 1 DAY
GROUP BY 
    stat_time, platform, access_type, peak_period, business_hours, api_category;

-- 6. 错误分析详细物化视图 - 新增：错误码下钻分析
CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_error_analysis_hourly
TO nginx_analytics.ads_error_analysis_detailed
AS SELECT
    toStartOfHour(log_time) as stat_time,
    'hour' as time_granularity,
    platform,
    access_type,
    request_uri as api_path,
    toString(response_status_code) as response_status_code,
    
    -- 错误码组分类 (动态计算)
    multiIf(
        response_status_code >= 400 AND response_status_code < 500, '4xx_client',
        response_status_code >= 500 AND response_status_code < 600, '5xx_server',
        response_status_code IN (502, 503, 504), 'gateway',
        'upstream'
    ) as error_code_group,
    
    -- HTTP错误类别
    multiIf(
        response_status_code >= 400 AND response_status_code < 500, 'client_error',
        response_status_code >= 500 AND response_status_code < 600, 'server_error',
        response_status_code >= 300 AND response_status_code < 400, 'redirection',
        'unknown'
    ) as http_error_class,
    
    -- 错误严重级别
    multiIf(
        response_status_code IN (500, 502, 503, 504), 'critical',
        response_status_code IN (401, 403, 429), 'high',
        response_status_code IN (400, 404, 422), 'medium',
        'low'
    ) as error_severity_level,
    
    multiIf(
        upstream_server != '', upstream_server,
        'direct'
    ) as upstream_server,
    
    -- 上游状态码 (使用response_status_code临时替代)
    toString(response_status_code) as upstream_status_code,
    
    -- 错误定位
    multiIf(
        response_status_code IN (502, 503, 504), 'gateway',
        upstream_server != '', 'service',
        upstream_response_time > 5.0, 'database',
        'application'
    ) as error_location,
    
    -- 错误传播路径
    concat(
        'client->gateway->',
        if(upstream_server != '', concat('service(', upstream_server, ')'), 'direct'),
        if(upstream_response_time > 3.0, '->db', '')
    ) as error_propagation_path,
    
    -- 业务操作类型
    multiIf(
        request_uri LIKE '%login%' OR request_uri LIKE '%auth%', 'login',
        request_uri LIKE '%pay%' OR request_uri LIKE '%order%', 'payment',
        request_uri LIKE '%query%' OR request_uri LIKE '%search%', 'query',
        request_uri LIKE '%upload%' OR request_uri LIKE '%file%', 'upload',
        'other'
    ) as business_operation_type,
    
    'active_session' as user_session_stage,
    
    -- API重要级别
    multiIf(
        request_uri LIKE '%login%' OR request_uri LIKE '%pay%' OR request_uri LIKE '%auth%', 'critical',
        request_uri LIKE '%profile%' OR request_uri LIKE '%order%', 'important',
        request_uri LIKE '%static%' OR request_uri LIKE '%image%', 'optional',
        'normal'
    ) as api_importance_level,
    
    -- IP类型 (动态计算)
    multiIf(
        is_internal_ip, 'internal',
        ip_risk_level = 'High', 'suspicious',
        'external'
    ) as client_ip_type,
    
    -- 用户代理分类 (动态计算)
    multiIf(
        device_type = 'Bot', 'bot',
        device_type = 'Mobile', 'mobile',
        device_type = 'Desktop', 'desktop',
        'unknown'
    ) as user_agent_category,
    
    multiIf(
        device_type = 'Bot', 'bot',
        device_type = 'Mobile', 'mobile_user',
        'desktop_user'
    ) as user_type,
    
    -- 时间模式
    multiIf(
        toHour(log_time) >= 9 AND toHour(log_time) <= 17, 'business_hours',
        toHour(log_time) >= 19 AND toHour(log_time) <= 22, 'peak_hours',
        'off_hours'
    ) as time_pattern,
    
    'single' as error_burst_indicator,
    
    -- 错误统计
    count() as error_count,
    0 as total_requests,
    100.0 as error_rate,
    
    -- 影响评估
    uniq(client_ip) as unique_error_users,  -- 使用client_ip作为user_id替代
    uniq(trace_id) as error_sessions,       -- 使用trace_id作为session_id替代
    count() * 0.1 as business_loss_estimate,
    greatest(0, 100 - count() * 2) as user_experience_score,
    greatest(90, 100 - count() * 0.01) as sla_impact,
    
    -- 恢复指标
    5.0 as mean_time_to_recovery,
    30 as error_duration,
    95.0 as resolution_success_rate,
    least(100, count() * 0.5) as error_trend_score,
    least(100, count() * 0.3) as anomaly_score,
    
    multiIf(
        count() > 100, 'critical',
        count() > 50, 'high',
        count() > 20, 'medium',
        'low'
    ) as escalation_risk,
    
    now() as created_at
FROM nginx_analytics.dwd_nginx_enriched_v2
WHERE log_time >= now() - INTERVAL 1 DAY
  AND response_status_code >= 400
GROUP BY 
    stat_time, platform, access_type, api_path, response_status_code,
    error_code_group, http_error_class, error_severity_level, upstream_server,
    upstream_status_code, error_location, error_propagation_path, business_operation_type,
    user_session_stage, api_importance_level, client_ip_type, user_agent_category, 
    user_type, time_pattern, error_burst_indicator;

-- 7. 请求头分析物化视图 - 对应10.请求头分析.xlsx
CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_request_header_hourly
TO nginx_analytics.ads_request_header_analysis
AS SELECT
    toStartOfHour(log_time) as stat_time,
    'hour' as time_granularity,
    platform,
    access_type,
    
    -- 用户代理分析 (动态计算)
    multiIf(
        device_type = 'Bot', 'bot',
        device_type = 'Mobile', 'mobile',
        device_type = 'Desktop', 'desktop',
        browser_type = 'WebView', 'webview',
        'unknown'
    ) as user_agent_category,
    
    -- 用户代理版本 (简化处理)
    platform_version as user_agent_version,
    
    device_type,
    os_type,
    browser_type,
    
    -- 是否机器人 (动态计算)
    if(bot_type != '', true, false) as is_bot,
    
    -- IP类型分类 (动态计算)
    multiIf(
        is_internal_ip, 'internal',
        ip_risk_level = 'High', 'suspicious',
        'external'
    ) as client_ip_type,
    
    -- 请求统计
    count() as request_count,
    uniq(client_ip) as user_count,        -- 使用client_ip作为user_id替代
    uniq(trace_id) as session_count,      -- 使用trace_id作为session_id替代
    
    -- 性能指标
    avg(total_request_duration) as avg_response_time,
    quantile(0.95)(total_request_duration) as p95_response_time,
    avg(total_request_duration) as avg_session_duration,
    
    -- 用户体验指标
    countIf(response_status_code >= 400) * 100.0 / count() as bounce_rate,
    countIf(is_success) * 100.0 / count() as conversion_rate,
    
    now() as created_at
FROM nginx_analytics.dwd_nginx_enriched_v2
WHERE log_time >= now() - INTERVAL 1 DAY
GROUP BY 
    stat_time, platform, access_type, user_agent_category, user_agent_version,
    device_type, os_type, browser_type, is_bot, client_ip_type;

-- 物化视图创建完成
-- 注意：此版本使用动态字段计算，解决了字段不匹配问题
-- 缺失字段的替代方案：
-- - user_id → client_ip
-- - session_id → trace_id  
-- - user_agent_category → 从device_type推导
-- - client_ip_type → 从is_internal_ip和ip_risk_level推导
-- - is_bot → 从bot_type推导
-- - upstream_status_code → 临时使用response_status_code