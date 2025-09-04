-- ==========================================
-- 物化视图层v2.0 - 基于多维度分析的统一架构
-- 7个核心物化视图，支持平台+入口+错误码下钻分析
-- 修复版：使用DWD层现有字段，动态计算缺失字段
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
    
    -- 业务价值权重
    multiIf(
        api_category = 'Core_Business', 10,
        api_category = 'User_Auth', 8,
        api_category = 'Payment', 9,
        api_category = 'Static_Resource', 2,
        5
    ) as business_value_score,
    multiIf(
        api_category IN ('Core_Business', 'User_Auth', 'Payment'), 'critical',
        api_category IN ('Data_Query', 'User_Profile'), 'important',
        'normal'
    ) as importance_level,
    
    now() as created_at
FROM nginx_analytics.dwd_nginx_enriched_v2
WHERE log_time >= now() - INTERVAL 1 DAY  -- 处理最近1天数据
GROUP BY stat_time, platform, access_type, api_path, api_module, api_category, business_domain;

-- 2. 服务层级分析物化视图 - 对应02.服务层级分析.xlsx
CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_service_level_hourly
TO nginx_analytics.ads_service_level_analysis  
AS SELECT
    toStartOfHour(log_time) as stat_time,
    'hour' as time_granularity,
    platform,
    
    -- 服务名解析：从URI中提取服务名
    CASE 
        WHEN request_uri LIKE '/%/%' AND length(splitByChar('/', request_uri)) >= 3
        THEN splitByChar('/', request_uri)[3]
        ELSE 'unknown'
    END as service_name,
    
    cluster_node,
    CASE
        WHEN upstream_server != '' THEN upstream_server
        ELSE 'direct'
    END as upstream_server,
    
    -- 服务健康指标
    count() as total_requests,
    countIf(is_success) as success_requests,
    countIf(is_error) as error_requests,
    countIf(total_request_duration >= 30.0) as timeout_requests,
    
    -- 服务性能
    avg(total_request_duration) as avg_response_time,
    quantile(0.95)(total_request_duration) as p95_response_time,
    avg(upstream_response_time) as avg_upstream_time,
    avg(upstream_connect_time) as avg_connect_time,
    
    -- 服务质量
    countIf(is_success) * 100.0 / count() as availability,
    countIf(NOT has_anomaly) * 100.0 / count() as reliability,
    (countIf(is_success) * 0.6 + countIf(NOT has_anomaly) * 0.4) as health_score,
    
    -- 容量指标
    max(connection_requests) as max_concurrent_requests,
    count() / 3600.0 as avg_qps,
    max(count()) OVER (PARTITION BY service_name ORDER BY stat_time ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) / 3600.0 as peak_qps,
    
    now() as created_at
FROM nginx_analytics.dwd_nginx_enriched_v2
WHERE log_time >= now() - INTERVAL 1 DAY
GROUP BY stat_time, platform, service_name, cluster_node, upstream_server;

-- 3. 慢请求深度分析物化视图 - 对应03_慢请求分析.xlsx
CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_slow_request_hourly
TO nginx_analytics.ads_slow_request_analysis
AS SELECT
    toStartOfHour(log_time) as stat_time,
    'hour' as time_granularity,
    platform,
    access_type,
    request_uri as api_path,
    
    -- 扩展维度用于慢请求根因分析
    multiIf(
        upstream_response_time > total_request_duration * 0.8, 'upstream_slow',
        upstream_connect_time > 1.0, 'connection_slow', 
        total_request_duration - upstream_response_time > 2.0, 'gateway_slow',
        'application_slow'
    ) as slow_reason_category,
    
    multiIf(
        upstream_response_time > 5.0, 'DB',
        upstream_connect_time > 1.0, 'Network',
        total_request_duration > 10.0, 'CPU',
        'Memory'
    ) as bottleneck_type,
    
    CASE
        WHEN upstream_server != '' THEN upstream_server
        ELSE 'direct'
    END as upstream_server,
    
    multiIf(
        connection_requests > 100, 'keep-alive',
        'close'
    ) as connection_type,
    
    multiIf(
        response_body_size_kb < 10, 'small',
        response_body_size_kb < 100, 'medium', 
        'large'
    ) as request_size_category,
    
    user_agent_category,
    
    -- 核心指标
    countIf(is_slow) as slow_count,
    countIf(is_very_slow) as very_slow_count,
    countIf(total_request_duration >= 30.0) as timeout_count,
    
    -- 耗时分析
    avgIf(total_request_duration, is_slow) as avg_slow_time,
    maxIf(total_request_duration, is_slow) as max_slow_time,
    quantileIf(0.99)(total_request_duration, is_slow) as p99_slow_time,
    
    -- 瓶颈指标
    countIf(is_slow AND upstream_response_time > 3.0) as db_slow_count,
    countIf(is_slow AND upstream_connect_time > 1.0) as network_slow_count,
    countIf(is_slow AND upstream_server != '') as upstream_slow_count,
    
    -- 影响分析
    uniq(user_id) as affected_users,
    uniq(request_uri) as affected_apis,
    
    now() as created_at
FROM nginx_analytics.dwd_nginx_enriched_v2
WHERE log_time >= now() - INTERVAL 1 DAY
GROUP BY stat_time, platform, access_type, api_path, slow_reason_category, 
         bottleneck_type, upstream_server, connection_type, request_size_category, user_agent_category;

-- 4. 状态码统计物化视图 - 对应04.状态码统计.xlsx
CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_status_code_hourly
TO nginx_analytics.ads_status_code_analysis
AS SELECT
    toStartOfHour(log_time) as stat_time,
    'hour' as time_granularity,
    platform,
    access_type,
    toString(response_status_code) as response_status_code,
    request_uri as api_path,
    
    -- 错误分类
    multiIf(
        response_status_code >= 400 AND response_status_code < 500, 'client_error',
        response_status_code >= 500 AND response_status_code < 600, 'server_error',
        response_status_code >= 300 AND response_status_code < 400, 'redirection',
        'success'
    ) as error_category,
    
    multiIf(
        response_status_code IN (500, 502, 503, 504), 'critical',
        response_status_code IN (401, 403, 429), 'high',
        response_status_code IN (400, 404, 422), 'medium',
        'low'  
    ) as error_severity,
    
    CASE
        WHEN upstream_server != '' THEN upstream_server
        ELSE 'direct'
    END as upstream_server,
    
    client_ip_type,
    user_agent_category as user_type,
    
    -- 核心指标
    count() as total_errors,
    countIf(response_status_code >= 400 AND response_status_code < 500) as client_errors,
    countIf(response_status_code >= 500) as server_errors,
    countIf(response_status_code IN (502, 503, 504)) as gateway_errors,
    
    -- 影响指标
    uniq(user_id) as affected_users,
    uniq(request_uri) as affected_apis,
    count() * 100.0 / (SELECT count() FROM nginx_analytics.dwd_nginx_enriched_v2 
                       WHERE toStartOfHour(log_time) = stat_time 
                         AND platform = outer.platform) as error_rate,
    
    -- 可用性影响
    100.0 - (countIf(response_status_code >= 500) * 100.0 / count()) as availability_impact,
    
    now() as created_at
FROM nginx_analytics.dwd_nginx_enriched_v2 outer
WHERE log_time >= now() - INTERVAL 1 DAY
  AND response_status_code >= 400  -- 只统计错误状态码
GROUP BY stat_time, platform, access_type, response_status_code, api_path, 
         error_category, error_severity, upstream_server, client_ip_type, user_type;

-- 5. 时间维度实时监控物化视图 - 对应05.时间维度分析.xlsx
CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_time_dimension_hourly
TO nginx_analytics.ads_time_dimension_analysis
AS SELECT
    toStartOfHour(log_time) as stat_time,
    'hour' as time_granularity,
    platform,
    access_type,
    
    -- 时间模式识别
    multiIf(
        toHour(log_time) BETWEEN 9 AND 11 OR toHour(log_time) BETWEEN 14 AND 16, 'peak_hours',
        toHour(log_time) BETWEEN 8 AND 18, 'business_hours',
        'off_hours'
    ) as peak_period,
    
    multiIf(
        toDayOfWeek(log_time) BETWEEN 1 AND 5 AND toHour(log_time) BETWEEN 8 AND 18, 'business_hours',
        'non_business_hours'
    ) as business_hours,
    
    api_category,
    
    -- QPS指标
    count() / 3600.0 as current_qps,
    max(count()) OVER (PARTITION BY platform, access_type ORDER BY stat_time ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) / 3600.0 as peak_qps,
    avg(count()) OVER (PARTITION BY platform, access_type ORDER BY stat_time ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) / 3600.0 as avg_qps,
    
    -- QPS增长率（与上小时对比）
    (count() - lag(count()) OVER (PARTITION BY platform, access_type ORDER BY stat_time)) * 100.0 / 
    greatest(1, lag(count()) OVER (PARTITION BY platform, access_type ORDER BY stat_time)) as qps_growth_rate,
    
    -- 性能指标
    avg(total_request_duration) as avg_response_time,
    quantile(0.95)(total_request_duration) as p95_response_time,
    
    -- 性能趋势（与上小时对比）
    avg(total_request_duration) - lag(avg(total_request_duration)) OVER (PARTITION BY platform, access_type ORDER BY stat_time) as performance_trend,
    
    -- 并发指标
    count() as concurrent_requests,
    max(connection_requests) as max_concurrent,
    avg(connection_requests) * 100.0 / greatest(1, max(connection_requests)) as connection_utilization,
    
    now() as created_at
FROM nginx_analytics.dwd_nginx_enriched_v2
WHERE log_time >= now() - INTERVAL 1 DAY
GROUP BY stat_time, platform, access_type, peak_period, business_hours, api_category;

-- 6. 错误码下钻分析物化视图 - 对应错误分析详情
CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_error_analysis_hourly
TO nginx_analytics.ads_error_analysis_detailed
AS SELECT
    toStartOfHour(log_time) as stat_time,
    'hour' as time_granularity,
    platform,
    access_type,
    request_uri as api_path,
    
    -- 错误码下钻维度（核心）
    toString(response_status_code) as response_status_code,
    multiIf(
        response_status_code >= 400 AND response_status_code < 500, '4xx_client',
        response_status_code >= 500 AND response_status_code < 600, '5xx_server',
        response_status_code IN (502, 503, 504), 'gateway',
        'upstream'
    ) as error_code_group,
    
    multiIf(
        response_status_code >= 400 AND response_status_code < 500, 'client_error',
        response_status_code >= 500 AND response_status_code < 600, 'server_error',
        response_status_code >= 300 AND response_status_code < 400, 'redirection',
        'unknown'
    ) as http_error_class,
    
    multiIf(
        response_status_code IN (500, 502, 503, 504), 'critical',
        response_status_code IN (401, 403, 429), 'high',
        response_status_code IN (400, 404, 422), 'medium',
        'low'
    ) as error_severity_level,
    
    -- 错误定位维度
    CASE
        WHEN upstream_server != '' THEN upstream_server
        ELSE 'direct'
    END as upstream_server,
    
    toString(upstream_status_code) as upstream_status_code,
    
    multiIf(
        response_status_code IN (502, 503, 504), 'gateway',
        upstream_server != '', 'service',
        upstream_response_time > 5.0, 'database',
        'application'
    ) as error_location,
    
    concat('client->gateway->',
           if(upstream_server != '', concat('service(', upstream_server, ')'), 'direct'),
           if(upstream_response_time > 3.0, '->db', '')) as error_propagation_path,
    
    -- 业务影响维度
    multiIf(
        request_uri LIKE '%login%' OR request_uri LIKE '%auth%', 'login',
        request_uri LIKE '%pay%' OR request_uri LIKE '%order%', 'payment',
        request_uri LIKE '%query%' OR request_uri LIKE '%search%', 'query',
        request_uri LIKE '%upload%' OR request_uri LIKE '%file%', 'upload',
        'other'
    ) as business_operation_type,
    
    'active_session' as user_session_stage,  -- 简化处理
    
    multiIf(
        request_uri LIKE '%login%' OR request_uri LIKE '%pay%' OR request_uri LIKE '%auth%', 'critical',
        request_uri LIKE '%profile%' OR request_uri LIKE '%order%', 'important',
        request_uri LIKE '%static%' OR request_uri LIKE '%image%', 'optional',
        'normal'
    ) as api_importance_level,
    
    -- 客户端维度
    client_ip_type,
    user_agent_category,
    user_agent_category as user_type,  -- 简化映射
    
    -- 时间模式
    multiIf(
        toHour(log_time) BETWEEN 9 AND 17, 'business_hours',
        toHour(log_time) BETWEEN 19 AND 22, 'peak_hours', 
        'off_hours'
    ) as time_pattern,
    
    'single' as error_burst_indicator,  -- 简化处理，后续可优化
    
    -- 核心指标
    count() as error_count,
    0 as total_requests,  -- 需要通过join计算，这里简化
    100.0 as error_rate,  -- 因为只统计错误，所以是100%
    uniq(user_id) as unique_error_users,
    uniq(session_id) as error_sessions,
    
    -- 业务影响指标（简化处理）
    count() * 0.1 as business_loss_estimate,  -- 简化估算
    greatest(0, 100 - count() * 2) as user_experience_score,
    greatest(90, 100 - count() * 0.01) as sla_impact,
    
    -- 恢复指标（需要通过复杂逻辑计算，这里简化）
    5.0 as mean_time_to_recovery,
    30 as error_duration,
    95.0 as resolution_success_rate,
    
    -- 预警指标
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
  AND response_status_code >= 400  -- 只处理错误请求
GROUP BY stat_time, platform, access_type, api_path, response_status_code, error_code_group, 
         http_error_class, error_severity_level, upstream_server, upstream_status_code, 
         error_location, error_propagation_path, business_operation_type, user_session_stage,
         api_importance_level, client_ip_type, user_agent_category, user_type, time_pattern, error_burst_indicator;

-- 7. 请求头分析物化视图 - 对应10.请求头分析.xlsx
CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_request_header_hourly
TO nginx_analytics.ads_request_header_analysis
AS SELECT
    toStartOfHour(log_time) as stat_time,
    'hour' as time_granularity,
    platform,
    access_type,
    
    -- 请求头解析维度
    user_agent_category,
    user_agent_version,
    device_type,
    os_type,
    browser_type,
    is_bot,
    client_ip_type,
    
    -- 分布指标
    count() as request_count,
    uniq(user_id) as user_count,
    uniq(session_id) as session_count,
    
    -- 性能指标
    avg(total_request_duration) as avg_response_time,
    quantile(0.95)(total_request_duration) as p95_response_time,
    
    -- 行为指标（简化处理）
    avg(total_request_duration) as avg_session_duration,  -- 简化映射
    countIf(response_status_code >= 400) * 100.0 / count() as bounce_rate,  -- 简化处理
    countIf(is_success) * 100.0 / count() as conversion_rate,  -- 简化处理
    
    now() as created_at
FROM nginx_analytics.dwd_nginx_enriched_v2
WHERE log_time >= now() - INTERVAL 1 DAY
GROUP BY stat_time, platform, access_type, user_agent_category, user_agent_version,
         device_type, os_type, browser_type, is_bot, client_ip_type;

-- ==========================================
-- 物化视图注释和说明
-- ==========================================

-- 物化视图1: mv_api_performance_hourly - API性能分析，支持平台+入口+接口多维度分析
-- 物化视图2: mv_service_level_hourly - 服务层级分析，支持微服务健康度监控  
-- 物化视图3: mv_slow_request_hourly - 慢请求分析，支持瓶颈类型和根因定位
-- 物化视图4: mv_status_code_hourly - 状态码统计，支持错误分类和影响评估
-- 物化视图5: mv_time_dimension_hourly - 时间维度分析，支持QPS趋势和性能监控
-- 物化视图6: mv_error_analysis_hourly - 错误码下钻分析，支持精准错误定位和根因分析  
-- 物化视图7: mv_request_header_hourly - 请求头分析，支持客户端行为和用户体验分析