-- ==========================================
-- 物化视图层 - 实时数据聚合和预计算
-- ==========================================

-- 1. 系统概览实时物化视图 - 从DWD到ADS自动聚合
CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_system_overview_hourly
TO nginx_analytics.ads_system_overview_hourly
AS SELECT
    toStartOfHour(log_time) as stat_time,
    platform,
    count() as total_requests,
    countIf(is_success) as success_requests,  
    countIf(is_error) as error_requests,
    countIf(is_slow) as slow_requests,
    countIf(is_bot) as bot_requests,
    uniq(client_ip) as unique_ips,
    uniq(user_id) as unique_users,
    
    -- 性能指标计算
    avg(request_time) as avg_response_time,
    quantile(0.5)(request_time) as p50_response_time,
    quantile(0.95)(request_time) as p95_response_time,
    quantile(0.99)(request_time) as p99_response_time,
    max(request_time) as max_response_time,
    
    -- 流量指标
    avg(bytes_sent) as avg_bytes_sent,
    sum(bytes_sent) as total_bytes_sent,
    
    -- 质量指标
    countIf(is_success) * 100.0 / count() as success_rate,
    countIf(is_error) * 100.0 / count() as error_rate,
    countIf(is_slow) * 100.0 / count() as slow_rate,
    
    -- SLA可用性 (99.9% 基准)
    if(countIf(response_status >= 500) * 100.0 / count() < 0.1, 99.9, 
       100.0 - (countIf(response_status >= 500) * 100.0 / count())) as availability,
       
    -- Apdex用户体验指数 (满意阈值1.5s, 容忍阈值6s)
    (countIf(request_time <= 1.5) + countIf(request_time > 1.5 AND request_time <= 6.0) * 0.5) / count() as apdex_score,
    
    now() as created_at
FROM nginx_analytics.dwd_nginx_enriched_v2
WHERE log_time >= now() - INTERVAL 7 DAY  -- 只处理最近7天数据，避免历史数据重复计算
GROUP BY stat_time, platform;

-- 2. API性能实时物化视图
CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_api_performance_hourly  
TO nginx_analytics.ads_api_performance_hourly
AS SELECT
    toStartOfHour(log_time) as stat_time,
    platform,
    api_category,
    request_path as api_path,
    api_module,
    count() as request_count,
    countIf(is_success) as success_count,
    countIf(is_error) as error_count,
    countIf(is_slow) as slow_count,
    
    -- 性能指标
    avg(request_time) as avg_response_time,
    quantile(0.5)(request_time) as p50_response_time,
    quantile(0.95)(request_time) as p95_response_time,
    quantile(0.99)(request_time) as p99_response_time,
    
    -- QPS计算
    count() / 3600.0 as qps,
    countIf(is_error) * 100.0 / count() as error_rate,
    countIf(is_slow) * 100.0 / count() as slow_rate,
    
    -- 业务价值权重 (核心API权重更高)
    multiIf(
        api_category = 'Core_Business', 10.0,
        api_category = 'User_Auth', 8.0,
        api_category = 'Payment', 9.0,
        api_category = 'Static_Resource', 2.0,
        5.0
    ) as business_value,
    
    -- API健康度评分 (综合成功率、响应时间、稳定性)
    least(100.0, 
        (countIf(is_success) * 100.0 / count()) * 0.4 +           -- 40% 成功率
        greatest(0, 100 - avg(request_time) * 20) * 0.3 +         -- 30% 响应时间  
        (100 - stddevSamp(request_time) * 10) * 0.3               -- 30% 稳定性
    ) as health_score,
    
    now() as created_at
FROM nginx_analytics.dwd_nginx_enriched_v2  
WHERE log_time >= now() - INTERVAL 7 DAY
GROUP BY stat_time, platform, api_category, api_path, api_module;

-- 3. 异常检测实时物化视图 - 基于统计学异常检测
CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_anomaly_detection_realtime
TO nginx_analytics.ads_anomaly_detection_realtime  
AS SELECT
    now() as detect_time,
    
    -- 异常类型识别
    multiIf(
        error_rate > 10 AND error_rate > historical_error_rate * 3, 'error_spike',
        avg_response_time > 5 AND avg_response_time > historical_avg_time * 2, 'slow_spike', 
        request_count > historical_request_count * 3, 'traffic_spike',
        suspicious_ip_ratio > 0.3, 'security',
        'performance'
    ) as anomaly_type,
    
    platform,
    api_path,
    
    -- 严重级别
    multiIf(
        error_rate > 50 OR avg_response_time > 30, 'critical',
        error_rate > 20 OR avg_response_time > 10, 'high', 
        error_rate > 10 OR avg_response_time > 5, 'medium',
        'low'
    ) as severity,
    
    current_metric as current_value,
    historical_baseline as baseline_value,
    current_metric / historical_baseline as deviation_ratio,
    
    uniq(user_id) as affected_users,
    5 as duration_minutes,  -- 固定5分钟窗口
    
    -- 异常描述
    concat(
        'API: ', api_path, 
        ' 平台: ', platform,
        ' 异常指标: ', toString(round(current_metric, 2)),
        ' 基线: ', toString(round(historical_baseline, 2))
    ) as description,
    
    0 as is_resolved,
    null as resolve_time,
    now() as created_at

FROM (
    SELECT
        platform,
        request_path as api_path,
        count() as request_count,
        countIf(is_error) * 100.0 / count() as error_rate,
        avg(request_time) as avg_response_time,
        countIf(is_bot OR client_ip_type = 'suspicious') * 1.0 / count() as suspicious_ip_ratio,
        uniq(user_id) as unique_users,
        
        -- 当前值（选择最主要的异常指标）
        greatest(
            countIf(is_error) * 100.0 / count(),
            avg(request_time),
            count() / 300.0  -- 5分钟QPS
        ) as current_metric,
        
        -- 历史基线（过去7天同时段平均值）
        (SELECT avg(value) FROM (
            SELECT 
                greatest(
                    countIf(is_error) * 100.0 / count(),
                    avg(request_time), 
                    count() / 300.0
                ) as value
            FROM nginx_analytics.dwd_nginx_enriched_v2 
            WHERE log_time BETWEEN now() - INTERVAL 7 DAY AND now() - INTERVAL 1 HOUR
                AND platform = outer.platform 
                AND request_path = outer.api_path
                AND toHour(log_time) = toHour(now())
            GROUP BY toDate(log_time)
        )) as historical_baseline,
        
        -- 历史对比指标  
        (SELECT avg(error_rate) FROM (
            SELECT countIf(is_error) * 100.0 / count() as error_rate
            FROM nginx_analytics.dwd_nginx_enriched_v2
            WHERE log_time BETWEEN now() - INTERVAL 7 DAY AND now() - INTERVAL 1 HOUR  
                AND platform = outer.platform
                AND request_path = outer.api_path
            GROUP BY toDate(log_time)
        )) as historical_error_rate,
        
        (SELECT avg(avg_time) FROM (
            SELECT avg(request_time) as avg_time  
            FROM nginx_analytics.dwd_nginx_enriched_v2
            WHERE log_time BETWEEN now() - INTERVAL 7 DAY AND now() - INTERVAL 1 HOUR
                AND platform = outer.platform
                AND request_path = outer.api_path  
            GROUP BY toDate(log_time)
        )) as historical_avg_time,
        
        (SELECT avg(req_count) FROM (
            SELECT count() as req_count
            FROM nginx_analytics.dwd_nginx_enriched_v2 
            WHERE log_time BETWEEN now() - INTERVAL 7 DAY AND now() - INTERVAL 1 HOUR
                AND platform = outer.platform
                AND request_path = outer.api_path
            GROUP BY toDate(log_time), toHour(log_time)
        )) as historical_request_count
        
    FROM nginx_analytics.dwd_nginx_enriched_v2 outer
    WHERE log_time >= now() - INTERVAL 5 MINUTE  -- 最近5分钟的数据
    GROUP BY platform, request_path
) 
WHERE 
    -- 只有真正异常的数据才插入
    (error_rate > greatest(10, historical_error_rate * 2) 
     OR avg_response_time > greatest(3, historical_avg_time * 2)
     OR request_count > historical_request_count * 2.5
     OR suspicious_ip_ratio > 0.2)
    AND request_count >= 10;  -- 最少10个请求才有统计意义

-- 4. 平台对比实时物化视图
CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_platform_comparison_hourly
TO nginx_analytics.ads_platform_comparison_hourly
AS SELECT
    toStartOfHour(log_time) as stat_time,
    platform,
    count() as total_requests,
    uniq(user_id) as unique_users,
    avg(request_time) as avg_response_time,
    countIf(is_success) * 100.0 / count() as success_rate,
    countIf(response_status >= 500) * 100.0 / count() as crash_rate,
    
    -- 用户满意度 (基于Apdex改进)
    (countIf(request_time <= 2.0) + countIf(request_time > 2.0 AND request_time <= 8.0) * 0.5) / count() * 100 as user_satisfaction,
    
    -- 市场份额 (该平台请求占总请求的比例)
    count() * 100.0 / (SELECT count() FROM nginx_analytics.dwd_nginx_enriched_v2 WHERE log_time = outer.log_time) as market_share,
    
    -- 增长率 (与上周同期对比)
    (count() - (SELECT count() FROM nginx_analytics.dwd_nginx_enriched_v2 
                WHERE toStartOfHour(log_time) = toStartOfHour(outer.log_time) - INTERVAL 7 DAY
                AND platform = outer.platform)) * 100.0 / 
    greatest(1, (SELECT count() FROM nginx_analytics.dwd_nginx_enriched_v2 
                 WHERE toStartOfHour(log_time) = toStartOfHour(outer.log_time) - INTERVAL 7 DAY
                 AND platform = outer.platform)) as growth_rate,
                 
    -- 留存率 (基于session_id的简化计算)
    uniq(session_id) * 100.0 / greatest(1, uniq(user_id)) as retention_rate,
    
    now() as created_at
    
FROM nginx_analytics.dwd_nginx_enriched_v2 outer
WHERE log_time >= now() - INTERVAL 7 DAY
GROUP BY stat_time, platform;

-- 5. 实时性能监控视图 (1分钟级别)
CREATE VIEW IF NOT EXISTS nginx_analytics.view_realtime_performance
AS SELECT
    toStartOfMinute(log_time) as minute,
    platform,
    api_category,
    count() as requests_per_minute,
    avg(request_time) as avg_response_time,
    countIf(is_error) as error_count,  
    countIf(is_slow) as slow_count,
    uniq(client_ip) as unique_ips,
    countIf(is_success) * 100.0 / count() as success_rate
FROM nginx_analytics.dwd_nginx_enriched_v2
WHERE log_time >= now() - INTERVAL 1 HOUR  -- 只看最近1小时实时数据
GROUP BY minute, platform, api_category
ORDER BY minute DESC, requests_per_minute DESC;

-- 6. TOP排行榜视图 (实时热点分析)  
CREATE VIEW IF NOT EXISTS nginx_analytics.view_top_apis_realtime
AS SELECT
    platform,
    request_path as api_path,
    count() as request_count,
    avg(request_time) as avg_response_time,
    countIf(is_error) as error_count,
    countIf(is_error) * 100.0 / count() as error_rate,
    uniq(client_ip) as unique_visitors,
    row_number() OVER (PARTITION BY platform ORDER BY count() DESC) as rank_by_requests,
    row_number() OVER (PARTITION BY platform ORDER BY avg(request_time) DESC) as rank_by_slowness
FROM nginx_analytics.dwd_nginx_enriched_v2
WHERE log_time >= now() - INTERVAL 15 MINUTE  -- 最近15分钟热点
GROUP BY platform, api_path
HAVING request_count >= 5  -- 至少5次请求
ORDER BY platform, request_count DESC;

-- 视图注释：mv_system_overview_hourly 系统概览物化视图-小时级聚合，自动从DWD层实时计算-- 视图注释：mv_api_performance_hourly API性能物化视图-包含健康度评分和业务价值权重-- 视图注释：mv_anomaly_detection_realtime 异常检测物化视图-基于统计学方法实时识别异常-- 视图注释：view_realtime_performance 实时性能监控视图-1分钟级别，用于实时大屏展示-- 视图注释：view_top_apis_realtime TOP排行榜视图-热点API实时识别';