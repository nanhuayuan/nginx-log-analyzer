-- ==========================================
-- 修复后的测试SQL - 可以直接在Grafana Explore中测试
-- ==========================================

-- 1. 最简单的基础查询（无变量）
SELECT
    count(*) as total_requests
FROM nginx_analytics.dwd_nginx_enriched_v3
WHERE log_time >= now() - INTERVAL 1 DAY;

-- 2. 平台分组查询（无变量）
SELECT
    platform,
    count(*) as requests,
    avg(total_request_duration) as avg_response_time
FROM nginx_analytics.dwd_nginx_enriched_v3
WHERE log_time >= now() - INTERVAL 1 DAY
GROUP BY platform
ORDER BY requests DESC;

-- 3. 修复后的Dashboard查询格式（带变量）
SELECT
    toStartOfMinute(log_time) as time,
    platform,
    count(*) as requests
FROM nginx_analytics.dwd_nginx_enriched_v3
WHERE log_time >= now() - INTERVAL 6 HOUR
    AND ('default_tenant' = 'All' OR tenant_code = 'default_tenant')
    AND ('All' = 'All' OR platform = 'All')
    AND ('All' = 'All' OR api_category = 'All')
GROUP BY time, platform
ORDER BY time;

-- 4. 针对实际数据的测试查询
SELECT
    toStartOfMinute(log_time) as time,
    platform,
    count(*) as requests
FROM nginx_analytics.dwd_nginx_enriched_v3
WHERE log_time >= '2025-08-29 07:00:00'
    AND log_time <= '2025-08-29 11:00:00'
GROUP BY time, platform
ORDER BY time;

-- ==========================================
-- 变量设置建议
-- ==========================================
-- 在Grafana中设置变量时：
-- $tenant_code: 当前值设为 'default_tenant'
-- $platform: 当前值设为 'All'
-- $api_category: 当前值设为 'All'
