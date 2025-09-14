-- 修复后的标准SQL查询示例

-- 1. API类别统计
SELECT
    api_category as `API类别`,
    count() as `请求数`,
    countIf(response_status_code = '200') as `成功请求`,
    countIf(response_status_code >= '400') as `错误请求`,
    countIf(response_status_code >= '400') / count() * 100 as `错误率(%)`,
    avg(total_request_duration * 1000) as `平均响应时间(ms)`,
    quantile(0.95)(total_request_duration * 1000) as `P95响应时间(ms)`
FROM nginx_analytics.dwd_nginx_enriched_v3
WHERE $__timeFilter(log_time)
GROUP BY api_category
ORDER BY count() DESC
LIMIT 10;

-- 2. 平台统计
SELECT
    platform as `平台`,
    count() as `请求数`
FROM nginx_analytics.dwd_nginx_enriched_v3
WHERE $__timeFilter(log_time)
GROUP BY platform
ORDER BY count() DESC;

-- 3. 时间序列
SELECT
    $__timeInterval(log_time) as time,
    platform as `平台`,
    count() as `请求数`
FROM nginx_analytics.dwd_nginx_enriched_v3
WHERE $__timeFilter(log_time)
GROUP BY time, platform
ORDER BY time;
