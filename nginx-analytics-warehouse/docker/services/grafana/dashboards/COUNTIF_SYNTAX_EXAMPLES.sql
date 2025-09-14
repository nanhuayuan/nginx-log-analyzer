-- ClickHouse中的正确SQL语法示例

-- ❌ 错误写法:
SELECT (count(*) WHERE response_status_code = '200') * 100.0 / count(*) as success_rate
FROM nginx_analytics.dwd_nginx_enriched_v3
WHERE $__timeFilter(log_time)

-- ✅ 正确写法:
SELECT countIf(response_status_code = '200') * 100.0 / count(*) as success_rate
FROM nginx_analytics.dwd_nginx_enriched_v3
WHERE $__timeFilter(log_time);

-- ❌ 错误写法:
SELECT (count(*) WHERE total_request_duration > 3) * 100.0 / count(*) as slow_rate
FROM nginx_analytics.dwd_nginx_enriched_v3
WHERE $__timeFilter(log_time)

-- ✅ 正确写法:
SELECT countIf(total_request_duration > 3) * 100.0 / count(*) as slow_rate
FROM nginx_analytics.dwd_nginx_enriched_v3
WHERE $__timeFilter(log_time);

-- ❌ 错误写法:
SELECT (count(*) WHERE response_status_code >= '400') * 100.0 / count(*) as error_rate
FROM nginx_analytics.dwd_nginx_enriched_v3
WHERE $__timeFilter(log_time)

-- ✅ 正确写法:
SELECT countIf(response_status_code >= '400') * 100.0 / count(*) as error_rate
FROM nginx_analytics.dwd_nginx_enriched_v3
WHERE $__timeFilter(log_time);

-- 其他常用countIf示例:
SELECT
    count() as total_requests,
    countIf(response_status_code = '200') as success_requests,
    countIf(response_status_code >= '400') as error_requests,
    countIf(total_request_duration > 1) as slow_requests_1s,
    countIf(total_request_duration > 3) as slow_requests_3s,
    countIf(response_status_code = '404') as not_found_requests
FROM nginx_analytics.dwd_nginx_enriched_v3
WHERE $__timeFilter(log_time);
