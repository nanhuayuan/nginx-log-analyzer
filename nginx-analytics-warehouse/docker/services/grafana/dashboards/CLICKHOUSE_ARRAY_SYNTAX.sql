-- ClickHouse数组函数正确用法

-- ❌ 错误的数组切片语法:
SELECT groupArray(response_status_code)[1:5] as status_codes
FROM nginx_analytics.dwd_nginx_enriched_v3;

-- ✅ 正确的数组切片语法:
SELECT arraySlice(groupArray(response_status_code), 1, 5) as status_codes
FROM nginx_analytics.dwd_nginx_enriched_v3;

-- ✅ 常用数组函数示例:

-- 1. 获取数组前N个元素
SELECT arraySlice(groupArray(response_status_code), 1, 3) as top3_status
FROM nginx_analytics.dwd_nginx_enriched_v3;

-- 2. 去重数组
SELECT groupUniqArray(response_status_code) as unique_status
FROM nginx_analytics.dwd_nginx_enriched_v3;

-- 3. 限制数组大小的去重
SELECT groupUniqArray(3)(response_status_code) as top3_unique_status
FROM nginx_analytics.dwd_nginx_enriched_v3;

-- 4. 数组连接为字符串
SELECT arrayStringConcat(groupArray(response_status_code), ',') as status_list
FROM nginx_analytics.dwd_nginx_enriched_v3;

-- 5. 数组长度
SELECT length(groupArray(response_status_code)) as status_count
FROM nginx_analytics.dwd_nginx_enriched_v3;

-- 6. 简化版本（推荐用于Dashboard）
-- 避免复杂的数组操作，直接显示有用信息
SELECT
    request_uri_normalized as api,
    count() as total_requests,
    countIf(response_status_code >= '400') as error_count,
    countIf(response_status_code = '404') as not_found,
    countIf(response_status_code = '500') as server_error,
    -- 使用简单的聚合而不是数组切片
    groupUniqArray(3)(response_status_code) as error_status_samples
FROM nginx_analytics.dwd_nginx_enriched_v3
WHERE response_status_code >= '400'
GROUP BY request_uri_normalized
ORDER BY error_count DESC;
