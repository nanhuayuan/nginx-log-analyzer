-- 创建平台小时聚合物化视图
CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_platform_hourly
TO nginx_analytics.dws_platform_hourly
AS SELECT
    date_partition,
    hour_partition,
    platform,
    count(*) as total_requests,
    sum(is_success::UInt64) as success_requests,
    sum((NOT is_success)::UInt64) as error_requests,
    sum(is_slow::UInt64) as slow_requests,
    avg(response_time) as avg_response_time,
    quantile(0.5)(response_time) as p50_response_time,
    quantile(0.95)(response_time) as p95_response_time,
    quantile(0.99)(response_time) as p99_response_time,
    max(response_time) as max_response_time,
    (sum(is_success::UInt64) * 100.0 / count(*)) as success_rate,
    (sum((NOT is_success)::UInt64) * 100.0 / count(*)) as error_rate,
    (sum(is_slow::UInt64) * 100.0 / count(*)) as slow_rate,
    uniq(client_ip) as unique_ips,
    sum(response_size_kb) / 1024 as total_response_size_mb,
    now() as created_at,
    now() as updated_at
FROM nginx_analytics.dwd_nginx_enriched
GROUP BY date_partition, hour_partition, platform;

-- 创建API聚合物化视图
CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_api_hourly
TO nginx_analytics.dws_api_hourly
AS SELECT
    date_partition,
    hour_partition,
    request_uri,
    platform,
    api_category,
    count(*) as total_requests,
    sum(is_success::UInt64) as success_requests,
    sum((NOT is_success)::UInt64) as error_requests,
    sum(is_slow::UInt64) as slow_requests,
    avg(response_time) as avg_response_time,
    quantile(0.95)(response_time) as p95_response_time,
    max(response_time) as max_response_time,
    (sum(is_success::UInt64) * 100.0 / count(*)) as success_rate,
    (sum((NOT is_success)::UInt64) * 100.0 / count(*)) as error_rate,
    (sum(is_slow::UInt64) * 100.0 / count(*)) as slow_rate,
    any(application_name) as application_name,
    any(service_name) as service_name,
    now() as created_at,
    now() as updated_at
FROM nginx_analytics.dwd_nginx_enriched
GROUP BY date_partition, hour_partition, request_uri, platform, api_category;

-- 创建实时统计视图
CREATE VIEW IF NOT EXISTS nginx_analytics.v_realtime_stats AS
SELECT
    platform,
    count(*) as total_requests,
    sum(is_success::UInt64) as success_requests,
    sum((NOT is_success)::UInt64) as error_requests,
    sum(is_slow::UInt64) as slow_requests,
    avg(response_time) as avg_response_time,
    quantile(0.95)(response_time) as p95_response_time,
    (sum(is_success::UInt64) * 100.0 / count(*)) as success_rate,
    (sum((NOT is_success)::UInt64) * 100.0 / count(*)) as error_rate,
    (sum(is_slow::UInt64) * 100.0 / count(*)) as slow_rate,
    uniq(client_ip) as unique_ips
FROM nginx_analytics.dwd_nginx_enriched
WHERE timestamp >= now() - INTERVAL 1 HOUR
GROUP BY platform
ORDER BY total_requests DESC;

-- 创建异常检测视图
CREATE VIEW IF NOT EXISTS nginx_analytics.v_anomaly_detection AS
SELECT
    platform,
    api_category,
    toDate(timestamp) as date,
    toHour(timestamp) as hour,
    count(*) as requests,
    avg(response_time) as avg_response_time,
    quantile(0.95)(response_time) as p95_response_time,
    sum((NOT is_success)::UInt64) * 100.0 / count(*) as error_rate,
    sum(is_slow::UInt64) * 100.0 / count(*) as slow_rate,
    -- 标记异常条件
    CASE 
        WHEN avg(response_time) > 5.0 THEN 'high_response_time'
        WHEN sum((NOT is_success)::UInt64) * 100.0 / count(*) > 10.0 THEN 'high_error_rate'
        WHEN sum(is_slow::UInt64) * 100.0 / count(*) > 20.0 THEN 'high_slow_rate'
        ELSE 'normal'
    END as anomaly_status
FROM nginx_analytics.dwd_nginx_enriched
WHERE timestamp >= today() - INTERVAL 1 DAY
GROUP BY platform, api_category, date, hour
HAVING requests >= 10  -- 只统计有足够样本的数据
ORDER BY date DESC, hour DESC, requests DESC;