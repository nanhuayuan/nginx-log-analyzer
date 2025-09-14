-- ==========================================
-- 标准的Grafana ClickHouse查询格式
-- ==========================================

-- 1. 时间序列查询（time_series格式）
-- 用于图表显示
SELECT
    $__timeInterval(log_time) as time,
    platform,
    count(*) as requests
FROM nginx_analytics.dwd_nginx_enriched_v3
WHERE $__timeFilter(log_time)
    AND $__conditionalAll(tenant_code = '$tenant_code', $tenant_code)
    AND $__conditionalAll(platform IN ($platform), $platform)
GROUP BY time, platform
ORDER BY time;

-- 2. 单值查询（table格式）
-- 用于Stat面板
SELECT count(*) as value
FROM nginx_analytics.dwd_nginx_enriched_v3
WHERE $__timeFilter(log_time)
    AND $__conditionalAll(tenant_code = '$tenant_code', $tenant_code);

-- 3. 表格查询（table格式）
-- 用于Table面板
SELECT
    platform as "平台",
    count(*) as "请求数",
    avg(total_request_duration) as "平均响应时间"
FROM nginx_analytics.dwd_nginx_enriched_v3
WHERE $__timeFilter(log_time)
    AND $__conditionalAll(tenant_code = '$tenant_code', $tenant_code)
GROUP BY platform
ORDER BY count(*) DESC;

-- ==========================================
-- Grafana ClickHouse插件配置要求
-- ==========================================

Target配置格式：
{
  "datasource": {
    "type": "grafana-clickhouse-datasource",
    "uid": "${DS_CLICKHOUSE}"
  },
  "format": "time_series",  // 或 "table"
  "intervalFactor": 1,
  "rawSql": "SQL查询语句",
  "refId": "A"
}

注意事项：
1. format字段必须是 "time_series" 或 "table"
2. 时间序列查询必须有时间字段作为第一列
3. 数据源配置必须正确
4. $__timeFilter和$__timeInterval宏必须正确使用
