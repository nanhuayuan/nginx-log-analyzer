# Altinity ClickHouse 数据源配置

## 基础配置

**Data Source Settings:**
```
Name: ClickHouse-nginx-analytics
URL: http://localhost:8123
Access: Server (default)
```

**Database Settings:**
```
Database: nginx_analytics
Username: analytics_user
Password: analytics_password
```

**Advanced Settings (可选):**
```
Timeout: 60
Max idle connections: 2
Max open connections: 5
```

## 验证连接

配置完成后点击 "Save & Test"，应该显示：
- "Data source is working" (绿色消息)

## 示例查询语法

### 基础查询
```sql
-- 总请求数
SELECT count() FROM ods_nginx_raw

-- 按时间分组的请求量
SELECT 
    $timeSeries as t, 
    count() as requests
FROM ods_nginx_raw 
WHERE $timeFilter
GROUP BY t
ORDER BY t
```

### 状态码分析
```sql
SELECT 
    response_status_code as status,
    count() as requests
FROM ods_nginx_raw 
WHERE $timeFilter
GROUP BY status
ORDER BY requests DESC
```

### API性能分析
```sql
SELECT 
    request_uri as api,
    count() as requests,
    round(avg(total_request_time), 3) as avg_response_time,
    round(quantile(0.95)(total_request_time), 3) as p95_response_time
FROM ods_nginx_raw 
WHERE $timeFilter AND total_request_time > 0
GROUP BY api
ORDER BY avg_response_time DESC
LIMIT 10
```

### 错误率监控
```sql
SELECT 
    $timeSeries as t,
    countIf(response_status_code LIKE '4%' OR response_status_code LIKE '5%') * 100.0 / count() as error_rate
FROM ods_nginx_raw 
WHERE $timeFilter
GROUP BY t
ORDER BY t
```

## 创建仪表板

1. 创建新的Dashboard
2. 添加Panel
3. 选择ClickHouse数据源
4. 输入上述查询语句
5. 配置可视化类型（表格、图表、饼图等）

## 注意事项

- 使用 `$timeFilter` 自动应用Grafana的时间范围过滤
- 使用 `$timeSeries` 进行时间序列分组
- 对于大数据量，建议添加LIMIT子句
- 响应时间字段使用 `total_request_time`
- 状态码字段使用 `response_status_code`