# Dashboard时间范围配置指南

## 数据实际时间范围
- **开始时间**: 2025-08-29 07:15:37 (UTC+8)
- **结束时间**: 2025-08-29 10:10:30 (UTC+8)
- **数据量**: 50万条记录
- **时长**: 约3小时

## Dashboard默认时间范围
所有Dashboard已设置为：
- **从**: 2025-08-29T07:00:00.000Z
- **到**: 2025-08-29T10:30:00.000Z

## 使用建议

### 1. 查看数据的最佳时间范围
```
绝对时间: 2025-08-29 07:00:00 ~ 2025-08-29 10:30:00
相对时间: 由于数据是历史数据，建议使用绝对时间
```

### 2. 测试查询
在Grafana Explore中测试查询：
```sql
-- 验证数据总量
SELECT count(*) FROM nginx_analytics.dwd_nginx_enriched_v3
WHERE log_time >= '2025-08-29 07:15:37' AND log_time <= '2025-08-29 10:10:30'

-- 按平台分组
SELECT platform, count(*)
FROM nginx_analytics.dwd_nginx_enriched_v3
WHERE log_time >= '2025-08-29 07:15:37' AND log_time <= '2025-08-29 10:10:30'
GROUP BY platform
```

### 3. Dashboard使用注意事项
- 所有Dashboard默认刷新已设置为手动(`refresh: false`)
- 时间选择器已设置为合适的数据范围
- 如需查看实时数据，请调整时间范围为"Last 6 hours"或"Last 1 day"

### 4. 权限变量设置
- `$tenant_code`: 选择"All"或"default_tenant"
- `$platform`: 可选择"All"、"Android"、"iOS"、"unknown"
- `$api_category`: 可选择"All"或具体API类别

### 5. 常见问题
- **显示"No data"**: 检查时间范围是否覆盖2025-08-29的数据时间
- **查询超时**: 尝试缩小时间范围或添加更多过滤条件
- **数据不准确**: 确认变量选择正确，特别是租户和平台过滤器

## 时间宏变量说明
Dashboard中使用的时间宏：
- `$__timeFilter(log_time)`: 自动根据时间选择器生成WHERE条件
- `$__timeInterval(log_time)`: 自动计算合适的时间间隔用于GROUP BY
- `$__fromTime` / `$__toTime`: 获取时间范围的开始/结束时间
