# Dashboard配置指南

## 数据源配置
所有Dashboard已更新为使用模板变量 `${DS_CLICKHOUSE}`，请确保在Grafana中：

1. 创建ClickHouse数据源
2. 数据源名称: `grafana-clickhouse-datasource`
3. 数据源UID: `fexr755u22ku8b` (或更新模板变量)

## 权限变量说明
每个Dashboard都包含以下变量：

- `$DS_CLICKHOUSE`: 数据源选择器
- `$tenant_code`: 租户过滤器 (支持多选)
- `$platform`: 平台过滤器 (支持多选)

## 使用说明
1. 所有查询都会自动根据选择的租户和平台进行过滤
2. 使用 `$__conditionalAll()` 宏来处理"All"选项
3. 时间过滤使用 `$__timeFilter(log_time)` 宏

## 故障排除
如果Dashboard显示"No data"：
1. 检查数据源配置是否正确
2. 确认表 `nginx_analytics.dwd_nginx_enriched_v3` 中有数据
3. 检查权限变量是否有有效值
