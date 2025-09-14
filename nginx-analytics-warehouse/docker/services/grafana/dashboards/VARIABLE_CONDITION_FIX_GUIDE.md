# 变量条件修复测试指南

## 问题分析
变量替换导致SQL语法错误：
- 多选变量被替换为 'value1','value2','value3'
- 生成了错误的SQL: platform = ''value1','value2','value3''

## 解决方案
**阶段1**: 移除所有变量条件，只保留基本查询
- 移除了tenant_code、platform、api_category等变量过滤
- 保留$__timeFilter(log_time)时间过滤
- 查询所有数据，不进行变量过滤

## 简化后的SQL示例：
```sql
-- 原来的复杂SQL：
SELECT platform, count(*) as requests
FROM nginx_analytics.dwd_nginx_enriched_v3
WHERE $__timeFilter(log_time)
  AND ('$platform' = 'All' OR platform IN ('$platform'))
GROUP BY platform ORDER BY requests DESC

-- 简化后的SQL：
SELECT platform, count(*) as requests
FROM nginx_analytics.dwd_nginx_enriched_v3
WHERE $__timeFilter(log_time)
GROUP BY platform ORDER BY requests DESC
```

## 测试步骤
1. 重新导入 nginx-测试面板.json
2. 选择时间范围
3. 验证所有面板都能正常显示数据
4. 如果成功，确认变量问题已解决

## 下一步计划
如果简化版本工作正常：
1. 逐步重新引入变量功能
2. 使用正确的Grafana变量语法
3. 测试多选变量的正确处理方法

注意：当前版本显示所有数据，不支持按变量过滤。这是为了先解决基本的显示问题。
