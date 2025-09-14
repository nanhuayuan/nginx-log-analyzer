# 变量依赖修复说明

## 发现的问题
之前的Dashboard中存在变量循环依赖问题：
- platform变量的查询依赖于tenant_code变量
- 这导致变量初始化时出现循环依赖，产生错误的变量值格式

## 修复方案
1. **简化变量查询**：移除变量查询中的依赖关系
2. **tenant_code**：改为单选，避免复杂的多选逻辑
3. **platform和api_category**：查询所有可能值，不依赖其他变量

## 修复后的变量配置
```sql
-- tenant_code查询（单选）
SELECT DISTINCT tenant_code FROM nginx_analytics.dwd_nginx_enriched_v3 ORDER BY tenant_code

-- platform查询（多选，支持All）
SELECT DISTINCT platform FROM nginx_analytics.dwd_nginx_enriched_v3 ORDER BY platform

-- api_category查询（多选，支持All）
SELECT DISTINCT api_category FROM nginx_analytics.dwd_nginx_enriched_v3 ORDER BY api_category
```

## $__conditionalAll宏的正确使用
现在变量配置正确后，$__conditionalAll宏应该能正常工作：

```sql
SELECT count(*) as value
FROM nginx_analytics.dwd_nginx_enriched_v3
WHERE $__timeFilter(log_time)
    AND $__conditionalAll(tenant_code = '$tenant_code', $tenant_code)
    AND $__conditionalAll(platform IN ($platform), $platform)
    AND $__conditionalAll(api_category IN ($api_category), $api_category)
```

## 变量设置建议
- **租户**: 选择 'default_tenant'（单选）
- **平台**: 选择 'All' 或具体平台（多选）
- **API类别**: 选择 'All' 或具体类别（多选）

修复后应该不会再出现变量值格式错误的问题！
