# Grafana ClickHouse 数据源配置指南

## 1. ClickHouse连接信息

```
主机地址: localhost
HTTP端口: 8123  
Native端口: 9000
数据库名: nginx_analytics
用户名: analytics_user
密码: analytics_password
```

## 2. 在Grafana中添加ClickHouse数据源

### 步骤1：访问Grafana
- 访问：http://localhost:3000
- 登录信息：admin / admin123

### 步骤2：添加数据源
1. 点击左侧菜单中的 "Configuration" (齿轮图标)
2. 选择 "Data Sources"
3. 点击 "Add data source"
4. 搜索并选择 "ClickHouse" 数据源

### 步骤3：配置连接信息
**基本配置:**
- **Name**: ClickHouse-nginx-analytics
- **URL**: http://localhost:8123
- **Database**: nginx_analytics

**认证配置:**
- **Username**: analytics_user  
- **Password**: analytics_password

**高级配置:**
- **Max Open Connections**: 10
- **Max Idle Connections**: 10
- **Query Timeout**: 60s
- **Connection Timeout**: 10s

### 步骤4：测试连接
点击 "Save & test" 按钮测试连接是否成功。

## 3. 创建示例仪表板

### 基础查询示例

**请求总量:**
```sql
SELECT count() FROM ods_nginx_raw
```

**状态码分布:**
```sql
SELECT 
    response_status_code as status,
    count() as count
FROM ods_nginx_raw 
GROUP BY response_status_code
```

**时间线请求量:**
```sql
SELECT 
    toStartOfHour(log_time) as time,
    count() as requests
FROM ods_nginx_raw 
WHERE log_time >= now() - INTERVAL 24 HOUR
GROUP BY time 
ORDER BY time
```

**API性能分析:**
```sql
SELECT 
    request_uri,
    count() as request_count,
    round(avg(total_request_time), 3) as avg_response_time,
    round(quantile(0.95)(total_request_time), 3) as p95_response_time
FROM ods_nginx_raw 
WHERE total_request_time > 0
GROUP BY request_uri
ORDER BY avg_response_time DESC
LIMIT 10
```

**错误率监控:**
```sql
SELECT 
    toStartOfMinute(log_time) as time,
    countIf(response_status_code LIKE '4%' OR response_status_code LIKE '5%') * 100.0 / count() as error_rate
FROM ods_nginx_raw 
WHERE log_time >= now() - INTERVAL 1 HOUR
GROUP BY time 
ORDER BY time
```

## 4. 常见问题

### 连接失败
- 检查ClickHouse容器是否正在运行：`docker ps | grep clickhouse`
- 检查端口是否正确映射：应该看到 `0.0.0.0:8123->8123/tcp`
- 验证用户名和密码是否正确

### 查询错误
- 确认数据库名称为 `nginx_analytics`
- 确认表名称，使用 `SHOW TABLES` 查看所有表
- 检查字段名称，使用 `DESCRIBE table_name` 查看表结构

### 性能优化
- 使用时间过滤条件提高查询性能
- 合理使用 LIMIT 限制返回结果数量
- 利用ClickHouse的列式存储特性，只查询需要的字段

## 5. 下一步

1. 创建nginx性能监控仪表板
2. 设置告警规则
3. 配置数据刷新间隔
4. 创建用户权限管理