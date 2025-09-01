# Grafana vs Superset 对比环境

## 🚀 服务状态

### 当前运行的服务

```bash
cd nginx-analytics-warehouse
docker-compose -f docker-compose-full.yml ps
```

**已部署服务:**
- ✅ **ClickHouse**: localhost:8123 (数据库)
- ✅ **Grafana**: localhost:3000 (可视化工具1)
- ✅ **Redis**: localhost:6380 (缓存)
- ⏳ **PostgreSQL**: localhost:5433 (Superset元数据)
- ⏳ **Superset**: localhost:8088 (可视化工具2，正在启动中)

## 📊 对比工具访问信息

### Grafana
- **地址**: http://localhost:3000
- **用户名**: admin
- **密码**: admin123
- **插件**: vertamedia-clickhouse-datasource (已安装)
- **特点**: 
  - 专业的监控和告警平台
  - 实时数据更新
  - 丰富的图表类型
  - 强大的告警功能

### Superset
- **地址**: http://localhost:8088  
- **用户名**: admin
- **密码**: admin123
- **驱动**: clickhouse-connect (已安装)
- **特点**:
  - 现代化的BI平台
  - 拖拽式仪表板创建
  - SQL编辑器
  - 数据探索功能

## 🔗 ClickHouse数据源配置

### 在Grafana中配置
1. 访问 http://localhost:3000
2. Connections -> Data Sources -> Add new data source
3. 选择 "ClickHouse" 
4. 配置信息:
   ```
   URL: http://clickhouse:8123
   Database: nginx_analytics
   Username: analytics_user
   Password: analytics_password
   ```

### 在Superset中配置
1. 访问 http://localhost:8088 (等待启动完成)
2. Settings -> Database Connections -> + Database
3. 选择 "ClickHouse Connect"
4. SQL Alchemy URI:
   ```
   clickhousedb://analytics_user:analytics_password@clickhouse:8123/nginx_analytics
   ```

## 📈 可用数据和查询

### nginx日志分析数据
- **表名**: ods_nginx_raw
- **记录数**: 99条样例数据
- **时间范围**: 2025-05-09 nginx访问日志

### 示例查询

**请求量统计:**
```sql
SELECT count() as total_requests FROM ods_nginx_raw
```

**状态码分布:**
```sql
SELECT 
    response_status_code,
    count() as count,
    count() * 100.0 / (SELECT count() FROM ods_nginx_raw) as percentage
FROM ods_nginx_raw 
GROUP BY response_status_code 
ORDER BY count DESC
```

**API性能分析:**
```sql
SELECT 
    request_uri,
    count() as requests,
    round(avg(total_request_time), 3) as avg_response_time,
    round(quantile(0.95)(total_request_time), 3) as p95_response_time
FROM ods_nginx_raw 
WHERE total_request_time > 0
GROUP BY request_uri
ORDER BY avg_response_time DESC
LIMIT 10
```

**时间序列分析:**
```sql
SELECT 
    toStartOfHour(log_time) as hour,
    count() as requests
FROM ods_nginx_raw 
GROUP BY hour 
ORDER BY hour
```

## 🎯 对比建议

### 创建相同的图表在两个工具中
1. **总请求量** - 单值统计
2. **状态码分布** - 饼图
3. **API响应时间** - 柱状图
4. **时间序列** - 折线图
5. **Top接口** - 表格

### 评估维度
- **易用性**: 哪个更容易上手
- **图表丰富度**: 图表类型和自定义选项
- **性能**: 查询响应速度
- **功能完整性**: 告警、分享、权限等
- **维护成本**: 资源占用和管理复杂度

## 🛠️ 管理命令

### 服务管理
```bash
# 查看所有服务状态
docker-compose -f docker-compose-full.yml ps

# 查看服务日志
docker-compose -f docker-compose-full.yml logs -f superset

# 重启特定服务
docker-compose -f docker-compose-full.yml restart grafana

# 停止所有服务
docker-compose -f docker-compose-full.yml down
```

### 故障排除
```bash
# 检查Superset启动进度
docker logs nginx-analytics-superset-full -f

# 重新启动PostgreSQL
docker-compose -f docker-compose-full.yml restart postgres

# 验证ClickHouse数据
curl -u analytics_user:analytics_password "http://localhost:8123/?query=SELECT count() FROM nginx_analytics.ods_nginx_raw"
```

## 📝 下一步

1. **等待Superset完全启动** (可能需要5-10分钟)
2. **在两个工具中分别配置ClickHouse数据源**
3. **创建相同的分析图表进行对比**
4. **根据使用体验选择最适合的工具**

现在你可以开始对比测试了！🚀