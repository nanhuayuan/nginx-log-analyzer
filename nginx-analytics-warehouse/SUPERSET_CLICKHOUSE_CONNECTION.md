# Superset连接ClickHouse配置指南

## 🔧 连接信息

### 方式1：使用容器网络内部地址（推荐）
```
Host: nginx-analytics-clickhouse-full
Port: 8123
Database: nginx_analytics
Username: analytics_user
Password: analytics_password
```

### 方式2：如果方式1不工作，使用内部IP
首先获取ClickHouse容器的内部IP：
```bash
docker inspect nginx-analytics-clickhouse-full | grep IPAddress
```
然后使用获取的IP地址，例如：
```
Host: 172.18.0.2  (示例IP，请使用实际获取的IP)
Port: 8123
Database: nginx_analytics
Username: analytics_user
Password: analytics_password
```

## 🔍 故障排除步骤

### 1. 验证容器间网络连通性
```bash
# 进入Superset容器测试连接
docker exec -it nginx-analytics-superset-full bash
ping nginx-analytics-clickhouse-full
```

### 2. 验证ClickHouse服务可访问性
```bash
# 在Superset容器内测试ClickHouse连接
docker exec nginx-analytics-superset-full curl -u analytics_user:analytics_password "http://nginx-analytics-clickhouse-full:8123/?query=SELECT%201"
```

### 3. 检查网络配置
```bash
# 查看容器网络
docker network ls
docker network inspect nginx-analytics-full
```

## 📋 在Superset界面中的具体操作

1. **访问Superset**: http://localhost:8088
2. **登录**: admin / admin123
3. **添加数据源**:
   - Settings → Database Connections → + Database
   - 选择 "ClickHouse Connect" 或 "ClickHouse"
4. **填写连接信息**:
   - **Display Name**: ClickHouse-nginx-analytics
   - **Host**: nginx-analytics-clickhouse-full
   - **Port**: 8123
   - **Database**: nginx_analytics
   - **Username**: analytics_user
   - **Password**: analytics_password
   - **Additional Parameters**: 留空或填写 `secure=false`

5. **测试连接**: 点击 "Test Connection"

## 🛠️ 如果仍然连接失败的解决方法

### 方法1：修改docker-compose网络配置
```yaml
# 在docker-compose-full.yml中确保所有服务在同一网络
networks:
  default:
    name: nginx-analytics-full
    driver: bridge
```

### 方法2：使用host网络模式（临时方案）
```bash
# 重启Superset使用host网络
docker stop nginx-analytics-superset-full
docker run -d --name nginx-analytics-superset-temp --network host apache/superset:latest
```

### 方法3：检查防火墙和端口
```bash
# 确保ClickHouse端口在容器内可访问
docker exec nginx-analytics-superset-full telnet nginx-analytics-clickhouse-full 8123
```

## 📊 验证数据连接成功后的测试查询

连接成功后，可以在Superset的SQL Lab中执行以下查询测试：

```sql
-- 基础连接测试
SELECT 1

-- 查看表结构
SHOW TABLES FROM nginx_analytics

-- 查询样例数据
SELECT count() FROM ods_nginx_raw

-- 状态码分布
SELECT response_status_code, count() as count 
FROM ods_nginx_raw 
GROUP BY response_status_code
```

## ⚠️ 常见错误和解决方案

| 错误信息 | 解决方案 |
|----------|----------|
| "The port is closed" | 使用容器名而非localhost |
| "Connection refused" | 检查ClickHouse服务状态 |
| "Authentication failed" | 验证用户名密码正确性 |
| "Database not found" | 确认数据库nginx_analytics存在 |

记住：容器内部通信使用容器名，而不是localhost！