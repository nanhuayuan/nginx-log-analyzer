# 🗄️ 数据库连接指南 - DBeaver & 其他工具

## 📊 系统中的数据库实例

本系统启动了 **多个数据库实例**，各有不同的用途：

| 数据库 | 容器名 | 端口映射 | 用途 | 连接信息 |
|--------|--------|----------|------|----------|
| **N9E MySQL** | n9e-mysql | 3308→3306 | 夜莺监控数据 | 主要业务数据库 |
| **DataEase MySQL** | nginx-analytics-dataease-mysql | 3307→3306 | DataEase BI | BI报表数据 |
| **ClickHouse** | nginx-analytics-clickhouse | 8123/9000→8123/9000 | 时序数据分析 | 日志分析数据 |
| **PostgreSQL** | nginx-analytics-postgres | 5433→5432 | Superset后端 | Apache Superset |
| **Redis** | nginx-analytics-redis | 6380→6379 | 缓存 | 缓存和队列 |

## 🪟 Windows DBeaver 连接配置

### 1. N9E MySQL (夜莺监控数据库) - **主要数据库**

```
连接类型: MySQL
服务器地址: localhost 或 127.0.0.1
端口: 3308
数据库: n9e_v6
用户名: root
密码: 1234
字符集: utf8mb4
```

**DBeaver配置步骤**:
1. 新建连接 → MySQL
2. 主机: `localhost`
3. 端口: `3308`
4. 数据库: `n9e_v6`
5. 用户名: `root`
6. 密码: `1234`
7. 测试连接

### 2. DataEase MySQL (BI数据库)

```
连接类型: MySQL
服务器地址: localhost
端口: 3307
数据库: dataease
用户名: root
密码: Password123@mysql
字符集: utf8mb4
```

### 3. ClickHouse (时序数据库)

```
连接类型: ClickHouse
服务器地址: localhost
端口: 8123 (HTTP) 或 9000 (Native)
数据库: nginx_analytics
用户名: analytics_user
密码: analytics_password_change_in_prod
```

**注意**: DBeaver需要安装ClickHouse驱动

### 4. PostgreSQL (Superset后端)

```
连接类型: PostgreSQL
服务器地址: localhost
端口: 5433
数据库: superset
用户名: superset
密码: superset_password
```

### 5. Redis (缓存数据库)

```
连接类型: Redis
服务器地址: localhost
端口: 6380
密码: redis_password
```

**注意**: DBeaver需要安装Redis插件

## 🔍 连接验证

### 验证N9E MySQL连接

在DBeaver或命令行中执行：

```sql
-- 检查数据库
SHOW DATABASES;

-- 检查N9E数据库
USE n9e_v6;
SHOW TABLES;

-- 检查表数量（应该是152个）
SELECT COUNT(*) as table_count
FROM information_schema.tables
WHERE table_schema = 'n9e_v6';

-- 检查用户数据
SELECT username, nickname, roles FROM users;

-- 检查权限表
SELECT COUNT(*) FROM role_operation;
```

### 预期结果:
- **表数量**: 152个表
- **用户数据**: 包含root用户
- **权限数据**: role_operation表有数据

## 🛠️ 常见连接问题

### 问题1: 连接被拒绝
```
ERROR 2003 (HY000): Can't connect to MySQL server on 'localhost' (10061)
```

**解决方案**:
1. 检查容器是否运行: `docker ps | findstr mysql`
2. 检查端口映射: `docker port n9e-mysql`
3. 重启容器: `docker-compose restart n9e-mysql`

### 问题2: 用户认证失败
```
ERROR 1045 (28000): Access denied for user 'root'@'localhost'
```

**解决方案**:
1. 确认密码正确 (N9E: `1234`, DataEase: `Password123@mysql`)
2. 运行修复脚本: `simple_fix.bat`
3. 检查c-init.sql是否执行: 该脚本配置了root用户权限

### 问题3: 数据库不存在
```
ERROR 1049 (42000): Unknown database 'n9e_v6'
```

**解决方案**:
1. 运行数据库修复: `simple_fix.bat`
2. 手动创建: `docker exec n9e-mysql mysql -uroot -p1234 -e "CREATE DATABASE n9e_v6;"`

### 问题4: 表数量不对
如果只有37个表而不是152个:

**解决方案**:
1. 运行: `simple_fix.bat` (会执行a-n9e.sql和c-init.sql)
2. 验证: 检查表数量和用户数据

## 📋 数据库初始化文件说明

系统使用两个关键初始化文件：

### 1. a-n9e.sql (主数据库结构)
- **作用**: 创建所有152个表和基础数据
- **内容**:
  - 数据库创建: `CREATE DATABASE n9e_v6`
  - 表结构: 152个表定义
  - 初始数据: 70条INSERT语句
  - 默认用户: root用户和基础配置

### 2. c-init.sql (权限配置)
- **作用**: 配置MySQL用户权限
- **内容**:
  ```sql
  CREATE USER IF NOT EXISTS 'root'@'127.0.0.1' IDENTIFIED BY '1234';
  CREATE USER IF NOT EXISTS 'root'@'localhost' IDENTIFIED BY '1234';
  CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED BY '1234';
  GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
  FLUSH PRIVILEGES;
  ```

**执行顺序**: Docker按字母顺序执行，所以是 `a-n9e.sql` → `c-init.sql`

## 🔧 手动连接测试

### 命令行连接测试:

```cmd
:: 测试N9E MySQL
docker exec -it n9e-mysql mysql -uroot -p1234

:: 测试DataEase MySQL
docker exec -it nginx-analytics-dataease-mysql mysql -uroot -p"Password123@mysql"

:: 测试ClickHouse
docker exec -it nginx-analytics-clickhouse clickhouse-client

:: 测试PostgreSQL
docker exec -it nginx-analytics-postgres psql -U superset -d superset

:: 测试Redis
docker exec -it nginx-analytics-redis redis-cli -a redis_password
```

## 📊 DBeaver高级配置

### MySQL连接优化:
```
连接参数:
- useSSL=false
- allowPublicKeyRetrieval=true
- serverTimezone=Asia/Shanghai
- useUnicode=true
- characterEncoding=utf8mb4
```

### ClickHouse连接优化:
```
JDBC URL示例:
jdbc:clickhouse://localhost:8123/nginx_analytics?user=analytics_user&password=analytics_password_change_in_prod
```

## 🔄 数据库维护

### 备份N9E数据库:
```cmd
docker exec n9e-mysql mysqldump -uroot -p1234 --single-transaction n9e_v6 > n9e_backup.sql
```

### 恢复N9E数据库:
```cmd
docker exec -i n9e-mysql mysql -uroot -p1234 n9e_v6 < n9e_backup.sql
```

### 重置N9E数据库:
```cmd
simple_fix.bat
```

---

**最后更新**: 2025-09-20
**适用系统**: Windows 10/11 + Docker Desktop + DBeaver