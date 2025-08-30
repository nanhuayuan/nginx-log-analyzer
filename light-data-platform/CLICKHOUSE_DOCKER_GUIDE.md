# 🐋 ClickHouse Docker Compose 部署指南

## 🚀 快速启动

### 方法一：使用管理脚本（推荐）
```bash
# Windows环境
cd light-data-platform\docker
clickhouse-manager.bat start

# Linux/Mac环境
cd light-data-platform/docker
chmod +x clickhouse-manager.sh
./clickhouse-manager.sh start
```

### 方法二：直接使用docker-compose
```bash
cd light-data-platform/docker

# 启动ClickHouse
docker-compose up -d clickhouse

# 启动完整环境(包含Grafana监控)
docker-compose --profile monitoring up -d
```

## 📁 目录结构
```
docker/
├── docker-compose.yml           # 主配置文件
├── clickhouse-manager.bat       # Windows管理脚本
├── clickhouse-manager.sh        # Linux管理脚本
├── clickhouse_config/           # ClickHouse配置
│   ├── config.xml              # 服务器配置
│   └── users.xml               # 用户权限配置
├── clickhouse_init/             # 初始化SQL脚本
│   ├── 001_create_database.sql # 创建数据库
│   ├── 002_create_tables.sql   # 创建表结构
│   └── 003_create_views.sql    # 创建视图和物化视图
└── grafana/                     # Grafana配置(可选)
    └── provisioning/
        └── datasources/
            └── clickhouse.yml   # ClickHouse数据源
```

## 🔧 服务配置

### ClickHouse服务
- **HTTP接口**: http://localhost:8123
- **Native TCP**: localhost:9000  
- **Web界面**: http://localhost:8123/play
- **数据库**: nginx_analytics
- **用户名**: analytics_user
- **密码**: analytics_password

### 可选服务
- **Grafana监控**: http://localhost:3000 (admin/admin)
- **Nginx代理**: http://localhost:80 (生产环境)

## 📊 预创建的表结构

### ODS层 - 原始数据
```sql
nginx_analytics.ods_nginx_log
- 字段：timestamp, client_ip, request_full_uri, response_status_code等
- 引擎：MergeTree
- 分区：按月分区 (toYYYYMM)
- 排序：timestamp, client_ip
```

### DWD层 - 富化数据  
```sql
nginx_analytics.dwd_nginx_enriched
- 字段：增加platform, entry_source, api_category等维度
- 引擎：MergeTree  
- 分区：按月分区
- 排序：timestamp, platform, api_category
```

### DWS层 - 聚合数据
```sql
nginx_analytics.dws_platform_hourly    # 平台小时聚合
nginx_analytics.dws_api_hourly          # API小时聚合
```

### 物化视图 - 实时聚合
```sql
nginx_analytics.mv_platform_hourly     # 自动聚合到DWS层
nginx_analytics.mv_api_hourly           # API维度聚合
nginx_analytics.v_realtime_stats       # 实时统计视图
nginx_analytics.v_anomaly_detection    # 异常检测视图
```

## 🛠️ 管理命令

### Windows (.bat)
```batch
clickhouse-manager.bat start          # 启动ClickHouse
clickhouse-manager.bat start-full     # 启动完整环境
clickhouse-manager.bat stop           # 停止服务
clickhouse-manager.bat status         # 查看状态
clickhouse-manager.bat logs           # 查看日志
clickhouse-manager.bat client         # 连接客户端
clickhouse-manager.bat backup         # 备份数据
```

### Linux/Mac (.sh)
```bash
./clickhouse-manager.sh start         # 启动ClickHouse
./clickhouse-manager.sh start-full    # 启动完整环境
./clickhouse-manager.sh stop          # 停止服务
./clickhouse-manager.sh status        # 查看状态
./clickhouse-manager.sh logs          # 查看日志
./clickhouse-manager.sh client        # 连接客户端
./clickhouse-manager.sh backup        # 备份数据
```

## 🔄 数据迁移步骤

### 1. 启动ClickHouse环境
```bash
# 启动服务
clickhouse-manager.bat start

# 等待服务就绪提示
# [SUCCESS] ClickHouse服务已就绪!
```

### 2. 执行数据迁移
```bash
cd light-data-platform

# 初始化ClickHouse环境（已自动完成）
python migration/clickhouse_migration.py --init

# 从SQLite迁移数据
python migration/clickhouse_migration.py --migrate

# 验证迁移结果  
python migration/clickhouse_migration.py --verify
```

### 3. 性能对比测试
```bash
# 测试ClickHouse vs SQLite性能
python migration/clickhouse_migration.py --performance

# 预期结果：
# ClickHouse查询时间: 0.001s
# SQLite查询时间: 0.030s  
# 性能提升: 30x
```

## 📈 使用场景配置

### 开发测试环境
```bash
# 仅启动ClickHouse
clickhouse-manager.bat start
```

### 生产监控环境
```bash
# 启动ClickHouse + Grafana监控
clickhouse-manager.bat start-full
```

### 完整生产环境
```bash
# 启动所有服务包括Nginx代理
docker-compose --profile production up -d
```

## 🔍 连接和查询

### 使用客户端连接
```bash
# 通过管理脚本连接
clickhouse-manager.bat client

# 或直接连接
docker exec -it nginx-analytics-clickhouse clickhouse-client \
  --user analytics_user \
  --password analytics_password \
  --database nginx_analytics
```

### 基础查询示例
```sql
-- 查看表列表
SHOW TABLES;

-- 查看数据概况
SELECT platform, count(*) as cnt 
FROM dwd_nginx_enriched 
GROUP BY platform;

-- 实时统计
SELECT * FROM v_realtime_stats;

-- 异常检测
SELECT * FROM v_anomaly_detection 
WHERE anomaly_status != 'normal';
```

### Web界面访问
访问 http://localhost:8123/play 使用ClickHouse内置Web界面进行查询。

## 🔒 安全配置

### 用户权限
- **analytics_user**: 完整分析权限
- **readonly_user**: 只读查询权限  
- **web_app**: Web应用专用权限

### 网络安全
- 容器间通信使用内部网络
- 外部仅暴露必要端口
- 支持SSL/TLS配置

## 📊 监控和维护

### 查看服务状态
```bash
clickhouse-manager.bat status
docker-compose ps
```

### 查看日志
```bash
clickhouse-manager.bat logs clickhouse
docker-compose logs -f clickhouse
```

### 数据备份
```bash
# 自动备份到backups目录
clickhouse-manager.bat backup

# 指定备份目录
clickhouse-manager.bat backup C:\backups
```

### 清理维护
```bash
# 清理所有数据和容器
clickhouse-manager.bat cleanup

# 重启服务
clickhouse-manager.bat restart
```

## ⚡ 性能优化

### 内存配置
- 默认配置：最大使用80%系统内存
- 查询内存限制：20GB
- 可通过config.xml调整

### 并发配置
- 最大并发查询：100
- 线程池大小：10000
- 异步插入：启用

### 存储优化
- 数据压缩：ZSTD Level 3
- 分区策略：按月分区
- 索引颗粒度：8192

## 🚨 故障排除

### 常见问题

#### 1. 启动失败
```bash
# 检查端口占用
netstat -an | findstr :8123

# 检查Docker状态
docker ps -a

# 查看详细日志
clickhouse-manager.bat logs
```

#### 2. 连接失败
```bash
# 检查服务状态
clickhouse-manager.bat status

# 测试连接
curl http://localhost:8123/ping

# 检查用户权限
clickhouse-manager.bat client
```

#### 3. 内存不足
修改 `docker-compose.yml` 中的内存限制：
```yaml
clickhouse:
  deploy:
    resources:
      limits:
        memory: 4G  # 根据系统调整
```

#### 4. 数据丢失
```bash
# 从备份恢复
clickhouse-manager.bat sql backup_file.sql

# 检查数据卷
docker volume ls
```

## 🎯 最佳实践

1. **开发阶段**: 使用单个ClickHouse容器
2. **测试阶段**: 启用监控和日志收集  
3. **生产阶段**: 启用完整环境+备份策略
4. **扩展阶段**: 配置集群和负载均衡

通过这套完整的Docker Compose配置，您可以轻松部署和管理ClickHouse环境，实现从SQLite到企业级数据库的平滑升级！🎉