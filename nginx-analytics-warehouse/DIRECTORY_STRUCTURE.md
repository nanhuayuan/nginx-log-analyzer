# 目录结构说明

## 推荐的标准目录结构

```
nginx-analytics-warehouse/
├── docker/                          # Docker相关配置
│   ├── docker-compose.yml          # 主要的docker-compose配置
│   ├── .env                         # 环境变量配置
│   └── README.md                    # Docker使用说明
│
├── data/                            # 数据持久化目录
│   ├── clickhouse/                  # ClickHouse数据文件
│   │   ├── data/                    # 数据库文件
│   │   ├── metadata/                # 元数据
│   │   └── tmp/                     # 临时文件
│   ├── grafana/                     # Grafana数据
│   │   ├── dashboards/              # 仪表板配置
│   │   ├── provisioning/            # 数据源配置
│   │   └── grafana.db              # Grafana数据库
│   ├── postgres/                    # PostgreSQL数据
│   │   └── pgdata/                  # Postgres数据文件
│   ├── redis/                       # Redis持久化数据
│   │   ├── dump.rdb                # RDB快照
│   │   └── appendonly.aof          # AOF日志
│   └── superset/                    # Superset配置
│       ├── superset_config.py       # Superset配置文件
│       └── static/                  # 静态文件
│
├── logs/                            # 日志文件目录
│   ├── clickhouse/                  # ClickHouse日志
│   │   ├── clickhouse-server.log    # 服务器日志
│   │   └── error.log               # 错误日志
│   ├── grafana/                     # Grafana日志
│   │   └── grafana.log             # Grafana日志
│   ├── postgres/                    # PostgreSQL日志
│   │   └── postgresql.log          # Postgres日志
│   ├── redis/                       # Redis日志
│   │   └── redis-server.log        # Redis服务日志
│   ├── superset/                    # Superset日志
│   │   └── superset.log            # Superset应用日志
│   └── nginx-processor/             # 处理器日志
│       ├── processing.log           # 处理过程日志
│       └── error.log               # 处理错误日志
│
├── config/                          # 配置文件目录
│   ├── clickhouse/                  # ClickHouse配置
│   │   ├── users.xml               # 用户配置
│   │   └── config.xml              # 服务器配置
│   ├── grafana/                     # Grafana配置
│   │   ├── datasources/            # 数据源配置
│   │   │   └── clickhouse.yml      # ClickHouse数据源
│   │   └── dashboards/             # 仪表板配置
│   │       └── nginx-analytics.json # Nginx分析仪表板
│   └── nginx/                       # Nginx相关配置
│       └── log-format.conf         # 建议的日志格式
│
├── processors/                      # 数据处理器代码
│   ├── main_simple.py              # 主要入口点
│   ├── nginx_processor_complete.py  # 核心处理器
│   ├── init_database.py            # 数据库初始化脚本
│   ├── manage_volumes.py           # 数据管理脚本
│   ├── validate_processing.py      # 数据验证脚本
│   ├── show_data_flow.py          # 状态检查脚本
│   └── processed_logs_complete.json # 处理记录
│
├── nginx_logs/                      # Nginx日志文件目录
│   ├── 20250422/                   # 按日期组织的日志
│   │   ├── access186.log           # 访问日志文件
│   │   └── access187.log
│   ├── 20250423/
│   │   └── *.log
│   └── archive/                    # 归档日志
│       └── 2025-04/               # 按月归档
│
├── backup/                          # 备份目录
│   ├── daily/                      # 每日备份
│   │   ├── 20250901/              # 按日期组织
│   │   └── 20250902/
│   ├── weekly/                     # 周备份
│   └── monthly/                    # 月备份
│
├── scripts/                         # 运维脚本
│   ├── backup.sh                   # 备份脚本
│   ├── restore.sh                  # 恢复脚本
│   ├── health-check.sh             # 健康检查
│   └── cleanup.sh                  # 清理脚本
│
├── docs/                           # 文档目录
│   ├── DEPLOYMENT_GUIDE.md         # 部署指南
│   ├── PYCHARM_SETUP.md           # PyCharm配置
│   ├── API_REFERENCE.md           # API参考
│   └── TROUBLESHOOTING.md         # 故障排除
│
├── .env.example                    # 环境变量模板
├── .gitignore                      # Git忽略文件
├── README.md                       # 项目说明
└── DIRECTORY_STRUCTURE.md          # 本文件
```

## 数据目录权限设置

### Linux/macOS
```bash
# 设置适当的权限
sudo chown -R 472:472 data/grafana     # Grafana用户
sudo chown -R 999:999 data/postgres    # PostgreSQL用户
sudo chown -R 999:999 data/redis       # Redis用户
sudo chmod -R 755 data/                # 基本权限
sudo chmod -R 755 logs/                # 日志权限
```

### Windows
```powershell
# Windows通常不需要特殊权限设置，但确保目录可写
icacls data /grant Everyone:(OI)(CI)F
icacls logs /grant Everyone:(OI)(CI)F
```

## 环境变量配置

创建`.env`文件在`docker/`目录下：

```bash
# ClickHouse配置
CLICKHOUSE_DB=nginx_analytics
CLICKHOUSE_USER=analytics_user
CLICKHOUSE_PASSWORD=analytics_password_change_in_prod

# Grafana配置
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=admin123_change_in_prod

# PostgreSQL配置
POSTGRES_DB=superset
POSTGRES_USER=superset
POSTGRES_PASSWORD=superset_password_change_in_prod

# Redis配置
REDIS_PASSWORD=redis_password_change_in_prod

# Superset配置
SUPERSET_SECRET_KEY=nginx_analytics_secret_key_change_in_production
SUPERSET_ADMIN_USER=admin
SUPERSET_ADMIN_PASSWORD=admin123_change_in_prod

# 网络配置
COMPOSE_PROJECT_NAME=nginx-analytics
```

## 数据备份策略

### 自动备份配置
1. **每日备份**: 保留7天
2. **周备份**: 保留4周
3. **月备份**: 保留12个月

### 备份内容
- ClickHouse数据库
- Grafana仪表板配置
- PostgreSQL元数据
- 处理记录文件

### 备份脚本示例
```bash
#!/bin/bash
# backup.sh
DATE=$(date +%Y%m%d)
BACKUP_DIR="./backup/daily/$DATE"

mkdir -p "$BACKUP_DIR"

# 备份数据
python processors/manage_volumes.py backup "$BACKUP_DIR"

# 压缩备份
tar -czf "$BACKUP_DIR.tar.gz" -C "./backup/daily" "$DATE"
rm -rf "$BACKUP_DIR"

echo "Backup completed: $BACKUP_DIR.tar.gz"
```

## 监控和维护

### 磁盘使用监控
```bash
# 检查数据目录大小
du -sh data/*/

# 检查日志目录大小
du -sh logs/*/

# 清理旧日志（保留30天）
find logs/ -name "*.log" -mtime +30 -delete
```

### 性能优化建议

1. **ClickHouse优化**
   - 数据目录使用SSD存储
   - 调整内存配置
   - 定期OPTIMIZE TABLE

2. **日志轮转**
   - 配置logrotate
   - 限制日志文件大小
   - 自动压缩旧日志

3. **备份优化**
   - 增量备份策略
   - 异地备份存储
   - 定期验证备份完整性

## 安全建议

1. **密码管理**
   - 生产环境修改默认密码
   - 使用环境变量存储敏感信息
   - 定期轮换密码

2. **网络安全**
   - 限制端口访问
   - 使用防火墙规则
   - 启用SSL/TLS加密

3. **文件权限**
   - 最小权限原则
   - 定期审核文件权限
   - 禁用不必要的服务

这种目录结构提供了清晰的数据组织方式，便于管理、备份和维护。