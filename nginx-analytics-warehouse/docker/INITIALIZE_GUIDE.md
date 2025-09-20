# 🚀 Docker 环境快速初始化指南

本指南帮助你在新电脑上快速启动整个nginx分析平台基础设施。

## 📋 前提条件

1. **Docker & Docker Compose** 已安装
2. **Git** 已安装并克隆了项目仓库
3. **端口检查**: 确保以下端口未被占用
   ```bash
   # 数据库端口
   3307, 3308, 5433, 6380, 8123, 9000

   # Web服务端口
   3000, 8082, 8088, 8100, 8428, 8810, 12345, 17000

   # 计算引擎端口
   7077, 8080, 8081, 5801

   # 监控端口
   9100, 20090
   ```

## 🔄 初始化步骤

### Step 1: 清理旧数据（如果存在）
```bash
cd nginx-analytics-warehouse/docker

# 停止所有服务
docker-compose down -v

# 清理数据卷（⚠️ 注意：这会删除所有数据）
docker volume prune -f
```

### Step 2: 快速启动
```bash
# 一键启动所有服务
docker-compose up -d

# 查看服务状态
docker-compose ps
```

### Step 3: 验证服务状态
等待2-3分钟后，检查关键服务：

```bash
# 检查数据库连接
docker exec nginx-analytics-clickhouse clickhouse-client --query "SELECT 1"
docker exec n9e-mysql mysql -uroot -p1234 -e "SHOW DATABASES;"

# 检查Web界面
curl -f http://localhost:3000/api/health      # Grafana
curl -f http://localhost:8088/health          # Superset
curl -f http://localhost:8810/                # DataEase
curl -f http://localhost:17000                # Nightingale
```

## 🌐 服务访问地址

启动完成后，可通过以下地址访问各服务：

| 服务 | 地址 | 用户名/密码 | 说明 |
|------|------|-------------|------|
| **Grafana** | http://localhost:3000 | admin/admin123 | 数据可视化 |
| **Superset** | http://localhost:8088 | - | 高级数据分析 |
| **DataEase** | http://localhost:8810 | - | BI报表平台 |
| **Nightingale** | http://localhost:17000 | root/root.2020 | 监控告警 |
| **Spark Master** | http://localhost:8080 | - | 计算引擎 |
| **Flink Dashboard** | http://localhost:8082 | - | 流处理引擎 |
| **DolphinScheduler** | http://localhost:12345 | - | 工作流调度 |

## 🗄️ 数据库连接信息

| 数据库 | 地址 | 端口 | 用户名 | 密码 | 数据库名 |
|--------|------|------|--------|------|----------|
| **ClickHouse** | localhost | 8123/9000 | analytics_user | analytics_password_change_in_prod | nginx_analytics |
| **N9E MySQL** | localhost | 3308 | root | 1234 | n9e_v6 |
| **DataEase MySQL** | localhost | 3307 | root | Password123@mysql | dataease |
| **PostgreSQL** | localhost | 5433 | superset | superset_password | superset |
| **Redis** | localhost | 6380 | - | redis_password | - |

## 🔧 常见问题排查

### 问题1: N9E数据库表不存在
```bash
# 手动初始化N9E数据库
docker exec n9e-mysql mysql -uroot -p1234 < /docker-entrypoint-initdb.d/00-init-database.sql
```

### 问题2: 某些服务启动失败
```bash
# 查看失败服务日志
docker-compose logs [service-name]

# 重启特定服务
docker-compose restart [service-name]
```

### 问题3: 端口冲突
```bash
# 检查端口占用
netstat -tlnp | grep :3000

# 修改docker-compose.yml中的端口映射
```

### 问题4: 服务依赖启动超时
```bash
# 分组启动服务
./start-services.sh databases    # 先启动数据库
sleep 30
./start-services.sh all          # 再启动所有服务
```

## 📁 关键配置文件位置

| 服务 | 配置文件路径 |
|------|-------------|
| **N9E数据库初始化** | `services/n9e/init-scripts/00-init-database.sql` |
| **Grafana数据源** | `services/grafana/datasources/clickhouse.yml` |
| **Nightingale配置** | `services/n9e/config/nightingale/config.toml` |
| **DataEase配置** | `services/dataease/config/application.yml` |
| **环境变量** | `.env` |

## 🚀 性能优化建议

1. **系统资源**: 建议至少8GB内存，4核CPU
2. **磁盘空间**: 预留至少20GB空间用于数据存储
3. **网络**: 确保Docker网络正常，可访问外网下载镜像

## 🔄 数据迁移

如需迁移现有数据：
1. 备份旧环境的`data/`目录
2. 在新环境启动服务后，停止服务
3. 替换`data/`目录内容
4. 重新启动服务

---

## 📞 技术支持

如遇到问题：
1. 检查Docker和Docker Compose版本
2. 查看服务日志: `docker-compose logs [service]`
3. 确认系统资源是否充足
4. 检查防火墙和网络设置

**最后更新**: 2025-09-20