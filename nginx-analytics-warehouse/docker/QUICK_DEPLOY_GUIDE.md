# 🚀 新环境快速部署指南

## 📋 使用场景

1. **全新环境**: 首次在新电脑部署
2. **环境迁移**: 从旧环境迁移到新环境
3. **问题修复**: 遇到数据库或配置问题需要重置

## ⚡ 快速部署步骤

### 方案一: 全新部署 (推荐)

```bash
# 1. 克隆项目
git clone <repository-url>
cd nginx-analytics-warehouse/docker

# 2. 检查配置
./check-config-files.sh

# 3. 一键部署
docker-compose up -d

# 4. 验证部署
./validate-deployment.sh
```

### 方案二: 完全重置部署

```bash
# 适用于遇到问题或需要完全清理的情况
cd nginx-analytics-warehouse/docker

# 执行完全重置
./reset-environment.sh
```

### 方案三: 仅数据库重置

```bash
# 适用于只有数据库问题的情况
cd nginx-analytics-warehouse/docker

# 停止服务
docker-compose down

# 清理数据卷
docker-compose down -v

# 重新启动
docker-compose up -d

# 如果还有问题，强制重置数据库
./force-init-databases.sh
```

## 🔍 问题排查

### N9E数据库错误

如果遇到以下错误：
```
Error 1146: Table 'n9e_v6.role_operation' doesn't exist
Error 1054: Unknown column 'username' in 'where clause'
```

**解决方案**:
```bash
# 检查N9E数据库状态
./test-n9e-init.sh

# 如果测试失败，强制重新初始化
./force-init-databases.sh
```

### 端口冲突

```bash
# 检查端口占用
netstat -tuln | grep -E ":(3000|8088|8810|17000|3308|5433|6380|8123)"

# 修改docker-compose.yml中的端口映射
```

### 服务启动失败

```bash
# 查看特定服务日志
docker-compose logs nginx-analytics-nightingale

# 重启特定服务
docker-compose restart nginx-analytics-nightingale

# 分阶段启动
docker-compose up -d n9e-mysql  # 先启动数据库
sleep 30
docker-compose up -d            # 再启动所有服务
```

## 🌐 服务访问地址

| 服务 | 地址 | 默认账号 | 说明 |
|------|------|----------|------|
| **Nightingale** | http://localhost:17000 | root/root.2020 | 监控告警平台 |
| **Grafana** | http://localhost:3000 | admin/admin123 | 数据可视化 |
| **DataEase** | http://localhost:8810 | admin/dataease | BI报表平台 |
| **Superset** | http://localhost:8088 | - | 数据分析平台 |
| **DolphinScheduler** | http://localhost:12345 | admin/dolphinscheduler123 | 工作流调度 |

## 📊 关键配置说明

### N9E数据库初始化

- **官方脚本**: 使用Nightingale v8.3.1官方初始化脚本
- **执行顺序**:
  1. `a-n9e.sql` (完整表结构+数据)
  2. `c-init.sql` (MySQL权限配置)
- **数据库**: n9e_v6 (152个表 + 70条初始数据)

### Docker配置要点

```yaml
# 关键挂载点
volumes:
  - ./services/n9e/init-scripts:/docker-entrypoint-initdb.d
  - ./services/grafana/datasources:/etc/grafana/provisioning/datasources
  - ./services/dataease/config:/opt/apps/config
```

### 数据库连接信息

| 数据库 | 端口 | 用户名 | 密码 | 数据库名 |
|--------|------|--------|------|----------|
| N9E MySQL | 3308 | root | 1234 | n9e_v6 |
| DataEase MySQL | 3307 | root | Password123@mysql | dataease |
| ClickHouse | 8123/9000 | analytics_user | analytics_password_change_in_prod | nginx_analytics |
| PostgreSQL | 5433 | superset | superset_password | superset |
| Redis | 6380 | - | redis_password | - |

## 🔧 高级操作

### 单独测试N9E

```bash
# 只启动N9E相关服务
docker-compose up -d n9e-mysql victoriametrics redis nightingale

# 测试N9E数据库
./test-n9e-init.sh
```

### 数据备份与恢复

```bash
# 备份数据
docker run --rm -v nginx-analytics_clickhouse_data:/data -v $(pwd):/backup alpine tar czf /backup/clickhouse-backup.tar.gz /data

# 恢复数据
docker run --rm -v nginx-analytics_clickhouse_data:/data -v $(pwd):/backup alpine tar xzf /backup/clickhouse-backup.tar.gz -C /
```

### 性能优化

```bash
# 查看资源使用
docker stats

# 调整内存限制 (在docker-compose.yml中)
deploy:
  resources:
    limits:
      memory: 2G
    reservations:
      memory: 1G
```

## ⚠️ 注意事项

1. **系统要求**: 最少8GB内存，4核CPU，20GB可用磁盘空间
2. **Docker版本**: 建议Docker Engine 20.x+, Docker Compose v2.x
3. **网络要求**: 确保可以访问外网下载镜像
4. **端口规划**: 确保关键端口未被占用
5. **数据持久化**: 重要数据存储在Docker卷中，重置前请备份

## 📞 故障支持

1. **配置检查**: `./check-config-files.sh`
2. **部署验证**: `./validate-deployment.sh`
3. **N9E测试**: `./test-n9e-init.sh`
4. **完全重置**: `./reset-environment.sh`
5. **数据库重置**: `./force-init-databases.sh`

---

**最后更新**: 2025-09-20
**版本**: v2.1 官方兼容版