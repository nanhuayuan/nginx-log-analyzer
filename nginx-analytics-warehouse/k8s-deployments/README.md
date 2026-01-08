# Nginx日志分析数据仓库 - K8s部署配置

## 📋 部署概览

**方案**: 方案B - Grafana快速验证方案
**部署策略**: 全部服务固定在worker-148节点,使用hostPath持久化

### 服务清单

| 服务 | 类型 | 副本 | 存储 | 端口 | 说明 |
|------|------|------|------|------|------|
| ClickHouse | StatefulSet | 1 | 50Gi | 8123, 9000 | 核心数据库 |
| Redis | Deployment | 1 | 5Gi | 6380 | 缓存服务 |
| DataEase-MySQL | StatefulSet | 1 | 10Gi | 3307 | DataEase后端 |
| DataEase | Deployment | 1 | 10Gi | 8810 | BI可视化 |
| Grafana | Deployment | 1 | 2Gi | 3000 | 性能监控 |

**总计**: 5个Pod, ~80Gi存储

### 存储规划 (worker-148)

```
D:\soft_work\k8s-data\
├── clickhouse-data/          # 50GB - ClickHouse数据
├── redis-data/               # 5GB - Redis持久化
├── dataease-mysql-data/      # 10GB - MySQL数据
├── dataease-data/            # 10GB - DataEase数据
└── grafana-data/             # 2GB - Grafana配置
```

### ETL Pipeline

**保留Windows定时任务方式**:
- 日志下载: 每天凌晨1:00 (NginxZipLogProcessor)
- ETL处理: 每天凌晨1:30 (NginxETLAutoProcessor)
- 日志位置: D:\project\nginx-log-analyzer\nginx_logs
- 直接在worker-148 Windows上运行,无需容器化

## 🚀 快速部署指南

### 前置条件

1. **K3s集群已就绪**
   - master-140: 控制平面
   - worker-148: 工作负载节点(本项目全部服务)
   - worker-168: 工作负载节点(预留)

2. **验证节点就绪**
   ```bash
   kubectl get nodes -o wide
   ```

3. **在worker-148创建数据目录**
   ```powershell
   # 在worker-148的PowerShell中执行
   mkdir D:\soft_work\k8s-data\clickhouse-data -Force
   mkdir D:\soft_work\k8s-data\redis-data -Force
   mkdir D:\soft_work\k8s-data\dataease-mysql-data -Force
   mkdir D:\soft_work\k8s-data\dataease-data -Force
   mkdir D:\soft_work\k8s-data\grafana-data -Force
   ```

### 部署步骤

#### Step 1: 创建命名空间和配置
```bash
kubectl apply -f 00-namespace.yaml
kubectl apply -f 01-configmap.yaml
kubectl apply -f 02-secrets.yaml
```

#### Step 2: 创建持久化卷
```bash
kubectl apply -f 03-persistent-volumes.yaml
```

#### Step 3: 部署核心数据服务
```bash
# ClickHouse (优先部署)
kubectl apply -f 04-clickhouse.yaml

# 等待ClickHouse就绪
kubectl wait --for=condition=ready pod -l app=clickhouse -n nginx-analytics --timeout=120s

# Redis
kubectl apply -f 05-redis.yaml
```

#### Step 4: 初始化ClickHouse数据库
```bash
# 在worker-148 WSL中执行
wsl -d k3s-agent-148

# 复制DDL文件到ClickHouse Pod
kubectl cp D:/project/nginx-log-analyzer/nginx-analytics-warehouse/ddl nginx-analytics/clickhouse-0:/tmp/ddl

# 进入ClickHouse Pod执行初始化
kubectl exec -it clickhouse-0 -n nginx-analytics -- bash

# 在Pod内执行DDL
clickhouse-client --user=analytics_user --password=analytics_password_change_in_prod < /tmp/ddl/01_ods_layer_real.sql
clickhouse-client --user=analytics_user --password=analytics_password_change_in_prod < /tmp/ddl/02_dwd_layer_real.sql
clickhouse-client --user=analytics_user --password=analytics_password_change_in_prod < /tmp/ddl/03_ads_layer_real.sql
clickhouse-client --user=analytics_user --password=analytics_password_change_in_prod < /tmp/ddl/04_materialized_views_corrected.sql
```

#### Step 5: 部署可视化服务
```bash
# DataEase MySQL
kubectl apply -f 06-dataease-mysql.yaml
kubectl wait --for=condition=ready pod -l app=dataease-mysql -n nginx-analytics --timeout=120s

# DataEase
kubectl apply -f 07-dataease.yaml

# Grafana
kubectl apply -f 08-grafana.yaml
```

#### Step 6: 验证部署
```bash
# 查看所有Pod状态
kubectl get pods -n nginx-analytics -o wide

# 查看服务暴露端口
kubectl get svc -n nginx-analytics

# 查看PVC状态
kubectl get pvc -n nginx-analytics
```

#### Step 7: 配置ETL连接到K8s ClickHouse
```bash
# 在worker-148 Windows上修改ETL连接配置
# 编辑: D:\project\nginx-log-analyzer\nginx-analytics-warehouse\etl\writers\dwd_writer.py
# 修改ClickHouse连接为: 192.168.0.140:8123
```

## 🌐 访问地址

部署完成后,在局域网内访问:

| 服务 | 访问地址 | 默认账号 |
|------|----------|----------|
| **Grafana** | http://192.168.0.140:3000 | admin / admin123 |
| **DataEase** | http://192.168.0.140:8810 | admin / DataEase123@ |
| **ClickHouse** | http://192.168.0.140:8123 | analytics_user / analytics_password_change_in_prod |
| **ClickHouse Client** | tcp://192.168.0.140:9000 | 同上 |

## 📊 监控检查

```bash
# 实时日志查看
kubectl logs -f clickhouse-0 -n nginx-analytics
kubectl logs -f deployment/grafana -n nginx-analytics

# 进入ClickHouse验证数据
kubectl exec -it clickhouse-0 -n nginx-analytics -- clickhouse-client --user=analytics_user --password=analytics_password_change_in_prod

# 查询数据库
SELECT database, name, engine, total_rows
FROM system.tables
WHERE database = 'nginx_analytics';
```

## 🔧 故障排查

### ClickHouse无法启动
```bash
# 检查Pod日志
kubectl logs clickhouse-0 -n nginx-analytics

# 检查PVC绑定状态
kubectl describe pvc clickhouse-data-pvc -n nginx-analytics

# 验证hostPath目录权限(在worker-148 WSL中)
ls -la /mnt/d/soft_work/k8s-data/clickhouse-data
```

### DataEase无法访问
```bash
# 检查MySQL是否就绪
kubectl get pods -n nginx-analytics | grep dataease-mysql

# 查看DataEase日志
kubectl logs -f deployment/dataease -n nginx-analytics

# 检查Service端口映射
kubectl get svc dataease -n nginx-analytics
```

### ETL无法写入数据
```bash
# 测试ClickHouse连接(在worker-148 Windows)
curl http://192.168.0.140:8123/ping

# 验证用户权限
kubectl exec -it clickhouse-0 -n nginx-analytics -- clickhouse-client --user=analytics_user --password=analytics_password_change_in_prod -q "SELECT 1"
```

## 🔄 升级到方案A (Nightingale)

验证成功后,可升级到完整监控方案:

```bash
# 部署Nightingale组件
kubectl apply -f 09-victoriametrics.yaml
kubectl apply -f 10-n9e-mysql.yaml
kubectl apply -f 11-nightingale.yaml
kubectl apply -f 12-categraf.yaml

# 访问Nightingale
# http://192.168.0.140:17000
```

## 📝 后续优化

1. **备份策略**: 配置ClickHouse定期备份到外部存储
2. **资源限制**: 根据实际负载调整CPU/内存Limits
3. **自动扩缩容**: worker-168加入后配置Pod分布策略
4. **监控告警**: 配置Nightingale告警规则
5. **数据清理**: 设置ClickHouse TTL策略清理旧数据

## 🔐 安全建议

部署到生产环境前,务必修改:

```yaml
# 02-secrets.yaml
- CLICKHOUSE_PASSWORD: 修改为强密码
- REDIS_PASSWORD: 修改为强密码
- DATAEASE_MYSQL_PASSWORD: 修改为强密码
- GRAFANA_ADMIN_PASSWORD: 修改为强密码
```

## 📞 支持

遇到问题查看:
1. Pod日志: `kubectl logs <pod-name> -n nginx-analytics`
2. 事件: `kubectl get events -n nginx-analytics --sort-by='.lastTimestamp'`
3. 资源状态: `kubectl describe <resource> -n nginx-analytics`
