# ✅ Nginx日志分析数据仓库 - K8s迁移配置完成总结

## 🎉 配置生成完成！

所有Kubernetes部署配置文件已成功生成，存放在：

```
D:\soft_work\k8s-deployments\nginx-analytics\
```

---

## 📦 已生成文件清单 (共16个文件)

### 📚 文档文件 (5个)

1. **README.md** - 完整部署文档，包含架构说明、部署步骤、故障排查
2. **QUICKSTART.md** - 30分钟快速开始指南，适合快速部署
3. **DEPLOYMENT-CHECKLIST.md** - 部署检查清单，确保部署完整性
4. **config-etl-for-k8s.md** - ETL配置指南，连接到K8s ClickHouse
5. **FILES-INDEX.md** - 文件清单索引，快速定位所需文件

### ⚙️ Kubernetes配置文件 (9个)

#### 基础配置 (4个)
- **00-namespace.yaml** - 命名空间
- **01-configmap.yaml** - 配置文件 (ClickHouse/Redis/Grafana/DataEase)
- **02-secrets.yaml** - 密钥管理 (所有密码)
- **03-persistent-volumes.yaml** - 持久化存储 (5个PV和PVC)

#### 服务部署 (5个)
- **04-clickhouse.yaml** - ClickHouse StatefulSet (核心数据库)
- **05-redis.yaml** - Redis Deployment (缓存服务)
- **06-dataease-mysql.yaml** - DataEase MySQL StatefulSet
- **07-dataease.yaml** - DataEase Deployment (BI可视化)
- **08-grafana.yaml** - Grafana Deployment (性能监控)

### 🔧 部署脚本 (2个)

- **deploy-all.sh** - 一键部署所有服务的自动化脚本
- **init-clickhouse-db.sh** - ClickHouse数据库初始化脚本 (执行DDL)

---

## 🏗️ 部署架构概览

### 服务清单

| 序号 | 服务 | 类型 | 副本 | 存储 | 端口 | 访问地址 |
|------|------|------|------|------|------|----------|
| 1 | ClickHouse | StatefulSet | 1 | 50Gi | 8123, 9000 | http://192.168.0.140:8123 |
| 2 | Redis | Deployment | 1 | 5Gi | 6380 | (内部访问) |
| 3 | DataEase MySQL | StatefulSet | 1 | 10Gi | 3306 | (内部访问) |
| 4 | DataEase | Deployment | 1 | 10Gi | 8810 | http://192.168.0.140:8810 |
| 5 | Grafana | Deployment | 1 | 2Gi | 3000 | http://192.168.0.140:3000 |

**总计**: 5个Pod，~80Gi存储，全部运行在worker-148节点

### 数据流架构

```
Windows定时任务 (worker-148)
    ↓ 1:00 下载日志
nginx_zip_log_processor.py → nginx_logs/YYYYMMDD/*.log
    ↓ 1:30 ETL处理
integrated_ultra_etl_controller.py
    ↓ 批量写入(3000条/批，6线程)
ClickHouse (K8s LoadBalancer:8123)
    ↓ 自动触发物化视图
18个ADS主题表
    ↓ 可视化查询
┌─────────────┬──────────────┐
│  Grafana    │   DataEase   │
│  (监控)     │   (BI分析)   │
└─────────────┴──────────────┘
```

---

## 🚀 下一步操作指南

### Step 1: 在worker-148创建存储目录

```powershell
# 在worker-148的PowerShell(管理员)中执行
mkdir D:\soft_work\k8s-data\clickhouse-data -Force
mkdir D:\soft_work\k8s-data\redis-data -Force
mkdir D:\soft_work\k8s-data\dataease-mysql-data -Force
mkdir D:\soft_work\k8s-data\dataease-data -Force
mkdir D:\soft_work\k8s-data\grafana-data -Force

# 验证目录创建
ls D:\soft_work\k8s-data
```

### Step 2: 一键部署服务

```bash
# 在master-140的WSL(k3s-server)中执行
cd /mnt/d/soft_work/k8s-deployments/nginx-analytics

# 赋予执行权限
chmod +x deploy-all.sh
chmod +x init-clickhouse-db.sh

# 执行一键部署
./deploy-all.sh

# 预计耗时: 5-10分钟
# 自动完成: 命名空间创建、配置部署、存储绑定、服务启动
```

### Step 3: 初始化ClickHouse数据库

```bash
# 在master-140 WSL中执行
./init-clickhouse-db.sh

# 预计耗时: 2-3分钟
# 自动完成: 复制DDL文件、执行建表语句、创建物化视图
```

### Step 4: 配置ETL连接

```powershell
# 在worker-148 Windows中执行

# 1. 修改ETL连接配置
notepad D:\project\nginx-log-analyzer\nginx-analytics-warehouse\etl\writers\dwd_writer.py

# 修改以下内容:
#   self.host = '192.168.0.140'  # 改为K3s LoadBalancer IP
#   self.port = 8123

# 2. 测试连接
curl http://192.168.0.140:8123/ping
# 应该返回: Ok.

# 3. 运行ETL测试
cd D:\project\nginx-log-analyzer\nginx-analytics-warehouse\etl
conda activate py39
python controllers\integrated_ultra_etl_controller.py --date 20250106 --test --limit 100
```

### Step 5: 验证系统

```bash
# 查看所有Pod状态
kubectl get pods -n nginx-analytics -o wide

# 查看数据写入
kubectl exec -it clickhouse-0 -n nginx-analytics -- clickhouse-client \
  --user=analytics_user \
  --password=analytics_password_change_in_prod \
  -q "SELECT count(*) FROM nginx_analytics.dwd_nginx_enriched_v3"

# 访问Grafana
# 浏览器打开: http://192.168.0.140:3000
# 账号: admin / admin123
```

---

## 📋 重要提示

### 🔐 安全配置

**⚠️ 部署前必须修改默认密码！**

编辑文件: `02-secrets.yaml`

```yaml
stringData:
  clickhouse-password: "修改为强密码"
  redis-password: "修改为强密码"
  dataease-mysql-root-password: "修改为强密码"
  grafana-admin-password: "修改为强密码"
```

### 💾 存储规划

**worker-148 D盘存储规划**:

```
D:\soft_work\k8s-data\
├── clickhouse-data/          (50GB) - ClickHouse数据库
├── redis-data/               (5GB)  - Redis持久化
├── dataease-mysql-data/      (10GB) - MySQL数据
├── dataease-data/            (10GB) - DataEase应用数据
└── grafana-data/             (2GB)  - Grafana配置

总计: ~77GB
```

### ⏱️ ETL定时任务

**保留Windows定时任务配置**:

1. **日志下载**: 每天凌晨1:00
   - 任务: NginxZipLogProcessor
   - 脚本: `D:\project\nginx-log-analyzer\zip\nginx_zip_log_processor.py`

2. **ETL处理**: 每天凌晨1:30
   - 任务: NginxETLAutoProcessor
   - 脚本: `D:\project\nginx-log-analyzer\nginx-analytics-warehouse\etl\run_auto_etl.bat`
   - 参数: `--auto-monitor --monitor-duration 7200`

**无需修改定时任务配置**，只需修改ETL连接到K8s ClickHouse

---

## 🔍 故障排查

### 常见问题

#### 1. Pod一直Pending
```bash
# 检查PVC绑定状态
kubectl get pvc -n nginx-analytics

# 检查节点标签
kubectl get nodes worker-148 --show-labels | grep hostname
```

#### 2. ClickHouse无法启动
```bash
# 查看日志
kubectl logs clickhouse-0 -n nginx-analytics

# 检查数据目录权限(在worker-148 WSL)
ls -la /mnt/d/soft_work/k8s-data/clickhouse-data
```

#### 3. ETL无法连接
```powershell
# 测试网络
Test-NetConnection -ComputerName 192.168.0.140 -Port 8123

# 检查Service
kubectl get svc clickhouse-service -n nginx-analytics
```

更多故障排查请参考: `README.md` 的"故障排查"章节

---

## 📚 文档导航

### 快速查找

| 我想要... | 查看文档 |
|-----------|----------|
| **快速部署** | QUICKSTART.md |
| **了解架构** | README.md |
| **解决问题** | README.md 故障排查章节 |
| **配置ETL** | config-etl-for-k8s.md |
| **检查部署** | DEPLOYMENT-CHECKLIST.md |
| **查找文件** | FILES-INDEX.md |

---

## 🎯 部署目标

### 成功标准

部署成功后，应该实现以下目标：

- ✅ 5个Pod全部运行正常 (Running状态)
- ✅ 所有PVC成功绑定到PV
- ✅ ClickHouse可以从外部访问 (192.168.0.140:8123)
- ✅ Grafana可以访问并连接ClickHouse
- ✅ DataEase可以访问并配置数据源
- ✅ ETL可以连接并写入数据到ClickHouse
- ✅ 物化视图自动聚合数据到ADS层
- ✅ 可视化界面正常展示数据

### 性能目标

- ETL处理速度: **2000+ RPS** (Records Per Second)
- ClickHouse查询响应: **< 1秒** (简单查询)
- 日数据处理能力: **1000万-1亿** 条记录
- 数据保留时间: **全量保留** (后续可配置TTL)

---

## 🔄 后续优化方向

### 1. 监控告警 (可选 - 方案A)

部署Nightingale完整监控栈:
- VictoriaMetrics (时序数据库)
- N9E MySQL (元数据存储)
- Nightingale (监控核心)
- Categraf (数据采集)

**访问地址**: http://192.168.0.140:17000

### 2. 高可用优化

- ClickHouse集群化 (3副本)
- Redis哨兵模式
- 多节点负载均衡
- 自动故障转移

### 3. 数据治理

- 设置TTL自动清理旧数据
- 配置数据备份策略
- 实施数据质量监控
- 建立数据访问权限

### 4. 性能调优

- 根据实际负载调整资源limits
- 优化ClickHouse配置参数
- 调整ETL批处理大小和线程数
- 添加缓存层加速查询

---

## 📞 获取支持

### 查看日志

```bash
# 查看Pod日志
kubectl logs <pod-name> -n nginx-analytics

# 实时跟踪日志
kubectl logs -f <pod-name> -n nginx-analytics
```

### 查看事件

```bash
# 查看最新事件
kubectl get events -n nginx-analytics --sort-by='.lastTimestamp'
```

### 进入容器调试

```bash
# 进入ClickHouse
kubectl exec -it clickhouse-0 -n nginx-analytics -- bash

# 执行SQL查询
kubectl exec -it clickhouse-0 -n nginx-analytics -- clickhouse-client \
  --user=analytics_user \
  --password=analytics_password_change_in_prod
```

---

## ✨ 总结

你现在拥有:

✅ **16个配置文件** - 完整的K8s部署配置
✅ **5个文档** - 详细的部署和操作指南
✅ **2个自动化脚本** - 一键部署和初始化
✅ **完整架构方案** - 从日志采集到可视化展示的全链路

**立即开始部署！**

参考 `QUICKSTART.md`，30分钟即可完成完整部署。

---

**版本信息**
- 配置生成时间: 2026-01-07
- K3s版本: v1.34.3+k3s1
- 部署方案: 方案B - Grafana快速验证方案
- 配置版本: v1.0

**记住方案A（Nightingale监控栈）**
验证成功后可随时升级到完整监控方案！
