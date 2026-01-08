# 📁 文件清单索引

## 📊 目录结构

```
D:\soft_work\k8s-deployments\nginx-analytics\
├── README.md                           # 完整部署文档
├── QUICKSTART.md                       # 30分钟快速开始指南
├── DEPLOYMENT-CHECKLIST.md            # 部署检查清单
├── FILES-INDEX.md                      # 本文件 - 文件清单索引
├── config-etl-for-k8s.md              # ETL配置指南
│
├── 00-namespace.yaml                   # Kubernetes命名空间
├── 01-configmap.yaml                   # ConfigMap配置
├── 02-secrets.yaml                     # Secret密钥配置
├── 03-persistent-volumes.yaml          # PV和PVC存储配置
│
├── 04-clickhouse.yaml                  # ClickHouse StatefulSet
├── 05-redis.yaml                       # Redis Deployment
├── 06-dataease-mysql.yaml              # DataEase MySQL StatefulSet
├── 07-dataease.yaml                    # DataEase Deployment
├── 08-grafana.yaml                     # Grafana Deployment
│
├── deploy-all.sh                       # 一键部署脚本
└── init-clickhouse-db.sh               # ClickHouse数据库初始化脚本
```

---

## 📋 文件分类说明

### 核心部署配置 (YAML)

#### 1. 基础配置 (00-03)

| 文件 | 用途 | 内容 |
|------|------|------|
| **00-namespace.yaml** | 命名空间 | 创建nginx-analytics命名空间 |
| **01-configmap.yaml** | 配置文件 | ClickHouse配置、Redis配置、Grafana数据源配置、DataEase配置 |
| **02-secrets.yaml** | 密钥管理 | ClickHouse密码、Redis密码、Grafana密码、MySQL密码 |
| **03-persistent-volumes.yaml** | 持久化存储 | 5个PV和PVC（ClickHouse 50G、Redis 5G、MySQL 10G、DataEase 10G、Grafana 2G） |

#### 2. 服务部署配置 (04-08)

| 文件 | 服务 | 类型 | 副本 | 存储 | 端口 |
|------|------|------|------|------|------|
| **04-clickhouse.yaml** | ClickHouse | StatefulSet | 1 | 50Gi | 8123, 9000 |
| **05-redis.yaml** | Redis | Deployment | 1 | 5Gi | 6380 |
| **06-dataease-mysql.yaml** | MySQL | StatefulSet | 1 | 10Gi | 3306(内部) |
| **07-dataease.yaml** | DataEase | Deployment | 1 | 10Gi | 8810 |
| **08-grafana.yaml** | Grafana | Deployment | 1 | 2Gi | 3000 |

### 部署脚本

| 文件 | 用途 | 执行环境 |
|------|------|----------|
| **deploy-all.sh** | 一键部署所有服务 | master-140 WSL |
| **init-clickhouse-db.sh** | 初始化ClickHouse数据库(执行DDL) | master-140 WSL |

### 文档指南

| 文件 | 用途 | 适用场景 |
|------|------|----------|
| **README.md** | 完整部署文档 | 详细了解系统架构和部署步骤 |
| **QUICKSTART.md** | 快速开始指南 | 30分钟快速部署 |
| **DEPLOYMENT-CHECKLIST.md** | 部署检查清单 | 确保部署完整性和正确性 |
| **config-etl-for-k8s.md** | ETL配置指南 | 配置ETL连接到K8s ClickHouse |
| **FILES-INDEX.md** | 本文件 | 文件清单和说明 |

---

## 🚀 使用顺序

### 阶段1: 准备阶段

1. **阅读文档**
   - [ ] 先阅读 `README.md` 了解整体架构
   - [ ] 然后阅读 `QUICKSTART.md` 了解部署步骤
   - [ ] 使用 `DEPLOYMENT-CHECKLIST.md` 做前置检查

2. **准备环境**
   ```powershell
   # 在worker-148创建数据目录
   mkdir D:\soft_work\k8s-data\clickhouse-data -Force
   mkdir D:\soft_work\k8s-data\redis-data -Force
   mkdir D:\soft_work\k8s-data\dataease-mysql-data -Force
   mkdir D:\soft_work\k8s-data\dataease-data -Force
   mkdir D:\soft_work\k8s-data\grafana-data -Force
   ```

### 阶段2: 部署阶段

**推荐方式: 使用一键部署脚本**

```bash
# 在master-140 WSL执行
cd /mnt/d/soft_work/k8s-deployments/nginx-analytics
chmod +x deploy-all.sh
./deploy-all.sh
```

**手动方式: 逐步部署**

```bash
# 基础配置
kubectl apply -f 00-namespace.yaml
kubectl apply -f 01-configmap.yaml
kubectl apply -f 02-secrets.yaml
kubectl apply -f 03-persistent-volumes.yaml

# 等待PVC绑定
kubectl get pvc -n nginx-analytics -w

# 核心服务
kubectl apply -f 04-clickhouse.yaml
kubectl apply -f 05-redis.yaml

# 等待核心服务就绪
kubectl wait --for=condition=ready pod -l app=clickhouse -n nginx-analytics --timeout=180s
kubectl wait --for=condition=ready pod -l app=redis -n nginx-analytics --timeout=120s

# 可视化服务
kubectl apply -f 06-dataease-mysql.yaml
kubectl apply -f 07-dataease.yaml
kubectl apply -f 08-grafana.yaml

# 等待所有服务就绪
kubectl get pods -n nginx-analytics -w
```

### 阶段3: 初始化阶段

```bash
# 在master-140 WSL执行
cd /mnt/d/soft_work/k8s-deployments/nginx-analytics
chmod +x init-clickhouse-db.sh
./init-clickhouse-db.sh
```

### 阶段4: ETL配置阶段

参考 `config-etl-for-k8s.md` 配置ETL连接

```powershell
# 在worker-148 Windows执行
# 1. 修改dwd_writer.py的ClickHouse连接
# 2. 测试连接
# 3. 运行ETL测试
```

### 阶段5: 验证阶段

使用 `DEPLOYMENT-CHECKLIST.md` 逐项检查

---

## 📌 重要配置项

### 需要修改的密码

**文件**: `02-secrets.yaml`

```yaml
stringData:
  clickhouse-password: "analytics_password_change_in_prod"  # 修改
  redis-password: "redis_password_change_in_prod"           # 修改
  dataease-mysql-root-password: "Password123@mysql"        # 修改
  grafana-admin-password: "admin123"                        # 修改
```

### 存储路径配置

**文件**: `03-persistent-volumes.yaml`

所有PV的hostPath都指向: `/mnt/d/soft_work/k8s-data/<service>-data`

对应Windows路径: `D:\soft_work\k8s-data\<service>-data`

### 节点选择器

**所有服务都固定在worker-148节点**

```yaml
nodeSelector:
  kubernetes.io/hostname: worker-148
```

如需调整节点分布，修改此配置。

---

## 🔍 快速定位指南

### 我想要...

#### 快速部署系统
→ 阅读 `QUICKSTART.md`，执行 `deploy-all.sh`

#### 了解系统架构
→ 阅读 `README.md` 的"核心架构"和"服务清单"部分

#### 解决部署问题
→ 参考 `README.md` 的"故障排查"部分

#### 配置ETL连接
→ 阅读 `config-etl-for-k8s.md`

#### 检查部署是否完整
→ 使用 `DEPLOYMENT-CHECKLIST.md` 逐项核对

#### 修改ClickHouse配置
→ 编辑 `01-configmap.yaml` 的 `clickhouse-config.xml`

#### 调整资源限制
→ 编辑对应服务的YAML文件(04-08)中的 `resources` 部分

#### 修改存储大小
→ 编辑 `03-persistent-volumes.yaml` 中对应PV的 `storage` 大小

#### 更换存储位置
→ 编辑 `03-persistent-volumes.yaml` 中对应PV的 `hostPath.path`

#### 查看访问地址
→ 查看 `README.md` 或 `QUICKSTART.md` 的"访问地址"部分

---

## 🔐 安全注意事项

### 生产环境部署前必须修改

1. **所有默认密码** (02-secrets.yaml)
   - ClickHouse密码
   - Redis密码
   - MySQL密码
   - Grafana密码

2. **网络访问控制**
   - 根据需求调整Service类型(LoadBalancer/ClusterIP)
   - 考虑添加NetworkPolicy

3. **RBAC权限控制**
   - 创建专用ServiceAccount
   - 配置最小权限原则

---

## 📦 依赖关系图

```
ETL Pipeline (Windows)
    ↓ (通过LoadBalancer:8123)
ClickHouse ← Grafana (读取数据)
    ↑           ↓
Redis     DataEase ← DataEase MySQL
```

### 启动顺序

1. **第一层** (无依赖): Namespace, ConfigMap, Secret, PV/PVC
2. **第二层** (存储就绪): ClickHouse, Redis, DataEase MySQL
3. **第三层** (数据库就绪): DataEase, Grafana
4. **第四层** (服务就绪): ETL初始化、数据导入

---

## 🔄 更新和维护

### 更新服务镜像

```bash
# 方法1: 修改YAML后重新应用
kubectl apply -f 04-clickhouse.yaml

# 方法2: 直接修改Deployment
kubectl set image deployment/grafana grafana=grafana/grafana:10.0.0 -n nginx-analytics

# 方法3: 编辑资源
kubectl edit deployment grafana -n nginx-analytics
```

### 扩缩容

```bash
# 扩展Redis副本(非StatefulSet可以扩展)
kubectl scale deployment redis --replicas=2 -n nginx-analytics

# StatefulSet扩展(慎重，需要考虑数据分片)
kubectl scale statefulset clickhouse --replicas=3 -n nginx-analytics
```

### 备份配置

```bash
# 导出所有配置
kubectl get all,pv,pvc,cm,secret -n nginx-analytics -o yaml > backup-$(date +%Y%m%d).yaml

# 只导出特定资源
kubectl get deployment,statefulset -n nginx-analytics -o yaml > deployments-backup.yaml
```

---

## 🆘 紧急操作

### 重启所有服务

```bash
kubectl rollout restart deployment -n nginx-analytics
kubectl rollout restart statefulset -n nginx-analytics
```

### 强制重新拉取镜像

```bash
kubectl delete pod --all -n nginx-analytics
```

### 清理重新部署

```bash
# ⚠️ 警告：会删除所有数据
kubectl delete namespace nginx-analytics
kubectl delete pv clickhouse-data-pv redis-data-pv dataease-mysql-data-pv dataease-data-pv grafana-data-pv

# 重新部署
./deploy-all.sh
```

---

## 📞 获取支持

### 查看日志

```bash
# 查看Pod日志
kubectl logs <pod-name> -n nginx-analytics

# 实时跟踪日志
kubectl logs -f <pod-name> -n nginx-analytics

# 查看前一个容器的日志(重启后)
kubectl logs <pod-name> -n nginx-analytics --previous
```

### 查看事件

```bash
# 查看最新事件
kubectl get events -n nginx-analytics --sort-by='.lastTimestamp'

# 查看特定资源的事件
kubectl describe pod <pod-name> -n nginx-analytics
```

### 进入容器调试

```bash
# 进入ClickHouse
kubectl exec -it clickhouse-0 -n nginx-analytics -- bash

# 进入Redis
kubectl exec -it deployment/redis -n nginx-analytics -- sh

# 执行单条命令
kubectl exec clickhouse-0 -n nginx-analytics -- clickhouse-client -q "SELECT 1"
```

---

**版本信息**
- 文档版本: v1.0
- 创建日期: 2026-01-07
- K3s版本: v1.34.3+k3s1
- 部署方案: 方案B - Grafana快速验证方案
