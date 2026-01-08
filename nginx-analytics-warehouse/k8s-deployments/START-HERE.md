# 🚀 快速开始 - 三步部署

> **零配置，直接运行！**所有密码、配置已自动设置，无需手动修改任何文件。

---

## ⚡ 前置条件检查

在Windows PowerShell (管理员模式) 中运行：

```powershell
# 检查K8s集群
kubectl get nodes

# 应该看到3个节点：
# master-140   Ready   control-plane   ...
# worker-148   Ready   <none>          ...
# worker-168   Ready   <none>          ...
```

---

## 📦 三步部署流程

### 步骤 1️⃣：部署所有K8s服务

```powershell
cd D:\project\nginx-log-analyzer\nginx-analytics-warehouse\k8s-deployments
.\deploy-all.ps1
```

**这个脚本会自动：**
- ✅ 创建命名空间 nginx-analytics
- ✅ 部署 ClickHouse (8123端口)
- ✅ 部署 Redis (6380端口)
- ✅ 部署 DataEase MySQL + DataEase
- ✅ 部署 Grafana (3000端口)
- ✅ 等待所有Pod就绪 (约3-5分钟)

**预期时间：** 5-8分钟

---

### 步骤 2️⃣：初始化ClickHouse数据库

```powershell
.\init-clickhouse-db.ps1
```

**这个脚本会自动：**
- ✅ 检查 ClickHouse Pod 状态
- ✅ 使用 database_manager_unified.py 执行DDL
- ✅ 创建ODS/DWD/ADS三层表结构
- ✅ 创建17个物化视图
- ✅ 验证表创建成功

**预期时间：** 1-2分钟

---

### 步骤 3️⃣：配置ETL连接到K8s

```powershell
.\config-etl-auto.ps1
```

**这个脚本会自动：**
- ✅ 备份原配置文件
- ✅ 修改 dwd_writer.py 连接到 192.168.0.140:8123
- ✅ 配置ClickHouse用户名/密码
- ✅ 测试连接是否成功

**预期时间：** 10秒

---

## ✅ 验证部署

### 1. 检查服务状态

```powershell
kubectl get pods -n nginx-analytics
```

所有Pod应该都是 `Running` 状态。

### 2. 访问服务

| 服务 | 地址 | 账号 |
|------|------|------|
| **Grafana** | http://192.168.0.140:3000 | admin / admin123 |
| **DataEase** | http://192.168.0.140:8810 | admin / DataEase123@ |
| **ClickHouse** | http://192.168.0.140:8123 | analytics_user / analytics_password_change_in_prod |

### 3. 验证ClickHouse表

```powershell
kubectl exec -it clickhouse-0 -n nginx-analytics -- clickhouse-client --user=analytics_user --password=analytics_password_change_in_prod -q "SHOW TABLES FROM nginx_analytics"
```

应该看到：
- `ods_nginx_raw` (ODS层)
- `dwd_nginx_enriched_v3` (DWD层)
- `ads_*` (18个ADS主题表)
- `mv_*` (17个物化视图)

---

## 🧪 测试ETL数据流

### 1. 切换到ETL目录

```powershell
cd D:\project\nginx-log-analyzer\nginx-analytics-warehouse\etl
```

### 2. 激活conda环境

```powershell
conda activate py39
```

### 3. 测试处理100条日志

```powershell
python controllers\integrated_ultra_etl_controller.py --date 20250106 --test --limit 100
```

### 4. 验证数据写入

```powershell
kubectl exec -it clickhouse-0 -n nginx-analytics -- clickhouse-client --user=analytics_user --password=analytics_password_change_in_prod -q "SELECT count(*) FROM nginx_analytics.dwd_nginx_enriched_v3"
```

应该看到 `100` (或处理的记录数)。

---

## 🎯 下一步

1. **配置Grafana Dashboard**
   - 访问 http://192.168.0.140:3000
   - 已自动配置ClickHouse数据源
   - 导入预置Dashboard或创建自定义面板

2. **运行完整ETL处理**
   ```powershell
   # 处理指定日期的全量日志
   python controllers\integrated_ultra_etl_controller.py --date 20250106
   ```

3. **设置定时任务**
   - 凌晨1:00 下载日志 (已有脚本)
   - 凌晨1:30 运行ETL处理

4. **【可选】部署Nightingale监控栈 (方案A)**
   - 参考 `README.md` 中的"方案A"部署步骤
   - 需要额外部署：VictoriaMetrics, N9E MySQL, Nightingale, Categraf

---

## ❓ 常见问题

### Pod启动失败？

```powershell
# 查看Pod日志
kubectl logs <pod-name> -n nginx-analytics

# 查看Pod事件
kubectl describe pod <pod-name> -n nginx-analytics
```

### PVC无法绑定？

检查worker-148上的目录权限：
```powershell
# 在WSL中
ls -la /mnt/d/soft_work/k8s-data/
```

### ClickHouse连接失败？

```powershell
# 测试连接
curl http://192.168.0.140:8123/ping

# 应该返回 "Ok."
```

### ETL写入失败？

检查 `dwd_writer.py` 配置：
```powershell
# 应该包含：
# self.host = '192.168.0.140'
# self.port = 8123
# self.user = 'analytics_user'
# self.password = 'analytics_password_change_in_prod'
```

---

## 📚 详细文档

- **完整部署指南**: `README.md`
- **部署检查清单**: `DEPLOYMENT-CHECKLIST.md`
- **30分钟快速入门**: `QUICKSTART.md`
- **文件索引**: `FILES-INDEX.md`

---

## 🎊 部署完成！

**现在你已经有了：**
- ✅ K8s上运行的数据仓库 (ClickHouse + Redis)
- ✅ 完整的三层表结构 (ODS/DWD/ADS)
- ✅ 自动聚合的物化视图
- ✅ 可视化平台 (Grafana + DataEase)
- ✅ 配置好的ETL连接

**开始分析你的Nginx日志吧！** 🚀
