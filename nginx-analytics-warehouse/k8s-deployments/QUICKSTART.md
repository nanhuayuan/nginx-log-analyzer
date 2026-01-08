# 🚀 快速开始 - 30分钟部署指南

## 前置条件检查

```bash
# 在master-140节点执行
kubectl get nodes -o wide

# 期望输出：
# NAME         STATUS   ROLES                  AGE   VERSION
# master-140   Ready    control-plane,master   1d    v1.34.3+k3s1
# worker-148   Ready    <none>                 1d    v1.34.3+k3s1
# worker-168   Ready    <none>                 1d    v1.34.3+k3s1
```

## Step 1: 准备存储目录 (worker-148)

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

## Step 2: 一键部署服务 (master-140 WSL)

```bash
# 在master-140的WSL(k3s-server)中执行
cd /mnt/d/soft_work/k8s-deployments/nginx-analytics

# 方式1: 使用一键部署脚本(推荐)
chmod +x deploy-all.sh
./deploy-all.sh

# 方式2: 手动逐步部署
kubectl apply -f 00-namespace.yaml
kubectl apply -f 01-configmap.yaml
kubectl apply -f 02-secrets.yaml
kubectl apply -f 03-persistent-volumes.yaml

# 等待PVC绑定
kubectl get pvc -n nginx-analytics -w

# 部署服务
kubectl apply -f 04-clickhouse.yaml
kubectl apply -f 05-redis.yaml
kubectl apply -f 06-dataease-mysql.yaml
kubectl apply -f 07-dataease.yaml
kubectl apply -f 08-grafana.yaml
```

## Step 3: 验证部署状态

```bash
# 查看所有Pod
kubectl get pods -n nginx-analytics -o wide

# 期望所有Pod都是Running状态
# NAME                        READY   STATUS    RESTARTS   AGE   NODE
# clickhouse-0                1/1     Running   0          5m    worker-148
# redis-xxx                   1/1     Running   0          4m    worker-148
# dataease-mysql-0            1/1     Running   0          3m    worker-148
# dataease-xxx                1/1     Running   0          2m    worker-148
# grafana-xxx                 1/1     Running   0          1m    worker-148

# 查看服务暴露端口
kubectl get svc -n nginx-analytics

# 期望输出LoadBalancer类型的服务都有EXTERNAL-IP
```

## Step 4: 初始化ClickHouse数据库

```bash
# 在master-140 WSL中执行
cd /mnt/d/soft_work/k8s-deployments/nginx-analytics

chmod +x init-clickhouse-db.sh
./init-clickhouse-db.sh

# 验证表创建成功
kubectl exec -it clickhouse-0 -n nginx-analytics -- clickhouse-client \
  --user=analytics_user \
  --password=analytics_password_change_in_prod \
  -q "SHOW TABLES FROM nginx_analytics"

# 应该看到以下表：
# - ods_nginx_raw
# - dwd_nginx_enriched_v3
# - ads_* (18个表)
```

## Step 5: 配置ETL连接 (worker-148 Windows)

```powershell
# 修改ETL Writer配置
notepad D:\project\nginx-log-analyzer\nginx-analytics-warehouse\etl\writers\dwd_writer.py

# 修改ClickHouse连接为:
# self.host = '192.168.0.140'
# self.port = 8123

# 测试连接
curl http://192.168.0.140:8123/ping
# 应该返回: Ok.
```

## Step 6: 运行ETL测试

```powershell
# 在worker-148 PowerShell中执行
cd D:\project\nginx-log-analyzer\nginx-analytics-warehouse\etl

# 激活conda环境
conda activate py39

# 测试处理(处理100条记录)
python controllers\integrated_ultra_etl_controller.py --date 20250106 --test --limit 100

# 验证数据写入
kubectl exec -it clickhouse-0 -n nginx-analytics -- clickhouse-client \
  --user=analytics_user \
  --password=analytics_password_change_in_prod \
  -q "SELECT count(*) as total_records FROM nginx_analytics.dwd_nginx_enriched_v3"

# 应该看到100条记录
```

## Step 7: 访问可视化界面

### Grafana (http://192.168.0.140:3000)

1. 打开浏览器访问: `http://192.168.0.140:3000`
2. 登录账号: `admin` / `admin123`
3. 左侧菜单 -> Connections -> Data sources
4. 验证ClickHouse数据源已配置
5. 创建Dashboard:
   - 点击左侧"+" -> "New Dashboard"
   - Add visualization -> 选择ClickHouse数据源
   - 输入查询SQL测试

示例查询:
```sql
SELECT
    toStartOfHour(log_time) as time,
    count(*) as requests,
    avg(total_request_duration) as avg_response_time,
    quantile(0.95)(total_request_duration) as p95_response_time
FROM nginx_analytics.dwd_nginx_enriched_v3
WHERE log_time >= now() - INTERVAL 24 HOUR
GROUP BY time
ORDER BY time
```

### DataEase (http://192.168.0.140:8810)

1. 打开浏览器访问: `http://192.168.0.140:8810`
2. 首次访问需要初始化(等待1-2分钟)
3. 默认账号: `admin` / `DataEase123@`
4. 配置ClickHouse数据源:
   - 数据源管理 -> 新建数据源
   - 选择 "ClickHouse"
   - 主机: `clickhouse-service.nginx-analytics.svc.cluster.local`
   - 端口: `8123`
   - 数据库: `nginx_analytics`
   - 用户名: `analytics_user`
   - 密码: `analytics_password_change_in_prod`

### ClickHouse Web UI (http://192.168.0.140:8123/play)

1. 打开浏览器访问: `http://192.168.0.140:8123/play`
2. 输入用户名: `analytics_user`
3. 输入密码: `analytics_password_change_in_prod`
4. 执行SQL查询测试数据

## 常见问题

### 1. Pod一直处于Pending状态

```bash
# 查看Pod事件
kubectl describe pod <pod-name> -n nginx-analytics

# 常见原因：PVC未绑定
kubectl get pvc -n nginx-analytics

# 检查PV
kubectl get pv | grep clickhouse

# 解决方案：确认worker-148节点标签正确
kubectl label nodes worker-148 kubernetes.io/hostname=worker-148 --overwrite
```

### 2. ClickHouse无法启动

```bash
# 查看日志
kubectl logs clickhouse-0 -n nginx-analytics

# 检查数据目录权限(在worker-148 WSL中)
ls -la /mnt/d/soft_work/k8s-data/clickhouse-data

# 如果权限问题，修复权限
chmod -R 777 /mnt/d/soft_work/k8s-data/clickhouse-data
```

### 3. ETL无法连接ClickHouse

```powershell
# 测试网络连通性
Test-NetConnection -ComputerName 192.168.0.140 -Port 8123

# 检查Service状态
kubectl get svc clickhouse-service -n nginx-analytics

# 验证端口转发
netstat -ano | findstr "8123"
```

### 4. DataEase无法访问

```bash
# 查看DataEase日志
kubectl logs deployment/dataease -n nginx-analytics

# 检查MySQL是否就绪
kubectl get pods -n nginx-analytics | grep mysql

# 重启DataEase
kubectl rollout restart deployment/dataease -n nginx-analytics
```

## 性能优化建议

### 调整资源限制

根据实际负载调整资源配置:

```yaml
# 编辑Deployment
kubectl edit deployment clickhouse -n nginx-analytics

# 修改resources部分
resources:
  requests:
    memory: "4Gi"  # 增加内存
    cpu: "2000m"
  limits:
    memory: "8Gi"
    cpu: "4000m"
```

### 优化ClickHouse配置

```bash
# 进入ClickHouse Pod
kubectl exec -it clickhouse-0 -n nginx-analytics -- bash

# 修改配置
vi /etc/clickhouse-server/config.d/performance.xml

# 重启Pod使配置生效
kubectl delete pod clickhouse-0 -n nginx-analytics
```

## 下一步

1. **配置定时任务**: 确保Windows定时任务正常运行
2. **创建Grafana Dashboard**: 根据业务需求创建监控面板
3. **设置告警规则**: 配置性能和错误告警
4. **数据备份策略**: 定期备份ClickHouse数据
5. **性能监控**: 观察系统资源使用情况

## 升级到方案A (Nightingale)

验证成功后，可部署完整监控方案:

```bash
# 部署Nightingale监控栈
kubectl apply -f 09-victoriametrics.yaml
kubectl apply -f 10-n9e-mysql.yaml
kubectl apply -f 11-nightingale.yaml
kubectl apply -f 12-categraf.yaml

# 访问Nightingale
# http://192.168.0.140:17000
# 默认账号: root / root.2020
```

## 获取帮助

遇到问题时:

1. 查看Pod日志: `kubectl logs <pod-name> -n nginx-analytics`
2. 查看事件: `kubectl get events -n nginx-analytics --sort-by='.lastTimestamp'`
3. 查看资源状态: `kubectl describe <resource> -n nginx-analytics`
4. 参考详细文档: `README.md`
5. 参考ETL配置: `config-etl-for-k8s.md`
