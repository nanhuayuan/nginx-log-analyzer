# 📋 部署检查清单

## 部署前检查

### 环境准备

- [ ] K3s集群已部署并正常运行
- [ ] 所有节点状态为Ready
  ```bash
  kubectl get nodes
  ```
- [ ] worker-148节点标签正确
  ```bash
  kubectl get nodes worker-148 --show-labels | grep hostname
  ```
- [ ] master-140可以访问kubectl命令
- [ ] worker-148 Windows可以访问局域网

### 存储准备

- [ ] worker-148已创建数据目录
  ```powershell
  ls D:\soft_work\k8s-data
  ```
- [ ] 目录结构完整：
  - [ ] `D:\soft_work\k8s-data\clickhouse-data`
  - [ ] `D:\soft_work\k8s-data\redis-data`
  - [ ] `D:\soft_work\k8s-data\dataease-mysql-data`
  - [ ] `D:\soft_work\k8s-data\dataease-data`
  - [ ] `D:\soft_work\k8s-data\grafana-data`
- [ ] D盘剩余空间 > 100GB
  ```powershell
  Get-PSDrive D
  ```

### 文件准备

- [ ] YAML配置文件已就位
  ```bash
  ls /mnt/d/soft_work/k8s-deployments/nginx-analytics/*.yaml
  ```
- [ ] DDL文件可访问
  ```bash
  ls /mnt/d/project/nginx-log-analyzer/nginx-analytics-warehouse/ddl/
  ```
- [ ] ETL代码可访问
  ```bash
  ls /mnt/d/project/nginx-log-analyzer/nginx-analytics-warehouse/etl/
  ```

---

## 部署过程检查

### Phase 1: 基础配置

- [ ] Namespace创建成功
  ```bash
  kubectl get namespace nginx-analytics
  ```
- [ ] ConfigMap创建成功
  ```bash
  kubectl get configmap -n nginx-analytics
  ```
- [ ] Secret创建成功
  ```bash
  kubectl get secret -n nginx-analytics
  ```

### Phase 2: 存储配置

- [ ] PersistentVolume创建成功
  ```bash
  kubectl get pv | grep clickhouse
  ```
- [ ] 所有PV状态为Available或Bound
- [ ] PersistentVolumeClaim创建成功
  ```bash
  kubectl get pvc -n nginx-analytics
  ```
- [ ] 所有PVC状态为Bound
  ```bash
  kubectl get pvc -n nginx-analytics -o wide
  ```

### Phase 3: 核心服务部署

#### ClickHouse

- [ ] StatefulSet创建成功
  ```bash
  kubectl get statefulset clickhouse -n nginx-analytics
  ```
- [ ] Pod运行正常
  ```bash
  kubectl get pods -l app=clickhouse -n nginx-analytics
  ```
- [ ] Service暴露端口正常
  ```bash
  kubectl get svc clickhouse-service -n nginx-analytics
  ```
- [ ] 健康检查通过
  ```bash
  kubectl get pods clickhouse-0 -n nginx-analytics -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}'
  ```
- [ ] 可以从外部访问
  ```bash
  curl http://192.168.0.140:8123/ping
  ```
- [ ] 数据目录挂载正确
  ```bash
  kubectl exec clickhouse-0 -n nginx-analytics -- df -h | grep clickhouse
  ```

#### Redis

- [ ] Deployment创建成功
- [ ] Pod运行正常
- [ ] Service暴露端口正常
- [ ] 可以连接测试
  ```bash
  kubectl exec -it deployment/redis -n nginx-analytics -- redis-cli -a redis_password_change_in_prod ping
  ```

### Phase 4: 数据库服务

#### DataEase MySQL

- [ ] StatefulSet创建成功
- [ ] Pod运行正常
- [ ] Service可访问
- [ ] MySQL可以连接
  ```bash
  kubectl exec -it dataease-mysql-0 -n nginx-analytics -- mysql -uroot -pPassword123@mysql -e "SELECT 1"
  ```

### Phase 5: 可视化服务

#### DataEase

- [ ] Deployment创建成功
- [ ] Pod运行正常(可能需要2-3分钟)
- [ ] Service暴露端口正常
- [ ] Web界面可访问
  ```bash
  curl -I http://192.168.0.140:8810
  ```
- [ ] 初始化完成

#### Grafana

- [ ] Deployment创建成功
- [ ] Pod运行正常
- [ ] Service暴露端口正常
- [ ] Web界面可访问
  ```bash
  curl http://192.168.0.140:3000/api/health
  ```
- [ ] ClickHouse数据源配置正确

---

## 数据库初始化检查

### DDL执行

- [ ] 01_ods_layer_real.sql执行成功
- [ ] 02_dwd_layer_real.sql执行成功
- [ ] 03_ads_layer_real.sql执行成功
- [ ] 04_materialized_views_corrected.sql执行成功

### 表验证

- [ ] nginx_analytics数据库存在
  ```bash
  kubectl exec clickhouse-0 -n nginx-analytics -- clickhouse-client -u analytics_user --password analytics_password_change_in_prod -q "SHOW DATABASES"
  ```
- [ ] ODS层表创建成功
  ```bash
  kubectl exec clickhouse-0 -n nginx-analytics -- clickhouse-client -u analytics_user --password analytics_password_change_in_prod -q "SHOW TABLES FROM nginx_analytics" | grep ods
  ```
- [ ] DWD层表创建成功
  ```bash
  kubectl exec clickhouse-0 -n nginx-analytics -- clickhouse-client -u analytics_user --password analytics_password_change_in_prod -q "SHOW TABLES FROM nginx_analytics" | grep dwd
  ```
- [ ] ADS层表创建成功(18个表)
  ```bash
  kubectl exec clickhouse-0 -n nginx-analytics -- clickhouse-client -u analytics_user --password analytics_password_change_in_prod -q "SHOW TABLES FROM nginx_analytics" | grep ads | wc -l
  ```
- [ ] 物化视图创建成功
  ```bash
  kubectl exec clickhouse-0 -n nginx-analytics -- clickhouse-client -u analytics_user --password analytics_password_change_in_prod -q "SHOW TABLES FROM nginx_analytics" | grep mv
  ```

---

## ETL配置检查

### 连接配置

- [ ] dwd_writer.py已修改ClickHouse连接
  ```powershell
  Get-Content D:\project\nginx-log-analyzer\nginx-analytics-warehouse\etl\writers\dwd_writer.py | Select-String "self.host"
  ```
- [ ] 连接配置正确
  - host: `192.168.0.140`
  - port: `8123`
  - user: `analytics_user`
  - password: `analytics_password_change_in_prod`

### 连接测试

- [ ] 可以从worker-148访问ClickHouse
  ```powershell
  Test-NetConnection -ComputerName 192.168.0.140 -Port 8123
  ```
- [ ] Python可以连接ClickHouse
  ```python
  # 在worker-148 WSL中测试
  python -c "from writers.dwd_writer import DWDWriter; w=DWDWriter(); print('Connected' if w.connect() else 'Failed')"
  ```

### ETL测试

- [ ] 测试模式运行成功
  ```powershell
  python controllers\integrated_ultra_etl_controller.py --test --limit 100
  ```
- [ ] 数据成功写入ClickHouse
  ```bash
  kubectl exec clickhouse-0 -n nginx-analytics -- clickhouse-client -u analytics_user --password analytics_password_change_in_prod -q "SELECT count(*) FROM nginx_analytics.dwd_nginx_enriched_v3"
  ```
- [ ] 物化视图自动聚合数据
  ```bash
  kubectl exec clickhouse-0 -n nginx-analytics -- clickhouse-client -u analytics_user --password analytics_password_change_in_prod -q "SELECT count(*) FROM nginx_analytics.ads_api_performance_analysis_v3"
  ```

### 定时任务

- [ ] NginxZipLogProcessor任务存在
  ```powershell
  Get-ScheduledTask -TaskName "NginxZipLogProcessor"
  ```
- [ ] NginxETLAutoProcessor任务存在
  ```powershell
  Get-ScheduledTask -TaskName "NginxETLAutoProcessor"
  ```
- [ ] 任务配置正确(时间/路径/参数)
- [ ] 日志目录可写
  ```powershell
  Test-Path D:\project\nginx-log-analyzer\nginx-analytics-warehouse\etl\logs -PathType Container
  ```

---

## 可视化验证

### Grafana

- [ ] 可以登录(admin/admin123)
- [ ] ClickHouse数据源连接成功
- [ ] 可以执行查询获取数据
- [ ] 创建测试Dashboard成功
- [ ] 图表显示正常

### DataEase

- [ ] 可以登录(admin/DataEase123@)
- [ ] 可以配置ClickHouse数据源
- [ ] 数据源连接测试成功
- [ ] 可以创建数据集
- [ ] 可以创建仪表板

### ClickHouse Web UI

- [ ] 可以访问 http://192.168.0.140:8123/play
- [ ] 可以登录(analytics_user/密码)
- [ ] 可以执行SQL查询
- [ ] 可以查看表结构

---

## 性能验证

### 资源使用

- [ ] 所有Pod CPU使用正常
  ```bash
  kubectl top pods -n nginx-analytics
  ```
- [ ] 所有Pod内存使用正常
- [ ] worker-148磁盘空间充足
  ```bash
  kubectl exec clickhouse-0 -n nginx-analytics -- df -h
  ```

### 数据处理性能

- [ ] ETL处理速度 > 2000 RPS
- [ ] ClickHouse查询响应时间 < 1秒(简单查询)
- [ ] 物化视图自动更新正常

---

## 安全检查

### 密码安全

- [ ] 已修改ClickHouse默认密码
- [ ] 已修改Redis默认密码
- [ ] 已修改MySQL默认密码
- [ ] 已修改Grafana默认密码
- [ ] Secret中不包含明文密码

### 网络安全

- [ ] 仅必要端口对外暴露
- [ ] 内部服务使用ClusterIP
- [ ] 未暴露敏感端口到外网

---

## 备份策略

- [ ] ClickHouse数据备份计划已制定
- [ ] 备份目录已创建
  ```powershell
  mkdir D:\soft_work\k8s-data\clickhouse-backup -Force
  ```
- [ ] 测试备份流程可用
- [ ] 测试恢复流程可用

---

## 文档完整性

- [ ] README.md已阅读
- [ ] QUICKSTART.md已按步骤执行
- [ ] config-etl-for-k8s.md已配置完成
- [ ] 所有访问地址已记录
- [ ] 所有账号密码已记录(安全保存)

---

## 最终验证

### 端到端测试

1. [ ] 日志文件已准备
2. [ ] 运行ETL处理日志
3. [ ] 数据成功写入ClickHouse
4. [ ] 物化视图自动聚合
5. [ ] Grafana可以查询并展示数据
6. [ ] DataEase可以创建报表
7. [ ] 监控指标正常

### 压力测试(可选)

- [ ] 处理大量数据(1000万+记录)
- [ ] 系统稳定运行
- [ ] 资源使用在可控范围
- [ ] 无OOM或磁盘满错误

---

## 问题记录

遇到的问题和解决方案:

| 问题描述 | 解决方案 | 解决时间 |
|---------|---------|---------|
|         |         |         |
|         |         |         |

---

## 签署确认

- [ ] 部署负责人: ____________  日期: ______
- [ ] 测试确认: ____________    日期: ______
- [ ] 上线批准: ____________    日期: ______

---

**注意**:
- 每完成一项打勾✓
- 遇到问题及时记录在"问题记录"中
- 所有检查项通过后方可上线生产环境
