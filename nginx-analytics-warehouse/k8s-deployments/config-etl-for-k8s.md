# ETL配置指南 - 连接到K8s ClickHouse

## 概述

ETL Pipeline继续在worker-148的Windows上运行(通过定时任务),但需要修改连接配置指向K8s集群中的ClickHouse。

## 修改步骤

### 1. 修改ClickHouse连接配置

编辑ETL Writer配置文件:

**文件路径**: `D:\project\nginx-log-analyzer\nginx-analytics-warehouse\etl\writers\dwd_writer.py`

**查找以下内容**:
```python
class DWDWriter:
    def __init__(self):
        self.host = 'localhost'  # 或 '127.0.0.1'
        self.port = 8123
        self.user = 'analytics_user'
        self.password = 'analytics_password_change_in_prod'
        self.database = 'nginx_analytics'
```

**修改为**:
```python
class DWDWriter:
    def __init__(self):
        self.host = '192.168.0.140'  # K3s LoadBalancer暴露的IP
        self.port = 8123
        self.user = 'analytics_user'
        self.password = 'analytics_password_change_in_prod'
        self.database = 'nginx_analytics'
```

### 2. 测试连接

在worker-148的PowerShell中执行:

```powershell
# 测试HTTP连接
curl http://192.168.0.140:8123/ping

# 测试查询
$headers = @{
    "X-ClickHouse-User" = "analytics_user"
    "X-ClickHouse-Key" = "analytics_password_change_in_prod"
}
Invoke-RestMethod -Uri "http://192.168.0.140:8123/?query=SELECT 1" -Headers $headers
```

### 3. 验证数据库连接

在worker-148 WSL中执行Python测试:

```bash
wsl -d k3s-agent-148

cd /mnt/d/project/nginx-log-analyzer/nginx-analytics-warehouse/etl

# 激活conda环境
conda activate py39

# 测试连接
python -c "
from writers.dwd_writer import DWDWriter
writer = DWDWriter()
if writer.connect():
    print('✓ ClickHouse连接成功')
    writer.close()
else:
    print('✗ ClickHouse连接失败')
"
```

### 4. 运行ETL测试

```powershell
# 在Windows PowerShell中
cd D:\project\nginx-log-analyzer\nginx-analytics-warehouse\etl

# 激活conda环境
conda activate py39

# 测试模式运行(处理100条记录)
python controllers\integrated_ultra_etl_controller.py --date 20250106 --test --limit 100

# 查看是否有数据写入
kubectl exec -it clickhouse-0 -n nginx-analytics -- clickhouse-client --user=analytics_user --password=analytics_password_change_in_prod -q "SELECT count(*) FROM nginx_analytics.dwd_nginx_enriched_v3"
```

### 5. 确认定时任务配置

定时任务无需修改,继续使用现有配置:

**日志下载任务**: 每天凌晨1:00
- 任务名: `NginxZipLogProcessor`
- 脚本: `D:\project\nginx-log-analyzer\zip\nginx_zip_log_processor.py`

**ETL处理任务**: 每天凌晨1:30
- 任务名: `NginxETLAutoProcessor`
- 脚本: `D:\project\nginx-log-analyzer\nginx-analytics-warehouse\etl\run_auto_etl.bat`
- 参数: `--auto-monitor --monitor-duration 7200 --batch-size 3000 --workers 6`

## 故障排查

### 问题1: 连接超时

```powershell
# 检查K3s Service状态
kubectl get svc clickhouse-service -n nginx-analytics

# 检查端口转发
netstat -ano | findstr "8123"

# 检查防火墙
Test-NetConnection -ComputerName 192.168.0.140 -Port 8123
```

### 问题2: 认证失败

```bash
# 验证ClickHouse用户权限
kubectl exec -it clickhouse-0 -n nginx-analytics -- clickhouse-client --user=analytics_user --password=analytics_password_change_in_prod -q "SELECT currentUser()"

# 检查Secret配置
kubectl get secret nginx-analytics-secrets -n nginx-analytics -o yaml
```

### 问题3: 数据写入失败

```python
# 在ETL代码中添加详细日志
import logging
logging.basicConfig(level=logging.DEBUG)

# 查看ClickHouse错误日志
kubectl logs clickhouse-0 -n nginx-analytics | tail -50
```

## 性能优化建议

### 1. 批处理大小调整

根据实际情况调整 `--batch-size` 参数:

- **小数据量** (< 100万/天): `--batch-size 2000`
- **中等数据量** (100万-1000万/天): `--batch-size 3000`
- **大数据量** (> 1000万/天): `--batch-size 5000`

### 2. 工作线程数调整

根据CPU核心数调整 `--workers` 参数:

- **4核CPU**: `--workers 4`
- **6核CPU**: `--workers 6`
- **8核CPU**: `--workers 8`

### 3. ClickHouse连接池

如果遇到连接耗尽,修改 `--pool-size` 参数:

```bash
--pool-size 8  # 默认等于workers数量
```

## 监控ETL运行状态

### 查看日志

```powershell
# 查看最新ETL日志
Get-Content D:\project\nginx-log-analyzer\nginx-analytics-warehouse\etl\logs\etl_auto_*.log -Tail 50 -Wait
```

### 查看数据统计

```bash
# 在worker-148 WSL中执行
kubectl exec -it clickhouse-0 -n nginx-analytics -- clickhouse-client --user=analytics_user --password=analytics_password_change_in_prod

# 查看各层数据量
SELECT
    table,
    formatReadableSize(sum(bytes)) as size,
    formatReadableQuantity(sum(rows)) as rows
FROM system.parts
WHERE database = 'nginx_analytics' AND active
GROUP BY table
ORDER BY sum(bytes) DESC;

# 查看最新数据时间
SELECT
    'dwd_nginx_enriched_v3' as table,
    max(log_time) as latest_time,
    count(*) as total_records
FROM nginx_analytics.dwd_nginx_enriched_v3;
```

## 常见场景

### 场景1: 首次数据导入

```bash
# 处理所有历史数据
python controllers\integrated_ultra_etl_controller.py --all

# 查看进度(另开一个终端)
Get-Content D:\project\nginx-log-analyzer\nginx-analytics-warehouse\etl\logs\etl_*.log -Tail 30 -Wait
```

### 场景2: 重新处理指定日期

```bash
# 强制重新处理
python controllers\integrated_ultra_etl_controller.py --date 20250106 --force

# 验证数据
kubectl exec -it clickhouse-0 -n nginx-analytics -- clickhouse-client --user=analytics_user --password=analytics_password_change_in_prod -q "SELECT toDate(log_time) as date, count(*) FROM nginx_analytics.dwd_nginx_enriched_v3 WHERE toDate(log_time) = '2025-01-06' GROUP BY date"
```

### 场景3: 清理错误数据

```bash
# 删除指定日期数据
kubectl exec -it clickhouse-0 -n nginx-analytics -- clickhouse-client --user=analytics_user --password=analytics_password_change_in_prod -q "ALTER TABLE nginx_analytics.dwd_nginx_enriched_v3 DELETE WHERE toDate(log_time) = '2025-01-06'"

# 重新处理
python controllers\integrated_ultra_etl_controller.py --date 20250106 --force
```

## 备份和恢复

### 备份ClickHouse数据

```bash
# 在worker-148创建备份目录
mkdir D:\soft_work\k8s-data\clickhouse-backup

# 执行备份
kubectl exec -it clickhouse-0 -n nginx-analytics -- clickhouse-client --user=analytics_user --password=analytics_password_change_in_prod -q "BACKUP DATABASE nginx_analytics TO Disk('default', 'backup/nginx_analytics_$(date +%Y%m%d).zip')"

# 复制备份文件到Windows
kubectl cp nginx-analytics/clickhouse-0:/var/lib/clickhouse/backup/ D:/soft_work/k8s-data/clickhouse-backup/
```

### 恢复数据

```bash
# 从备份恢复
kubectl cp D:/soft_work/k8s-data/clickhouse-backup/nginx_analytics_20250106.zip nginx-analytics/clickhouse-0:/var/lib/clickhouse/backup/

kubectl exec -it clickhouse-0 -n nginx-analytics -- clickhouse-client --user=analytics_user --password=analytics_password_change_in_prod -q "RESTORE DATABASE nginx_analytics FROM Disk('default', 'backup/nginx_analytics_20250106.zip')"
```
