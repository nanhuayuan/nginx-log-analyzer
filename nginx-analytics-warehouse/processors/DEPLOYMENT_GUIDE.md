# Nginx日志分析系统部署指南

## 1. 环境要求

### 软件要求
- Python 3.8+
- Docker & Docker Compose
- PyCharm (推荐)
- Git

### Python依赖包
```bash
pip install clickhouse-connect pandas hashlib pathlib argparse subprocess
```

## 2. 项目部署步骤

### 步骤1: 克隆代码
```bash
git clone <repository>
cd nginx-log-analyzer/nginx-analytics-warehouse/processors
```

### 步骤2: 启动ClickHouse服务
```bash
# 使用Docker Compose启动所有服务
python main_simple.py start-services
```

服务地址：
- ClickHouse: http://localhost:8123
- Grafana: http://localhost:3000 (admin/admin123)
- Superset: http://localhost:8088 (admin/admin123)

### 步骤3: 创建数据库表结构
系统会在首次运行时自动创建所需的表结构，包括：
- `ods_nginx_raw` (原始数据层)
- `dwd_nginx_enriched` (明细数据层)
- `ads_top_hot_apis` (应用数据层)

## 3. PyCharm配置

### 项目设置
1. 打开PyCharm，选择"Open"打开项目目录
2. 设置Python解释器：File -> Settings -> Project -> Python Interpreter
3. 选择系统Python或虚拟环境：`D:\soft\Anaconda3\python.exe`

### 运行配置
创建运行配置：
- **Name**: Nginx Log Processor
- **Script path**: `D:\project\nginx-log-analyzer\nginx-analytics-warehouse\processors\main_simple.py`
- **Parameters**: `process-all` (处理所有未处理日志)
- **Working directory**: `D:\project\nginx-log-analyzer\nginx-analytics-warehouse\processors`

### 调试配置
- 设置断点在关键函数：`process_all_unprocessed_logs()`
- 启用Python调试器
- 配置环境变量（如需要）

## 4. 系统入口点说明

### 主要入口文件：`main_simple.py`

#### 常用命令
```bash
# 处理所有未处理的日志（推荐默认模式）
python main_simple.py process-all

# 处理指定日期的日志
python main_simple.py process --date 20250422

# 强制重新处理指定日期
python main_simple.py process --date 20250422 --force

# 查看系统状态
python main_simple.py status

# 清空所有数据（开发环境）
python main_simple.py clear-all

# 启动/停止服务
python main_simple.py start-services
python main_simple.py stop-services
```

### 核心处理器：`nginx_processor_complete.py`
- 包含完整的ODS→DWD→ADS数据处理流程
- 实现文件hash追踪避免重复处理
- 支持底座格式nginx日志解析

## 5. 日志目录结构

### 标准目录结构
```
nginx-analytics-warehouse/
├── nginx_logs/
│   ├── 20250422/          # 日期目录 (YYYYMMDD格式)
│   │   ├── access186.log  # nginx日志文件
│   │   ├── access187.log
│   │   └── ...
│   ├── 20250423/
│   │   └── *.log
│   └── ...
└── processors/           # 处理器代码目录
```

### 日志格式说明
系统支持底座格式nginx日志：
```
http_host:"domain.com" remote_addr:"192.168.1.1" time:"2025-04-23T00:00:02+08:00" request:"GET /api/user HTTP/1.1" status:"200" response_time:"0.123"
```

## 6. 数据流程说明

### 4层数据仓库架构
1. **ODS层** (`ods_nginx_raw`): 原始日志数据
2. **DWD层** (`dwd_nginx_enriched`): 清洗后的明细数据，包含平台识别和API分类
3. **DWS层**: 聚合统计数据（可选）
4. **ADS层** (`ads_top_hot_apis`): 应用主题数据，如热门API统计

### 处理记录机制
- 文件：`processed_logs_complete.json`
- 使用文件hash避免重复处理
- 记录处理统计信息和记录数量

## 7. 验证部署

### 验证步骤
1. 确认服务启动：
```bash
python main_simple.py status
```

2. 处理样例日志：
```bash
python main_simple.py process-all
```

3. 验证数据质量：
```bash
python validate_processing.py
```

### 预期结果
- ODS表记录数 = DWD表记录数 = 日志文件行数
- 处理记录文件包含正确的统计信息
- ADS表包含聚合数据

## 8. 数据持久化说明

### 持久化存储卷
系统使用Docker volumes实现数据持久化：

```yaml
volumes:
  clickhouse_data: ClickHouse数据文件
  clickhouse_logs: ClickHouse日志文件
  grafana_data: Grafana配置和仪表板
  postgres_data: Superset元数据
  redis_data: Redis缓存数据
  superset_home: Superset配置文件
```

### 数据备份和恢复
```bash
# 列出存储卷
python manage_volumes.py list

# 备份所有数据
python manage_volumes.py backup ./data_backup

# 从备份恢复数据  
python manage_volumes.py restore ./data_backup

# 查看存储使用情况
python manage_volumes.py usage

# 清理未使用的卷
python manage_volumes.py clean
```

### 数据目录位置
- **Windows**: `C:\ProgramData\docker\volumes\`
- **Linux**: `/var/lib/docker/volumes/`

## 9. 常见问题排查

### 问题1: Docker服务未启动
```bash
# Windows
net start docker
# 或重启Docker Desktop
```

### 问题2: ClickHouse连接失败
```bash
# 检查容器状态
docker ps | grep clickhouse
# 重启服务
python main_simple.py start-services
```

### 问题3: 日志解析错误
- 检查日志格式是否符合底座格式
- 确认文件编码为UTF-8
- 查看处理记录中的错误统计

### 问题4: 数据不一致
- 清空数据重新处理：`python main_simple.py clear-all`
- 强制重新处理：`python main_simple.py process --date YYYYMMDD --force`

### 问题5: 数据丢失
- 检查Docker volumes是否正常：`python manage_volumes.py list`
- 从备份恢复：`python manage_volumes.py restore ./backup_path`

## 9. 开发调试

### 日志级别
系统输出详细的处理日志，包括：
- 文件处理进度
- 解析成功/失败统计
- 数据库插入记录数
- 处理耗时

### 调试建议
1. 使用PyCharm断点调试核心函数
2. 检查`processed_logs_complete.json`确认处理状态
3. 直接查询ClickHouse验证数据：
```sql
SELECT count() FROM ods_nginx_raw;
SELECT count() FROM dwd_nginx_enriched;
SELECT count() FROM ads_top_hot_apis;
```

## 10. 生产环境注意事项

1. **性能优化**: 系统支持大文件分块处理，内存使用优化
2. **数据备份**: 定期备份ClickHouse数据和处理记录文件
3. **监控告警**: 使用Grafana配置处理失败告警
4. **日志轮转**: 配置nginx日志轮转避免单文件过大
5. **资源监控**: 监控Docker容器资源使用情况

---

**快速开始**: 执行 `python main_simple.py process-all` 开始处理nginx日志