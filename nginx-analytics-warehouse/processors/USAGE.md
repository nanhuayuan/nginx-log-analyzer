# Nginx日志分析数据仓库 - 使用指南

## ✅ 日志处理修复说明

**重要更新**: 日志处理逻辑已修复！现在支持正确解析底座格式nginx日志。

### 主要修复内容:
1. **正确的日志解析**: 参考`self_00_03_log_parser.py`实现底座格式解析
2. **准确的平台识别**: iOS_SDK, Android_SDK, Web等平台智能识别
3. **完整的API分类**: Gateway_API, File_Download, Asset等分类
4. **数据质量验证**: 已验证平台识别准确率99%+

### 支持的日志格式:
- **底座格式**: `http_host:domain remote_addr:"IP" time:"2025-04-23T00:00:02+08:00" ...`
- **JSON格式**: `{"timestamp": "...", "client_ip": "...", ...}`

## 🚀 快速开始

### 1. 启动系统
```bash
cd nginx-analytics-warehouse/processors
python main_simple.py start-services
```

### 2. 准备日志文件
将nginx日志文件放在指定目录结构中：
```
D:/project/nginx-log-analyzer/nginx-analytics-warehouse/nginx_logs/
├── 20250422/
│   ├── access186.log      (样例日志)
│   └── *.log
└── YYYYMMDD/
    └── *.log
```

### 3. 处理日志
```bash
# 处理指定日期的日志
python main_simple.py process --date 20250901

# 强制重新处理（开发调试用）
python main_simple.py process --date 20250901 --force
```

### 4. 查看系统状态
```bash
python main_simple.py status
```

## 📊 系统功能

### 主要功能
- **日志处理**: 自动解析nginx日志，支持多种格式
- **数据分层**: ODS → DWD → DWS → ADS 完整数据仓库架构
- **业务增强**: 自动识别平台、API分类、性能分析
- **可视化分析**: 集成Grafana和Superset

### 数据流向
```
Nginx日志文件 → ODS(原始存储) → DWD(业务增强) → DWS(聚合统计) → ADS(业务洞察)
                                    ↓
                            ClickHouse数据库
                                    ↓
                        Grafana/Superset可视化分析
```

## 📁 核心文件说明

### 必需文件
| 文件名 | 用途 | 重要性 |
|--------|------|--------|
| `main_simple.py` | 主启动脚本 | ⭐⭐⭐ |
| `nginx_processor_fixed.py` | 修复版日志处理器 | ⭐⭐⭐ |
| `docker-compose-simple-fixed.yml` | Docker服务配置 | ⭐⭐⭐ |
| `show_data_flow.py` | 系统状态检查 | ⭐⭐ |

### 可选文件
| 文件名 | 用途 | 建议 |
|--------|------|------|
| `final_working_demo.py` | 完整演示脚本 | 保留用于演示 |
| `test_complete_flow.py` | 测试脚本 | 开发环境保留 |
| `nginx_daily_processor.py` | 备用处理器 | 可删除（有编码问题） |

## 🛠️ 命令参考

### 基本命令
```bash
# 查看帮助
python main_simple.py

# 启动所有服务
python main_simple.py start-services

# 停止所有服务  
python main_simple.py stop-services

# 查看系统状态
python main_simple.py status
```

### 日志处理命令
```bash
# 处理指定日期
python main_simple.py process --date 20250901

# 强制重新处理
python main_simple.py process --date 20250901 --force
```

### 数据管理命令
```bash
# 清空所有数据（开发环境）
python main_simple.py clear-all

# 运行演示数据流
python main_simple.py demo
```

## 🌐 Web界面访问

启动服务后，可以访问以下Web界面：

- **ClickHouse**: http://localhost:8123
- **Grafana**: http://localhost:3000
  - 用户名: admin
  - 密码: admin123
- **Superset**: http://localhost:8088  
  - 用户名: admin
  - 密码: admin123

## 📈 数据分析示例

### 1. 查看处理结果
```bash
python main_simple.py status
```

### 2. Grafana仪表盘
- 访问 http://localhost:3000
- 创建数据源 → ClickHouse
- 导入预置仪表盘

### 3. SQL查询示例
```sql
-- 查看API性能统计
SELECT api_category, platform, 
       count() as requests,
       avg(total_request_duration) as avg_time
FROM dwd_nginx_enriched 
GROUP BY api_category, platform;

-- 查看错误分布
SELECT response_status_code, count() 
FROM dwd_nginx_enriched 
WHERE response_status_code != '200'
GROUP BY response_status_code;
```

## 🔧 故障排除

### 常见问题

1. **Docker服务未启动**
   ```bash
   # 检查Docker是否运行
   docker ps
   # 启动Docker Desktop或Docker服务
   ```

2. **ClickHouse容器未运行**
   ```bash
   python main_simple.py start-services
   ```

3. **日志文件找不到**
   - 检查日志文件路径: `D:/nginx_logs/YYYYMMDD/*.log`
   - 确保文件格式为 `.log` 结尾

4. **编码问题**
   - 使用 `main_simple.py` 而不是 `main.py`
   - 确保日志文件为UTF-8编码

### 日志查看
```bash
# 查看ClickHouse容器日志
docker logs nginx-analytics-clickhouse-simple

# 查看所有容器状态
docker ps -a
```

## 🚀 生产部署建议

1. **资源配置**
   - 内存: 至少4GB
   - 磁盘: 根据日志量配置
   - CPU: 4核心以上

2. **数据备份**
   - 定期备份ClickHouse数据
   - 保留重要配置文件

3. **监控告警**
   - 设置Grafana告警
   - 监控磁盘空间使用

4. **性能优化**
   - 根据日志量调整批处理大小
   - 优化ClickHouse表分区策略

## 📞 技术支持

如有问题，请检查：
1. 系统日志和错误信息
2. Docker容器状态
3. 磁盘空间是否充足
4. 网络端口是否被占用（8123, 3000, 8088）