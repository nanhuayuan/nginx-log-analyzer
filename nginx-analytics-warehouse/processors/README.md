# Nginx日志分析数据仓库 - 处理器目录

这是nginx日志分析系统的核心处理目录。

## 🚀 快速开始

```bash
# 启动服务
python main.py start-services

# 处理今天的日志
python main.py process --date 20250901

# 查看系统状态
python main.py status
```

## 📁 目录文件说明

### 🔧 核心文件 (必需)
- **`main.py`** - 主启动脚本，统一入口
- **`nginx_processor_simple.py`** - 核心nginx日志处理器
- **`docker-compose-simple-fixed.yml`** - Docker服务配置
- **`show_data_flow.py`** - 系统状态检查
- **`final_working_demo.py`** - 完整数据流演示

### 🧪 开发测试文件 (可选)
- `test_complete_flow.py` - 完整流程测试
- `simple_data_flow_demo.py` - 简化数据流演示  
- `working_demo.py` - 工作演示

### 📊 其他处理器 (备用)
- `nginx_daily_processor.py` - 日志处理器(有编码问题)
- `nginx_log_processor.py` - 备用处理器
- `import_nginx_logs.py` - 日志导入工具

### 🗑️ 可删除文件
- `parse_sample_logs.py` - 样本日志解析(功能已整合)
- `verify_and_generate_ads.py` - ADS验证(功能已整合)
- `processed_logs.json` - 旧的处理记录文件

## 💡 使用建议

1. **生产环境**: 只需要保留核心文件即可
2. **开发环境**: 可以保留测试文件用于调试
3. **日志目录**: 确保日志放在 `D:/nginx_logs/YYYYMMDD/*.log`

## 🛠️ 系统架构

```
Nginx日志文件 → ODS(原始) → DWD(增强) → DWS(聚合) → ADS(洞察)
                     ↓
              ClickHouse数据库
                     ↓
            Grafana/Superset可视化
```