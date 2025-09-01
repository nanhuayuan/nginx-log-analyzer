# 文件使用指南

## 🔥 必需文件（生产环境必须保留）

| 文件 | 用途 | 说明 |
|------|------|------|
| **main_simple.py** | 主启动脚本 | 统一入口，推荐使用 |
| **nginx_processor_simple.py** | 核心处理器 | nginx日志处理核心 |
| **docker-compose-simple-fixed.yml** | Docker配置 | ClickHouse等服务配置 |
| **show_data_flow.py** | 状态检查 | 系统状态和数据流检查 |

## 📚 文档文件

| 文件 | 用途 |
|------|------|
| **README.md** | 项目说明 |
| **USAGE.md** | 详细使用指南 |
| **FILES_GUIDE.md** | 本文件 |

## 🧪 演示和测试文件（可选保留）

| 文件 | 用途 | 建议 |
|------|------|------|
| **final_working_demo.py** | 完整演示 | 保留，用于演示数据流 |
| **test_complete_flow.py** | 完整测试 | 开发环境保留 |
| **simple_data_flow_demo.py** | 简化演示 | 可保留 |
| **working_demo.py** | 基础演示 | 可保留 |

## 🔧 备用文件（可删除）

| 文件 | 问题 | 建议 |
|------|------|------|
| **main.py** | emoji编码问题 | 可删除，用main_simple.py |
| **nginx_daily_processor.py** | 编码问题 | 可删除，功能已在simple版本中 |
| **nginx_log_processor.py** | 老版本 | 可删除 |
| **import_nginx_logs.py** | 功能重复 | 可删除 |

## 🚀 快速清理建议

如果只想保留核心功能，可以删除以下文件：
```bash
# 删除可选文件（保留核心功能）
rm main.py nginx_daily_processor.py nginx_log_processor.py import_nginx_logs.py

# 删除测试文件（如果不需要演示功能）  
rm *demo.py test_*.py
```

## 💡 使用建议

1. **生产环境**: 只保留必需文件 + 文档
2. **开发环境**: 保留所有文件用于测试调试
3. **演示环境**: 保留必需文件 + 演示文件

## ⚡ 快速开始

```bash
# 进入目录
cd nginx-analytics-warehouse/processors

# 启动服务
python main_simple.py start-services

# 处理日志
python main_simple.py process --date 20250901

# 查看状态
python main_simple.py status
```