# Nginx日志处理器 - 模块化架构

## 架构概述

本项目实现了一个完全解耦的模块化nginx日志处理系统，采用"解析是解析，数据处理是数据处理，写入表是写入表"的设计理念。

## 核心模块

### 1. 日志解析器 (`log_parser.py`)
**功能**: 专门负责解析nginx日志的原始数据，不涉及业务逻辑
- 支持多种nginx日志格式
- 智能时间解析
- 请求字符串解析
- 数值字段转换
- 数据质量评分
- 错误处理和统计

**特性**:
- 支持文件和目录批量解析
- 内存友好的流式处理
- 编码自动检测 (UTF-8/GBK)
- 详细的解析统计信息

### 2. 数据处理器 (`data_processor.py`)
**功能**: 对解析后的原始数据进行业务逻辑处理和增强
- 平台识别 (iOS/Android/Web等)
- API分类和重要性评估
- 性能指标计算
- 用户体验分级
- 异常检测
- 业务价值评分

**处理能力**:
- 多平台User-Agent解析
- 动态API分类规则
- 响应时间阶段分析
- Apdex性能分类
- IP风险评估
- 数据质量重评估

### 3. 数据库写入器 (`database_writer.py`)
**功能**: 专门负责将处理后的数据写入ClickHouse数据库
- ODS层和DWD层数据写入
- 批量写入优化
- 错误重试机制
- 表状态监控

**特性**:
- 自动字段映射和类型转换
- 批量处理避免内存问题
- 详细的写入统计
- 数据清理功能

### 4. 主控制器 (`nginx_processor_modular.py`)
**功能**: 统一调度和管理各个模块
- 日志目录扫描
- 处理状态跟踪
- 交互式命令界面
- 性能监控

## 使用方法

### 交互式菜单 (推荐)
```bash
# 启动交互式菜单 - 最友好的方式
python nginx_processor_modular.py

# 或者使用简化版本
python main_simple.py
```

交互式菜单提供：
- 📋 直观的菜单选择
- ✅ 输入验证和确认机制
- 📊 实时状态显示
- 🔄 操作结果反馈
- ⚠️  安全的数据清理确认

### 命令行模式
```bash
# 查看系统状态
python nginx_processor_modular.py status

# 处理指定日期的日志
python nginx_processor_modular.py process --date 20250422

# 强制重新处理
python nginx_processor_modular.py process --date 20250422 --force

# 处理所有未处理的日志
python nginx_processor_modular.py process-all

# 清空所有数据 (开发环境)
python nginx_processor_modular.py clear-all
```

### 使用main_simple.py
```bash
# 交互式菜单 (推荐)
python main_simple.py

# 日常使用
python main_simple.py process-all

# 处理特定日期
python main_simple.py process --date 20250422

# 查看状态
python main_simple.py status

# 启动服务
python main_simple.py start-services
```

## 数据流程

```
原始日志文件
    ↓
[日志解析器] → 结构化数据
    ↓
[数据处理器] → 业务增强数据
    ↓
[数据库写入器] → ClickHouse (ODS + DWD层)
```

## 目录结构

```
nginx-analytics-warehouse/
├── processors/
│   ├── log_parser.py           # 日志解析器
│   ├── data_processor.py       # 数据处理器  
│   ├── database_writer.py      # 数据库写入器
│   ├── nginx_processor_modular.py  # 主控制器
│   ├── main_simple.py          # 简化入口
│   ├── requirements.txt        # 依赖管理
│   └── processed_logs_state.json  # 状态跟踪文件
├── nginx_logs/
│   └── YYYYMMDD/              # 按日期分组的日志目录
│       └── *.log              # 日志文件
└── ddl/                       # 数据库表结构
```

## 关键特性

### 1. 模块化设计
- **解耦架构**: 每个模块职责单一，可独立测试和维护
- **可插拔**: 可以轻松替换或扩展任何模块
- **标准接口**: 模块间通过标准数据格式通信

### 2. 状态管理
- **处理状态跟踪**: 记录哪些文件已处理，避免重复处理
- **文件变更检测**: 基于修改时间检测文件变化
- **处理历史**: 保留详细的处理历史记录

### 3. 错误处理
- **分层错误处理**: 每个模块都有独立的错误处理机制
- **详细错误报告**: 提供具体的错误信息和位置
- **容错设计**: 单个文件处理失败不影响整体流程

### 4. 性能优化
- **内存管理**: 分批处理大文件，控制内存使用
- **流式处理**: 避免将整个文件加载到内存
- **批量写入**: 数据库批量操作提高效率

## 数据质量

### 解析质量控制
- 字段完整性检查
- 数据类型验证
- 解析错误统计
- 质量评分机制

### 业务数据增强
- 平台识别准确率 > 95%
- API分类覆盖率 > 90%
- 性能指标计算精度
- 异常检测敏感度

## 监控和维护

### 系统状态监控
```bash
python nginx_processor_modular.py status
```
显示:
- 日志目录扫描结果
- 数据库连接状态
- 表数据量统计
- 处理状态概览

### 数据清理 (开发环境)
```bash
python nginx_processor_modular.py clear-all
```
- 清空数据库表
- 重置状态文件
- 确认机制防误操作

## 扩展指南

### 添加新的日志格式
1. 在 `log_parser.py` 中扩展 `field_patterns`
2. 更新 `parse_log_line` 方法
3. 添加对应的测试用例

### 增加业务处理规则
1. 在 `data_processor.py` 中扩展分类规则
2. 更新 `process_single_record` 方法
3. 调整数据质量评分逻辑

### 支持新的数据库
1. 继承 `database_writer.py` 基类
2. 实现特定数据库的写入逻辑
3. 更新 `main_controller` 中的数据库配置

## 依赖管理

```bash
pip install -r requirements.txt
```

主要依赖:
- `clickhouse-driver`: ClickHouse数据库驱动
- 标准库: `pathlib`, `json`, `logging`, `argparse`

## 最佳实践

### 日常使用
1. 使用 `python main_simple.py process-all` 处理日志
2. 定期运行 `python main_simple.py status` 检查状态
3. 在处理大量数据前先启动服务

### 开发调试
1. 使用单元测试验证各模块功能
2. 通过 `--force` 参数重新处理测试数据
3. 利用状态文件跟踪处理进度

### 生产部署
1. 配置适当的日志级别
2. 监控数据库连接状态
3. 定期备份状态文件

这个模块化架构为nginx日志处理提供了灵活、可扩展、易维护的解决方案。