# ETL自动化处理系统部署使用说明

## 概述

本文档说明如何配置和使用nginx日志ETL自动化处理系统，实现每天凌晨1:30自动启动数据清洗任务。

## 问题解决

### 已修复的问题
1. ✅ **自动文件发现功能缺陷** - 修复了方法调用错误
2. ✅ **交互式界面限制** - 新增非交互式自动监控模式
3. ✅ **监控逻辑问题** - 优化了文件处理和错误统计逻辑
4. ✅ **任务调度配置** - 创建专门的ETL调度脚本

### 系统架构

```
├── controllers/
│   ├── integrated_ultra_etl_controller.py    # 主ETL控制器（已优化，支持相对路径）
│   ├── integrated_ultra_etl_controller-v1.py # 备用版本1
│   └── integrated_ultra_etl_controller-v2.py # 备用版本2
├── run_auto_etl.bat                          # 自动运行脚本（原版）
├── run_auto_etl_portable.bat                 # 便携版自动运行脚本（推荐）
├── setup_etl_scheduler.ps1                   # PowerShell调度配置（智能检测脚本）
├── setup_etl_scheduler_admin.bat             # 管理员权限启动器
├── test_auto_etl.bat                         # 功能测试脚本
└── logs/                                     # 日志文件目录
```

### 主要优化特性

- ✅ **消除硬编码路径**：所有脚本使用 `%~dp0` 获取脚本所在目录
- ✅ **智能路径检测**：ETL控制器自动检测 `nginx_logs` 目录位置
- ✅ **便携性增强**：脚本可在任意位置运行，无需修改路径
- ✅ **环境兼容性**：自动检测Python环境和必需文件
- ✅ **实时进度显示**：使用PowerShell Tee-Object实现控制台+文件双重输出
- ✅ **智能错误处理**：详细的退出代码检查和错误提示
- ✅ **统一用户体验**：参考zip目录脚本风格，提供一致的交互体验

## 部署步骤

### 1. 测试系统功能

首先运行测试脚本验证系统是否正常工作：

```bash
# 切换到ETL目录
cd D:\project\nginx-log-analyzer\nginx-analytics-warehouse\etl

# 运行测试脚本
test_auto_etl.bat
```

测试包括：
- 基本处理功能测试（处理未处理的日志，限制100条记录）
- 自动监控功能测试（运行60秒）

### 2. 配置定时任务

运行管理员权限配置脚本：

```bash
# 方式1：双击运行（推荐）
setup_etl_scheduler_admin.bat

# 方式2：手动以管理员身份运行PowerShell
# 右键点击PowerShell -> 以管理员身份运行
# 然后执行：setup_etl_scheduler.ps1
```

配置完成后会创建名为 `NginxETLAutoProcessor` 的定时任务。

### 3. 验证任务配置

1. 按 `Win+R`，输入 `taskschd.msc` 打开任务计划程序
2. 在任务计划程序库中找到 `NginxETLAutoProcessor`
3. 查看任务属性：
   - **触发器**：每天凌晨1:30
   - **操作**：运行 `run_auto_etl.bat`
   - **设置**：最长运行3小时，失败重试3次

## 运行机制

### 自动化流程

1. **定时启动**：每天凌晨1:30自动启动
2. **初始处理**：首先处理所有未处理的日志文件
3. **监控模式**：进入2小时自动监控模式
4. **增量处理**：每3分钟检查新文件并自动处理
5. **自动结束**：2小时后自动停止

### 处理逻辑

```python
# 运行命令示例
python controllers\integrated_ultra_etl_controller.py \
    --auto-monitor \
    --monitor-duration 7200 \
    --batch-size 3000 \
    --workers 6 \
    --refresh-minutes 2
```

参数说明：
- `--auto-monitor`：启用非交互式自动监控模式
- `--monitor-duration 7200`：监控持续时间2小时
- `--batch-size 3000`：批处理大小3000条记录
- `--workers 6`：使用6个工作线程
- `--refresh-minutes 2`：每2分钟显示进度

### 日志系统

**双重输出机制**：
- 🖥️ **控制台实时显示**：所有进度信息实时显示在黑窗口中
- 📝 **文件完整记录**：同时保存到 `logs/` 目录下的日志文件
- 📁 **文件命名格式**：`etl_auto_YYYYMMDD_HHMM.log`

**实时监控优势**：
- ✅ 可以看到实时处理进度和状态
- ✅ 可以及时发现和中断异常情况
- ✅ 完整的历史日志便于问题排查
- ✅ 退出代码检查确保处理成功

## 手动操作

### 立即运行任务

```bash
# 方式1：通过任务计划程序
# 打开任务计划程序 -> 找到任务 -> 右键 -> 运行

# 方式2：直接运行脚本（推荐便携版）
run_auto_etl_portable.bat

# 方式2b：运行原版脚本
run_auto_etl.bat

# 方式3：命令行运行
python controllers\integrated_ultra_etl_controller.py --auto-monitor --monitor-duration 3600
```

### 交互式模式

如需手动控制处理过程：

```bash
# 启动交互式界面
python controllers\integrated_ultra_etl_controller.py

# 或者处理特定日期
python controllers\integrated_ultra_etl_controller.py --date 20250922

# 或者处理所有文件
python controllers\integrated_ultra_etl_controller.py --all
```

## 监控和维护

### 1. 检查任务运行状态

- 打开任务计划程序查看执行历史
- 检查 `logs/` 目录下的日志文件
- 观察数据库中的数据更新情况

### 2. 性能调优

根据系统性能调整参数：

```bash
# 高性能配置（8核CPU，16GB内存）
--batch-size 5000 --workers 8

# 中等配置（4核CPU，8GB内存）
--batch-size 3000 --workers 6

# 低配置（双核CPU，4GB内存）
--batch-size 2000 --workers 4
```

### 3. 故障排除

常见问题和解决方案：

| 问题 | 可能原因 | 解决方案 |
|------|----------|----------|
| 任务不执行 | 权限问题 | 检查任务运行用户权限 |
| 处理失败 | 数据库连接问题 | 检查ClickHouse服务状态 |
| 内存不足 | 批处理大小过大 | 减小batch-size参数 |
| 处理缓慢 | 线程数不足 | 增加workers参数 |

### 4. 维护操作

```bash
# 查看处理状态
python controllers\integrated_ultra_etl_controller.py
# 选择菜单项 7: 查看处理状态

# 清理日志文件（保留最近7天）
forfiles /p logs /s /m *.log /d -7 /c "cmd /c del @path"

# 备份状态文件
copy processed_logs_state.json processed_logs_state.json.backup
```

## 与现有下载系统的集成

系统已配置为与automa下载系统协同工作：

1. **00:30** - automa开始下载日志
2. **01:00** - 日志移动到nginx_logs目录
3. **01:30** - ETL系统自动启动处理
4. **03:30** - ETL处理完成，系统进入待机状态

这确保了每天的日志都能及时处理，第二天早上就能看到前一天的分析数据。

## 技术特性

1. **高性能处理**：支持多线程并行处理，优化的批量写入
2. **内存管理**：智能缓存管理，防止内存泄漏
3. **故障恢复**：自动重试机制，断点续传功能
4. **监控友好**：详细的日志记录和进度跟踪
5. **配置灵活**：支持多种运行模式和参数调优

该系统已经完全解决了原有的交互式限制和处理问题，能够稳定可靠地实现自动化日志处理。