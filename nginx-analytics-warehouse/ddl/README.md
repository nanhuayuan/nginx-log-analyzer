# ClickHouse 数据库管理工具使用说明

## 📋 概述

增强的ClickHouse数据库管理工具，支持快速建表、强制重建、状态检查等功能。

## 🚀 快速开始

### 1. 环境要求
```bash
pip install clickhouse-connect
```

### 2. 环境变量配置（可选）
```bash
export CLICKHOUSE_HOST=localhost
export CLICKHOUSE_PORT=8123
export CLICKHOUSE_USER=analytics_user
export CLICKHOUSE_PASSWORD=analytics_password
export CLICKHOUSE_DATABASE=nginx_analytics
```

### 3. 使用方式

#### 交互式模式
```bash
python database_manager.py
```

#### 命令行模式
```bash
# 快速建表
python database_manager.py quick

# 强制重建（删除数据库后重新创建）
python database_manager.py rebuild

# 检查状态
python database_manager.py status

# 验证表结构
python database_manager.py verify
```

## 📚 功能说明

### 1. 快速建表
- 自动发现所有DDL文件
- 按顺序执行建表语句
- 验证建表结果

### 2. 强制重建 ⚠️
- **危险操作**：完全删除 `nginx_analytics` 数据库
- 需要输入 'YES' 确认
- 删除后重新执行完整建表流程

### 3. 状态检查
- 显示数据库是否存在
- 列出所有表及记录数
- 对比DDL定义与实际表

### 4. 表结构验证
- 检查DDL定义的表是否都已创建
- 计算建表成功率
- 识别缺失或额外的表

### 5. 单文件执行
- 选择特定DDL文件执行
- 适用于增量更新场景

## 📄 DDL文件结构

当前支持的DDL文件：
- `01_ods_layer_real.sql` - ODS层原始数据表
- `02_dwd_layer_real.sql` - DWD层明细数据表  
- `03_ads_layer_real.sql` - ADS层聚合分析表
- `04_materialized_views.sql` - 物化视图和普通视图

## ⚡ 主要改进

### 相比原execute_ddl.py的优势：
1. **自动数据库创建** - 不需要手动创建数据库
2. **智能表发现** - 自动扫描DDL文件，无需硬编码表名
3. **更好的错误处理** - 单个语句失败不影响其他语句继续执行
4. **交互式界面** - 友好的菜单操作
5. **灵活的配置** - 支持环境变量和配置文件
6. **强制重建选项** - 支持完全重建数据库

### ClickHouse兼容性修复：
1. **移除COMMENT ON语法** - 改用CREATE TABLE中的COMMENT
2. **修正表名引用** - 物化视图引用正确的表名
3. **添加数据库前缀** - 所有表名带nginx_analytics前缀
4. **字段注释完整** - 每个字段都有中文注释说明

## 🛡️ 安全提醒

- **强制重建**会删除所有数据，请谨慎使用
- 建议在测试环境先验证DDL语句
- 生产环境建议使用**快速建表**模式

## 📊 使用示例

```bash
# 1. 首次部署
python database_manager.py quick

# 2. 开发环境重建
python database_manager.py rebuild  # 输入YES确认

# 3. 检查部署结果
python database_manager.py status

# 4. 验证表结构完整性
python database_manager.py verify
```

## 🔧 故障排除

### 连接失败
- 检查ClickHouse服务是否启动
- 确认连接参数正确
- 验证用户权限

### 建表失败  
- 查看具体错误信息
- 检查DDL语法是否正确
- 确认表依赖关系

### 权限问题
- 确保用户有CREATE DATABASE权限
- 检查用户对目标数据库的写权限