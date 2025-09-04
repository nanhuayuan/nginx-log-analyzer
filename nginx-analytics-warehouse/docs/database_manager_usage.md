# ClickHouse 统一数据库管理工具使用指南

**版本**: v2.0  
**最后更新**: 2025-09-04  
**管理器位置**: `processors/database_manager_unified.py`

## 🎯 功能概览

统一数据库管理器提供完整的nginx分析数据库架构管理功能：
- 🏗️ **架构初始化** - 一键创建完整的ODS/DWD/ADS三层架构
- 📊 **状态监控** - 实时查看表和物化视图状态
- 🔍 **质量验证** - 验证架构完整性和数据质量
- 🔄 **维护工具** - 支持重建、清理等运维操作
- 🛠️ **精细操作** - 支持单表、单视图的精确操作

## 🚀 快速开始

### 方式一: 交互式模式（推荐）
```bash
cd nginx-analytics-warehouse/processors
python database_manager_unified.py
```

进入交互式菜单，选择需要的操作：
```
🏛️   ClickHouse 统一数据库管理工具 v2.0
================================================================================
1. 🚀 初始化完整架构（创建所有表和物化视图）
2. 📊 检查架构状态（显示表和视图状态）
3. 🔍 验证架构完整性（检查字段映射和数据质量）
4. 🔄 强制重建架构（删除数据库后重新创建）
5. 🧹 清理所有数据（保留表结构，清空数据）
6. 📋 单独执行DDL文件
7. 🔧 创建单个物化视图
0. 👋 退出
```

### 方式二: 命令行模式
```bash
# 查看状态
python database_manager_unified.py status

# 初始化架构
python database_manager_unified.py init

# 验证架构
python database_manager_unified.py validate

# 强制重建
python database_manager_unified.py rebuild

# 清理数据
python database_manager_unified.py clean
```

## 📋 详细功能说明

### 1. 🚀 初始化完整架构
**功能**: 创建完整的nginx分析数据库架构  
**包含**:
- ODS层：`ods_nginx_raw` 原始日志表
- DWD层：`dwd_nginx_enriched_v2` 清洗增强表  
- ADS层：7个业务聚合表
- 物化视图：7个实时聚合视图

**执行阶段**:
1. 📚 第一阶段：创建基础表结构
2. 🔄 第二阶段：创建物化视图
3. ✅ 第三阶段：验证架构完整性

**使用场景**: 全新环境部署、完整架构重建

### 2. 📊 检查架构状态
**功能**: 显示当前数据库架构的完整状态  
**报告内容**:
- 连接状态
- 架构健康度评级
- 基础表状态和数据量
- 物化视图状态和目标表数据量

**输出示例**:
```
📊 数据库架构状态报告 - nginx_analytics
连接状态: ✅ connected
架构健康度: ✅ healthy

📋 基础表状态:
   ✅ ods_nginx_raw: 611,500 条
   ✅ dwd_nginx_enriched_v2: 611,000 条

🔄 物化视图状态:
   ✅ mv_api_performance_hourly → ads_api_performance_analysis
      📝 01.接口性能分析 - 支持平台+入口+接口多维度分析
      📊 目标表数据: 无数据
```

### 3. 🔍 验证架构完整性
**功能**: 深度验证数据库架构的完整性和一致性  
**验证项目**:
- 表结构完整性
- 物化视图状态
- 字段映射验证
- 数据质量检查

**健康度等级**:
- `✅ healthy` - 所有表和视图正常
- `⚠️ partial` - 部分组件缺失但基本可用  
- `❌ degraded` - 关键组件缺失，需要修复

### 4. 🔄 强制重建架构
**功能**: 删除整个数据库后重新创建  
**安全确认**: 需要输入 `YES` 确认  
**使用场景**: 
- 架构严重损坏需要重建
- 升级后需要清理旧结构
- 测试环境重置

**⚠️ 警告**: 此操作将删除所有数据！

### 5. 🧹 清理所有数据
**功能**: 清空所有表数据，保留表结构  
**安全确认**: 需要输入 `YES` 确认  
**使用场景**:
- 清理测试数据
- 重新导入数据前清空
- 释放存储空间

### 6. 📋 单独执行DDL文件
**功能**: 选择性执行特定的DDL文件  
**可用文件**:
- `01_ods_layer_real.sql` - ODS层表结构
- `02_dwd_layer_real.sql` - DWD层表结构  
- `03_ads_layer_real.sql` - ADS层表结构
- `04_materialized_views_fixed.sql` - 物化视图

**使用场景**: 增量更新、单层修复

### 7. 🔧 创建单个物化视图
**功能**: 精确创建或重建特定物化视图  
**可选视图**:
- `mv_api_performance_hourly` - 接口性能分析
- `mv_service_level_hourly` - 服务层级分析
- `mv_slow_request_hourly` - 慢请求分析
- `mv_status_code_hourly` - 状态码统计
- `mv_time_dimension_hourly` - 时间维度分析  
- `mv_error_analysis_hourly` - 错误码下钻分析
- `mv_request_header_hourly` - 请求头分析

**使用场景**: 单视图修复、功能测试

## 🛠️ 环境配置

### 数据库连接配置
管理器自动使用以下连接参数：
```python
{
    'host': 'localhost',
    'port': 8123,
    'username': 'analytics_user', 
    'password': 'analytics_password',
    'database': 'nginx_analytics'
}
```

### 依赖要求
```bash
pip install clickhouse-connect
```

### 文件结构
```
nginx-analytics-warehouse/
├── processors/
│   └── database_manager_unified.py  # 统一管理器
├── ddl/                             # DDL文件目录
│   ├── 01_ods_layer_real.sql
│   ├── 02_dwd_layer_real.sql  
│   ├── 03_ads_layer_real.sql
│   └── 04_materialized_views_fixed.sql
└── docs/
    └── database_manager_usage.md    # 本文档
```

## 🔍 故障排查

### 常见问题

**1. 连接失败**
```
❌ 连接ClickHouse失败: Authentication failed
```
**解决方案**: 检查ClickHouse服务状态和认证信息

**2. 物化视图创建失败**
```
❌ 创建物化视图失败: Unknown expression identifier
```
**解决方案**: 检查DWD层字段是否完整，使用验证功能排查

**3. 权限不足**
```
❌ 操作失败: Access denied
```
**解决方案**: 确认用户有数据库管理权限

### 日志查看
管理器会生成详细日志，包括：
- 操作执行日志
- 错误详情
- 初始化报告（保存为markdown文件）

### 手动恢复
如果自动化工具失效，可以手动执行DDL文件：
```bash
# 连接到ClickHouse
clickhouse-client --user analytics_user --password analytics_password

# 手动执行SQL文件
SOURCE /path/to/01_ods_layer_real.sql;
```

## 📈 最佳实践

### 1. 定期健康检查
```bash
# 每日执行状态检查
python database_manager_unified.py status

# 每周执行架构验证
python database_manager_unified.py validate
```

### 2. 安全操作流程
- 重建前先备份数据
- 在测试环境验证操作
- 使用validate功能确认结果

### 3. 监控指标
- 架构健康度保持 `healthy`
- 物化视图成功率 > 95%
- 数据流完整性 > 99%

## 📞 技术支持

如遇到问题，请提供：
1. 操作步骤和错误信息
2. 架构状态报告输出
3. ClickHouse服务日志

---

**最后更新**: 2025-09-04  
**版本**: v2.0  
**维护**: nginx-analytics 项目组