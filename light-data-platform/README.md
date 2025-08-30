# 轻量级Nginx日志数据平台

## 项目概述

基于现有CSV数据源，构建轻量级数据平台，支持多维度交互分析和问题精准定位。

## 数据分层架构

```
ODS (操作数据存储)
├── 原始nginx日志文件 
└── 直接读取现有CSV文件

DWD (数据仓库明细层)  
├── 清洗后的标准化数据
├── 添加维度标签: platform, entry_source, app_version
└── 数据质量检查和异常标记

DWS (数据仓库汇总层)
├── 按维度预聚合的宽表
├── 平台维度: iOS_SDK, Android_SDK, Web等
├── 时间维度: 小时/天级别聚合
└── API维度: 接口级别聚合

ADS (应用数据服务层)
├── 面向业务的应用数据
├── 异常发现和智能提醒
├── 对比分析和趋势预测
└── Web界面数据API
```

## 技术架构

### 后端技术栈
- **数据存储**: SQLite (轻量级) -> 后续可升级ClickHouse
- **数据处理**: Python + Pandas (复用现有代码)
- **Web框架**: Flask (轻量级API服务)
- **任务调度**: APScheduler (定时数据更新)

### 前端技术栈
- **UI框架**: Bootstrap + jQuery (简单快速)
- **图表库**: ECharts (丰富的交互图表)
- **表格组件**: DataTables (支持搜索、排序、分页)

## 核心功能

### 1. 多维度钻取分析
- **平台维度**: iOS vs Android vs Web性能对比
- **版本维度**: 不同APP版本的性能差异
- **入口维度**: 不同来源渠道的用户行为分析
- **时间维度**: 任意时间段的性能趋势

### 2. 异常自动发现
- **性能异常**: 响应时间突然增加
- **错误率异常**: 某平台/版本错误率飙升
- **流量异常**: 请求量异常波动
- **对比异常**: 不同维度间的显著差异

### 3. 交互式查询
- **灵活筛选**: 时间范围、平台、版本、API等
- **实时对比**: 拖拽式多维度对比
- **下钻分析**: 从概览到详细的逐层钻取
- **自定义报表**: 保存常用的查询组合

## 目录结构

```
light-data-platform/
├── README.md                 # 项目说明
├── requirements.txt          # Python依赖
├── config/                   # 配置文件
│   ├── database.py          # 数据库配置
│   ├── settings.py          # 系统设置
│   └── dimensions.py        # 维度定义
├── data_pipeline/           # 数据处理管道
│   ├── __init__.py
│   ├── ods_processor.py     # ODS层处理器
│   ├── dwd_processor.py     # DWD层处理器  
│   ├── dws_processor.py     # DWS层处理器
│   ├── ads_processor.py     # ADS层处理器
│   └── scheduler.py         # 任务调度器
├── database/                # 数据库相关
│   ├── __init__.py
│   ├── models.py            # 数据模型定义
│   ├── schema.sql           # 数据库表结构
│   └── migrations/          # 数据库迁移脚本
├── web_app/                 # Web应用
│   ├── __init__.py
│   ├── app.py               # Flask主应用
│   ├── api/                 # API路由
│   │   ├── __init__.py
│   │   ├── overview.py      # 概览数据API
│   │   ├── platform.py      # 平台维度API
│   │   ├── comparison.py    # 对比分析API
│   │   └── anomaly.py       # 异常检测API
│   ├── templates/           # HTML模板
│   │   ├── base.html        # 基础模板
│   │   ├── dashboard.html   # 主面板
│   │   ├── platform.html    # 平台分析页面
│   │   └── comparison.html  # 对比分析页面
│   └── static/              # 静态资源
│       ├── css/             # 样式文件
│       ├── js/              # JavaScript文件
│       └── images/          # 图片资源
├── utils/                   # 工具模块
│   ├── __init__.py
│   ├── data_enricher.py     # 数据标签化
│   ├── anomaly_detector.py  # 异常检测算法
│   └── report_generator.py  # 报告生成器
├── tests/                   # 测试用例
│   ├── __init__.py
│   ├── test_data_pipeline.py
│   ├── test_api.py
│   └── test_utils.py
└── run.py                   # 启动脚本
```

## 开发计划

### Phase 1: 核心数据管道 (1周)
1. 搭建基础框架和数据库
2. 实现ODS->DWD->DWS数据处理流程
3. 验证与现有CSV数据源的集成

### Phase 2: Web界面开发 (1周) 
1. 实现基础的Web查询界面
2. 开发平台维度对比功能
3. 集成图表和数据展示

### Phase 3: 高级功能 (1周)
1. 异常自动检测
2. 多维度交叉分析
3. 性能优化和用户体验提升

## 快速开始

```bash
# 1. 安装依赖
cd light-data-platform
pip install -r requirements.txt

# 2. 初始化数据库
python -c "from database.models import init_db; init_db()"

# 3. 处理数据 (使用现有CSV)
python data_pipeline/ods_processor.py --csv-path ../data/demo/自研Ng2025.05.09日志-样例_分析结果_20250829_224524_temp/processed_logs.csv

# 4. 启动Web服务
python run.py

# 5. 访问 http://localhost:5000
```

## 技术特点

### 轻量级设计
- 无需复杂的大数据组件
- SQLite本地存储，快速搭建
- 单机部署，运维简单

### 可扩展性
- 模块化设计，易于功能扩展  
- 支持升级到ClickHouse/ES等
- 接口设计支持分布式架构

### 复用现有资产
- 直接使用现有CSV数据源
- 复用现有的数据处理逻辑
- 保持与现有报告的兼容性