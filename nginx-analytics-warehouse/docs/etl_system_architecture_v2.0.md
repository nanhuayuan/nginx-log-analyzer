# ETL数据导入系统架构设计文档

**版本**: v2.0  
**创建时间**: 2025-09-04  
**最后更新**: 2025-09-04  
**状态**: 设计阶段 → 待实现

## 📋 目录
- [1. 系统概览](#1-系统概览)
- [2. 架构设计](#2-架构设计)
- [3. 核心组件](#3-核心组件)
- [4. 多解析器架构](#4-多解析器架构)
- [5. 数据流程](#5-数据流程)
- [6. 表结构设计](#6-表结构设计)
- [7. 实现计划](#7-实现计划)
- [8. 配置管理](#8-配置管理)
- [9. 使用场景](#9-使用场景)
- [10. 扩展性设计](#10-扩展性设计)

## 1. 系统概览

### 1.1 设计目标
- **多格式支持**: 支持底座、JSON、标准nginx等多种日志格式
- **高度解耦**: 解析、处理、存储组件完全分离
- **易于扩展**: 新增日志格式只需实现解析器接口
- **状态管理**: 完整的处理进度跟踪和断点续传
- **开发友好**: 支持快速清理和重新开始

### 1.2 技术栈
- **数据库**: ClickHouse 24.3.18.7
- **开发语言**: Python 3.9+
- **数据架构**: ODS/DWD/ADS 三层架构 + 物化视图
- **并发处理**: 支持多文件并行处理
- **配置管理**: YAML/JSON 配置文件

## 2. 架构设计

### 2.1 整体架构图
```
┌─────────────────────────────────────────────────────────────┐
│                ETL主控制器 (ETL Controller)                 │
├─────────────────────────────────────────────────────────────┤
│  ├─ 日志发现器 (LogDiscovery)      - 扫描YYYYMMDD目录       │
│  ├─ 状态管理器 (StateManager)      - 跟踪处理进度           │
│  ├─ 数据管道 (DataPipeline)        - 协调ETL流程           │
│  └─ 质量监控 (QualityMonitor)      - 数据质量检查           │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                解析器工厂 (Parser Factory)                  │
├─────────────────────────────────────────────────────────────┤
│  ├─ 格式检测器 (FormatDetector)    - 自动识别日志格式        │
│  ├─ 解析器注册 (ParserRegistry)    - 管理解析器类型          │
│  └─ 解析器路由 (ParserRouter)      - 路由到合适解析器        │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                数据处理层 (Processing Layer)                │
├─────────────────────────────────────────────────────────────┤
│  ├─ 数据增强器 (DataEnricher)      - 字段计算和增强        │
│  ├─ 字段映射器 (FieldMapper)       - ODS→DWD字段映射       │
│  ├─ 数据验证器 (DataValidator)     - 数据质量验证          │
│  └─ 类型转换器 (TypeConverter)     - 数据类型标准化        │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                数据存储层 (Storage Layer)                   │
├─────────────────────────────────────────────────────────────┤
│  ├─ ODS写入器 (ODSWriter)          - 写入原始数据表         │
│  ├─ DWD写入器 (DWDWriter)          - 写入清洗增强表        │
│  ├─ 批量处理器 (BatchProcessor)    - 批量高效写入          │
│  └─ 事务管理器 (TransactionMgr)    - 保证数据一致性         │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 目录结构
```
nginx-analytics-warehouse/
├── etl/                           # ETL系统主目录
│   ├── controllers/               # 主控制器
│   │   ├── nginx_etl_controller.py    # 主ETL控制器
│   │   ├── batch_processor.py         # 批处理控制器
│   │   └── data_pipeline.py           # 数据管道协调器
│   ├── parsers/                   # 解析器组件
│   │   ├── parser_factory.py          # 解析器工厂
│   │   ├── base_parser.py              # 解析器基类
│   │   ├── format_detector.py          # 格式检测器
│   │   ├── base_log_parser.py          # 底座格式解析器
│   │   ├── json_parsers/               # JSON解析器目录
│   │   │   ├── self_json_parser.py         # 自研JSON格式
│   │   │   ├── standard_json_parser.py     # 标准JSON格式
│   │   │   ├── project_a_parser.py         # 项目A专用
│   │   │   └── project_b_parser.py         # 项目B专用
│   │   └── text_parsers/               # 文本解析器目录
│   │       ├── nginx_standard_parser.py    # 标准nginx格式
│   │       └── apache_log_parser.py        # Apache格式
│   ├── processors/                # 数据处理器
│   │   ├── data_enricher.py           # 数据增强器
│   │   ├── field_mapper.py            # 字段映射器
│   │   ├── data_validator.py          # 数据验证器
│   │   └── type_converter.py          # 类型转换器
│   ├── writers/                   # 数据写入器
│   │   ├── ods_writer.py              # ODS层写入器
│   │   ├── dwd_writer.py              # DWD层写入器
│   │   ├── batch_writer.py            # 批量写入器
│   │   └── clickhouse_client.py       # ClickHouse客户端
│   ├── managers/                  # 管理器组件
│   │   ├── state_manager.py           # 状态管理器
│   │   ├── log_discovery.py           # 日志发现器
│   │   ├── quality_monitor.py         # 质量监控器
│   │   └── transaction_manager.py     # 事务管理器
│   ├── configs/                   # 配置文件
│   │   ├── field_mapping.yaml         # 字段映射配置
│   │   ├── parser_config.yaml         # 解析器配置
│   │   ├── etl_config.yaml            # ETL系统配置
│   │   └── database_config.yaml       # 数据库配置
│   ├── utils/                     # 工具函数
│   │   ├── logger.py                  # 日志工具
│   │   ├── date_utils.py              # 日期处理
│   │   └── performance_monitor.py     # 性能监控
│   └── tests/                     # 测试文件
│       ├── test_parsers.py            # 解析器测试
│       ├── test_processors.py         # 处理器测试
│       └── test_integration.py        # 集成测试
```

## 3. 核心组件

### 3.1 ETL主控制器 (NginxETLController)
**职责**: 统一调度和管理整个ETL流程
**核心功能**:
- 日志文件发现和筛选
- 处理状态管理和断点续传
- 错误处理和重试机制
- 性能监控和报告生成

### 3.2 数据管道 (DataPipeline)
**职责**: 协调数据在各层之间的流转
**核心功能**:
- 解析器 → 处理器 → 写入器的流程协调
- 批量处理优化
- 事务管理和回滚
- 数据质量检查点

### 3.3 状态管理器 (StateManager)
**职责**: 跟踪和管理处理状态
**状态文件结构**:
```json
{
  "processing_status": {
    "20250422": {
      "status": "completed|processing|failed",
      "files": {
        "access186.log": {
          "status": "completed",
          "records_processed": 102,
          "processing_time": "2.34s",
          "file_hash": "md5_hash_value",
          "processed_at": "2025-09-04T14:30:00"
        }
      },
      "total_records": 102,
      "start_time": "2025-09-04T14:29:00",
      "end_time": "2025-09-04T14:30:00"
    }
  },
  "global_stats": {
    "total_dates_processed": 1,
    "total_files_processed": 1,
    "total_records_processed": 102,
    "last_update": "2025-09-04T14:30:00"
  }
}
```

## 4. 多解析器架构

### 4.1 解析器接口设计
```python
from abc import ABC, abstractmethod
from typing import Dict, Any, Iterator, Optional

class BaseLogParser(ABC):
    """日志解析器基类"""
    
    @property
    @abstractmethod
    def parser_name(self) -> str:
        """解析器名称"""
        pass
    
    @property  
    @abstractmethod
    def supported_formats(self) -> List[str]:
        """支持的格式列表"""
        pass
    
    @abstractmethod
    def can_parse(self, sample_line: str) -> bool:
        """检查是否能解析指定格式"""
        pass
    
    @abstractmethod
    def parse_line(self, line: str, metadata: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        """解析单行日志"""
        pass
    
    @abstractmethod
    def parse_file(self, file_path: str) -> Iterator[Dict[str, Any]]:
        """解析整个日志文件"""
        pass
```

### 4.2 解析器实现层

#### 4.2.1 底座格式解析器 (BaseLogParser)
**格式特征**:
```
http_host:zgtapp.zwfw.gxzf.gov.cn remote_addr:"100.100.8.44" remote_port:"10305" ...
```
**字段提取策略**: 正则表达式匹配 `key:"value"` 和 `key:value` 模式

#### 4.2.2 JSON解析器群

##### 自研JSON格式解析器 (SelfJSONParser)
**格式特征**:
```json
{
  "timestamp": "2025-04-23T00:00:02+08:00",
  "request": {
    "method": "GET",
    "uri": "/api/user/profile",
    "protocol": "HTTP/1.1"
  },
  "response": {
    "status_code": 200,
    "body_size": 1024
  },
  "client": {
    "ip": "192.168.1.100",
    "user_agent": "Mozilla/5.0..."
  }
}
```

##### 标准JSON格式解析器 (StandardJSONParser)
**格式特征**: 标准nginx JSON格式
```json
{
  "time_local": "04/Sep/2025:14:30:15 +0800",
  "remote_addr": "192.168.1.100",
  "request": "GET /api/data HTTP/1.1",
  "status": 200,
  "body_bytes_sent": 1024
}
```

##### 项目专用解析器
- **ProjectAJSONParser**: 项目A的自定义JSON格式
- **ProjectBJSONParser**: 项目B的自定义JSON格式
- 支持项目特有字段和业务逻辑

### 4.3 格式自动检测策略
```python
class FormatDetector:
    """日志格式自动检测器"""
    
    def detect_format(self, sample_lines: List[str]) -> str:
        """
        检测日志格式
        优先级: 特定项目格式 > 标准格式 > 通用格式
        """
        detection_rules = [
            ("base_log", self._is_base_format),
            ("self_json", self._is_self_json),
            ("standard_json", self._is_standard_json),
            ("nginx_standard", self._is_nginx_standard),
            ("project_a_json", self._is_project_a_json),
            ("project_b_json", self._is_project_b_json)
        ]
        
        for format_name, detector_func in detection_rules:
            if detector_func(sample_lines):
                return format_name
        
        return "unknown"
```

## 5. 数据流程

### 5.1 完整ETL流程
```
1. 日志发现阶段
   ├─ 扫描 nginx_logs/ 目录下的 YYYYMMDD 文件夹
   ├─ 识别 *.log 文件
   ├─ 检查处理状态，跳过已处理文件
   └─ 生成处理队列

2. 解析阶段
   ├─ 格式自动检测（基于文件首几行）
   ├─ 选择合适的解析器
   ├─ 逐行解析生成标准化数据结构
   └─ 初步数据验证

3. 数据处理阶段
   ├─ 数据增强（计算衍生字段）
   ├─ 字段映射（适配目标表结构）
   ├─ 类型转换和格式标准化
   └─ 数据质量检查

4. 存储阶段
   ├─ ODS层写入（原始数据保存）
   ├─ DWD层写入（清洗后数据）
   ├─ 触发物化视图更新
   └─ 更新处理状态

5. 验证阶段
   ├─ 数据完整性检查
   ├─ 物化视图聚合验证
   ├─ 生成质量报告
   └─ 更新全局统计
```

### 5.2 批量处理策略
- **批次大小**: 500条记录/批（可配置）
- **内存限制**: 512MB最大内存使用
- **并行策略**: 支持文件级并行处理
- **错误处理**: 单条记录错误不影响整批处理

## 6. 表结构设计

### 6.1 核心表结构参考
详细表结构请参考:
- `ddl/01_ods_layer_real.sql` - ODS层表结构
- `ddl/02_dwd_layer_real.sql` - DWD层表结构（128个字段）
- `ddl/03_ads_layer_real.sql` - ADS层表结构
- `ddl/04_materialized_views_corrected.sql` - 物化视图定义

### 6.2 关键字段映射策略

#### ODS → DWD 核心字段映射
```yaml
# 基础字段映射
timestamp_fields:
  source: time
  target: log_time
  type: DateTime64(3)
  transformation: parse_iso_datetime

request_fields:
  - source: remote_addr
    target: client_ip
    type: String
  - source: code
    target: response_status_code
    type: LowCardinality(String)
  - source: body
    target: response_body_size
    type: UInt64

# 计算字段（需要增强）
derived_fields:
  - name: is_success
    formula: response_status_code < '400'
    type: Bool
  - name: is_slow
    formula: ar_time > 3.0
    type: Bool
  - name: device_type
    formula: parse_user_agent(agent).device_type
    type: LowCardinality(String)
```

## 7. 实现计划

### 7.1 Phase 1: 核心ETL组件 (Week 1)
**目标**: 完成基础ETL流程，支持底座格式
- [x] 数据库架构就绪
- [ ] 适配现有log_parser.py到新表结构
- [ ] 创建field_mapper.py处理ODS→DWD字段映射  
- [ ] 更新database_writer.py支持新表结构
- [ ] 使用102条测试数据验证端到端流程

**交付物**:
- 可运行的ETL系统基础版本
- 支持底座格式日志解析
- 完整的ODS/DWD数据流程
- 基础的状态管理功能

### 7.2 Phase 2: 多解析器架构 (Week 2)
**目标**: 实现可扩展的多解析器架构
- [ ] 实现解析器工厂和格式检测
- [ ] 创建JSON解析器群（自研、标准格式）
- [ ] 实现数据增强和质量监控
- [ ] 完善错误处理和恢复机制

**交付物**:
- 支持多种日志格式的解析器架构
- 自动格式检测功能
- 数据质量监控报告
- 完善的错误处理机制

### 7.3 Phase 3: 性能优化和运维工具 (Week 3)
**目标**: 优化性能，完善运维功能
- [ ] 实现并行处理和性能优化
- [ ] 完善状态管理和报告功能
- [ ] 实现数据清理和重置功能
- [ ] 性能测试和压力测试

**交付物**:
- 高性能ETL系统
- 完整的运维管理工具
- 性能基准测试报告
- 完整的用户文档

## 8. 配置管理

### 8.1 ETL系统配置 (etl_config.yaml)
```yaml
# ETL系统基础配置
etl:
  log_base_directory: "D:/project/nginx-log-analyzer/nginx-analytics-warehouse/nginx_logs"
  state_file: "etl_processing_state.json"
  batch_size: 500
  max_memory_mb: 512
  parallel_files: 2
  
# 数据库连接配置
database:
  host: localhost
  port: 8123
  database: nginx_analytics
  username: analytics_user
  password: analytics_password
  connection_timeout: 30
  
# 处理配置
processing:
  auto_detect_format: true
  skip_invalid_records: true
  max_error_rate: 0.05  # 5%错误率阈值
  quality_check_enabled: true
```

### 8.2 解析器配置 (parser_config.yaml)
```yaml
# 解析器注册配置
parsers:
  base_log:
    class: "parsers.base_log_parser.BaseLogParser"
    priority: 10
    description: "底座key-value格式解析器"
    
  self_json:
    class: "parsers.json_parsers.self_json_parser.SelfJSONParser"
    priority: 20
    description: "自研JSON格式解析器"
    
  standard_json:
    class: "parsers.json_parsers.standard_json_parser.StandardJSONParser"
    priority: 15
    description: "标准JSON格式解析器"

# 格式检测配置
detection:
  sample_lines: 10
  confidence_threshold: 0.8
  fallback_parser: "base_log"
```

### 8.3 字段映射配置 (field_mapping.yaml)
```yaml
# ODS到DWD的字段映射配置
field_mapping:
  # 时间字段
  timestamp:
    source_field: time
    target_field: log_time
    type: DateTime64(3)
    transformer: parse_iso_datetime
    required: true
    
  # 请求相关字段  
  client_ip:
    source_field: remote_addr
    target_field: client_ip
    type: String
    transformer: clean_ip_address
    required: true
    
  response_status:
    source_field: code
    target_field: response_status_code
    type: LowCardinality(String)
    transformer: ensure_string
    required: true

# 数据增强规则
enrichment_rules:
  is_success:
    formula: "response_status_code < '400'"
    type: Bool
    
  is_slow:
    formula: "total_request_duration > 3.0"
    type: Bool
    
  device_type:
    formula: "parse_user_agent(user_agent_string).device_type"
    type: LowCardinality(String)
```

## 9. 使用场景

### 9.1 日常数据导入
```bash
# 处理指定日期
python -m etl.controllers.nginx_etl_controller --date 20250422

# 处理日期范围
python -m etl.controllers.nginx_etl_controller --start-date 20250420 --end-date 20250422

# 自动发现并处理所有新增日志
python -m etl.controllers.nginx_etl_controller --auto-discover

# 强制重新处理（忽略已处理状态）
python -m etl.controllers.nginx_etl_controller --date 20250422 --force
```

### 9.2 开发调试场景
```bash
# 仅处理少量数据进行测试
python -m etl.controllers.nginx_etl_controller --date 20250422 --limit 100

# 清除指定日期的数据重新处理
python -m etl.controllers.nginx_etl_controller --date 20250422 --clean

# 清除所有数据重新开始
python -m etl.controllers.nginx_etl_controller --clean-all

# 调试模式（详细日志输出）
python -m etl.controllers.nginx_etl_controller --date 20250422 --debug
```

### 9.3 质量监控场景
```bash
# 数据质量检查
python -m etl.controllers.nginx_etl_controller --validate --date 20250422

# 生成处理报告
python -m etl.controllers.nginx_etl_controller --report --date 20250422

# 检查物化视图状态
python -m etl.controllers.nginx_etl_controller --check-views

# 性能基准测试
python -m etl.controllers.nginx_etl_controller --benchmark --date 20250422
```

### 9.4 管理维护场景
```bash
# 查看处理状态
python -m etl.controllers.nginx_etl_controller --status

# 重置处理状态
python -m etl.controllers.nginx_etl_controller --reset-state

# 修复不一致的数据
python -m etl.controllers.nginx_etl_controller --repair --date 20250422
```

## 10. 扩展性设计

### 10.1 新增解析器流程
1. **实现解析器类**:
   ```python
   class NewFormatParser(BaseLogParser):
       @property
       def parser_name(self) -> str:
           return "new_format"
           
       def can_parse(self, sample_line: str) -> bool:
           # 实现格式检测逻辑
           pass
           
       def parse_line(self, line: str) -> Dict[str, Any]:
           # 实现解析逻辑
           pass
   ```

2. **注册解析器**:
   在 `parser_config.yaml` 中添加配置

3. **测试验证**:
   编写单元测试确保解析正确性

### 10.2 新增数据源类型
支持扩展到其他类型的日志：
- **应用日志**: Spring Boot, Django等应用框架日志
- **系统日志**: Syslog, Windows Event Log等
- **中间件日志**: Redis, MySQL, Kafka等中间件日志
- **云服务日志**: AWS CloudTrail, 阿里云SLS等

### 10.3 输出格式扩展
支持多种数据输出目标：
- **其他数据库**: MySQL, PostgreSQL, ElasticSearch
- **消息队列**: Kafka, RabbitMQ, RocketMQ  
- **文件格式**: Parquet, CSV, JSON Lines
- **对象存储**: S3, OSS, COS

## 11. 风险评估与应对

### 11.1 技术风险
| 风险项 | 影响级别 | 概率 | 应对策略 |
|--------|----------|------|----------|
| 解析器性能瓶颈 | 中 | 中 | 实现并行解析，优化正则表达式 |
| 内存溢出 | 高 | 低 | 批量处理，内存监控和限制 |
| 数据一致性问题 | 高 | 低 | 事务管理，回滚机制 |
| ClickHouse连接不稳定 | 中 | 中 | 连接池，重试机制 |

### 11.2 业务风险  
| 风险项 | 影响级别 | 概率 | 应对策略 |
|--------|----------|------|----------|
| 日志格式变化 | 中 | 中 | 版本化解析器，向后兼容 |
| 数据量突增 | 中 | 低 | 自动扩容，分片处理 |
| 数据质量下降 | 高 | 低 | 质量监控，告警机制 |

## 12. 成功指标

### 12.1 性能指标
- **处理速度**: ≥ 10,000 条记录/秒
- **内存使用**: ≤ 512MB
- **错误率**: ≤ 0.1%
- **可用性**: ≥ 99.9%

### 12.2 功能指标
- **格式支持**: 支持5种以上日志格式
- **扩展性**: 新增解析器 ≤ 1天
- **易用性**: 单命令完成日常处理任务
- **可维护性**: 完整的监控和诊断功能

---

**文档状态**: 📝 设计完成，待实现  
**下一步**: 开始Phase 1实现，创建基础ETL组件  
**负责人**: ETL开发团队  
**评审状态**: 待评审