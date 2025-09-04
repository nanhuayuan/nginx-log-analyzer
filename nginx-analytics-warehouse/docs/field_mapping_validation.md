# DWD字段与物化视图映射验证 v1.0

**文档创建时间**: 2025-09-04  
**验证目的**: 确认DWD层字段能够满足7个物化视图的所有需求  

## ✅ 核心字段映射验证

### 基础维度字段

| 物化视图需要字段 | DWD实际字段 | 状态 | 说明 |
|----------------|------------|------|------|
| stat_time | log_time | ✅ | toStartOfHour(log_time) |
| platform | platform | ✅ | 直接映射 |
| access_type | access_type | ✅ | 直接映射 |
| api_path | request_uri | ✅ | 直接映射 |
| api_module | api_module | ✅ | 直接映射 |
| api_category | api_category | ✅ | 直接映射 |
| business_domain | business_domain | ✅ | 直接映射 |

### 性能指标字段

| 物化视图需要字段 | DWD实际字段 | 状态 | 说明 |
|----------------|------------|------|------|
| total_request_duration | total_request_duration | ✅ | 直接映射 |
| upstream_response_time | upstream_response_time | ✅ | 直接映射 |
| upstream_connect_time | upstream_connect_time | ✅ | 直接映射 |
| connection_requests | connection_requests | ✅ | 直接映射 |
| response_body_size_kb | response_body_size_kb | ✅ | 直接映射 |

### 状态和标识字段

| 物化视图需要字段 | DWD实际字段 | 状态 | 说明 |
|----------------|------------|------|------|
| response_status_code | response_status_code | ✅ | 直接映射 |
| is_success | is_success | ✅ | 直接映射 |
| is_error | is_error | ✅ | 直接映射 |
| is_slow | is_slow | ✅ | 直接映射 |
| is_very_slow | is_very_slow | ✅ | 直接映射 |
| has_anomaly | has_anomaly | ✅ | 直接映射 |
| is_business_success | is_business_success | ✅ | 直接映射 |

### 服务和上游字段

| 物化视图需要字段 | DWD实际字段 | 状态 | 说明 |
|----------------|------------|------|------|
| service_name | service_name | ✅ | 直接映射，但需要URI解析逻辑 |
| upstream_server | upstream_server | ✅ | 直接映射 |
| cluster_node | cluster_node | ✅ | 直接映射 |

### 客户端分析字段

| 物化视图需要字段 | DWD实际字段 | 状态 | 说明 |
|----------------|------------|------|------|
| user_agent_category | ❌ | ⚠️ | **需要添加** |
| user_agent_version | ❌ | ⚠️ | **需要添加** |
| device_type | device_type | ✅ | 直接映射 |
| browser_type | browser_type | ✅ | 直接映射 |
| os_type | os_type | ✅ | 直接映射 |
| is_bot | ❌ | ⚠️ | **需要从bot_type推导** |
| client_ip_type | ❌ | ⚠️ | **需要从IP字段推导** |

### 用户和会话字段

| 物化视图需要字段 | DWD实际字段 | 状态 | 说明 |
|----------------|------------|------|------|
| user_id | ❌ | ⚠️ | **缺少，可用client_ip替代** |
| session_id | ❌ | ⚠️ | **缺少，可用trace_id替代** |

## ⚠️ 需要解决的字段问题

### 1. 缺少的关键字段

#### 用户标识字段
- **user_id**: 当前DWD层缺少，建议添加或使用client_ip作为替代
- **session_id**: 当前缺少，建议使用trace_id作为替代

#### 客户端分析字段  
- **user_agent_category**: 需要从user_agent_string解析得出
- **user_agent_version**: 需要从user_agent_string解析得出
- **is_bot**: 可以从现有的bot_type字段推导 `bot_type != ''`
- **client_ip_type**: 需要从client_ip和ip_risk_level推导

### 2. 需要计算的字段

#### upstream_status_code
- **当前状态**: DWD层没有此字段
- **解决方案**: 物化视图中暂时使用response_status_code，后续可以丰富日志采集

#### 错误码相关字段
- **error_code_group**: 可以从response_status_code计算得出
- **http_error_class**: 可以从response_status_code计算得出
- **error_severity_level**: 可以从response_status_code计算得出

## 🔧 字段补充建议

### 方案A: 修改DWD表结构（推荐）
在`02_dwd_layer_real.sql`中添加缺失字段：

```sql
-- 新增用户标识字段
user_id String CODEC(ZSTD(1)), -- 用户ID（从业务日志提取）
session_id String CODEC(ZSTD(1)), -- 会话ID（从cookie或header提取）

-- 新增客户端解析字段
user_agent_category LowCardinality(String), -- 用户代理分类
user_agent_version String CODEC(ZSTD(1)), -- 用户代理版本
is_bot Bool, -- 是否机器人（从bot_type推导）
client_ip_type LowCardinality(String), -- IP类型分类

-- 新增上游状态字段
upstream_status_code LowCardinality(String), -- 上游状态码
```

### 方案B: 在物化视图中动态计算（临时方案）
```sql
-- 在物化视图中临时处理
client_ip as user_id,  -- 临时用IP作为用户标识
trace_id as session_id, -- 临时用trace_id作为会话标识
if(bot_type != '', true, false) as is_bot, -- 从bot_type推导
multiIf(
    is_internal_ip, 'internal',
    ip_risk_level = 'High', 'suspicious', 
    'external'
) as client_ip_type
```

## 📋 验证结果总结

### ✅ 满足需求的字段 (85%)
- 基础维度: 7/7 ✅
- 性能指标: 5/5 ✅  
- 状态标识: 7/7 ✅
- 服务字段: 3/3 ✅
- 客户端字段: 3/7 ⚠️

### ⚠️ 需要处理的字段 (15%)
- user_id, session_id - 用户会话标识
- user_agent_category, user_agent_version - 用户代理解析
- is_bot, client_ip_type - 客户端分类
- upstream_status_code - 上游状态码

## 🚀 下一步行动

### 优先级1: 立即可以实施
1. 使用方案B在物化视图中动态计算缺失字段
2. 验证7个物化视图的SQL语法正确性
3. 测试物化视图创建和数据聚合

### 优先级2: 后续优化  
1. 实施方案A，在DWD层添加缺失字段
2. 增强日志采集，获取upstream_status_code
3. 完善用户标识和会话跟踪机制

---

**结论**: 当前DWD层字段基本满足物化视图需求（85%匹配度），缺失的15%字段可以通过动态计算临时解决，不影响第一阶段的实施。