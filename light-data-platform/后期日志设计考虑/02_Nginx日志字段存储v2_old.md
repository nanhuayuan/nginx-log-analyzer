

## 字段分类策略

### 🔥 核心存储字段（必须原样保存）

| 字段名 | 中文名称 | 存储原因 | 查询频率 | 索引策略 |
|--------|----------|----------|----------|----------|
| `timestamp` | 时间戳 | 时序分析核心 | 极高 | 主分区键 |
| `cluster` | 集群标识 | 多集群对比分析 | 极高 | 分区键 |
| `service` | 服务名 | 服务级监控 | 极高 | 分区键 |
| `node_id` | 节点ID | 节点级故障定位 | 高 | 索引 |
| `client_ip` | 客户端IP | 安全分析、用户行为 | 高 | 索引 |
| `method` | HTTP方法 | 接口分类分析 | 极高 | 索引 |
| `uri` | 请求URI | 接口性能分析核心 | 极高 | 索引 |
| `status` | 状态码 | 错误率监控核心 | 极高 | 索引 |
| `request_time` | 请求总时长 | 性能监控核心 | 极高 | - |
| `upstream_response_time` | 后端响应时长 | 后端性能监控 | 极高 | - |
| `upstream_connect_time` | 后端连接时长 | 连接性能监控 | 高 | - |
| `upstream_header_time` | 后端处理时长 | 业务逻辑性能 | 高 | - |
| `body_bytes` | 响应体大小 | 流量分析 | 中 | - |
| `upstream_addr` | 后端服务器 | 负载均衡分析 | 中 | 索引 |
| `upstream_status` | 后端状态码 | 后端健康监控 | 高 | 索引 |
| `user_agent` | 用户代理 | 设备分析 | 低 | - |
| `trace_id` | 链路追踪ID | 分布式追踪 | 中 | 索引 |

### ⚡ 预计算字段（写入时计算并存储）

| 字段名 | 中文名称 | 计算公式 | 业务价值 | 存储必要性 |
|--------|----------|----------|----------|------------|
| `response_time_ms` | 响应时间(毫秒) | `request_time * 1000` | 性能基准统一 | 高 |
| `backend_process_time` | 后端纯处理时间 | `upstream_header_time - upstream_connect_time` | 业务逻辑性能评估 | 高 |
| `backend_transfer_time` | 后端传输时间 | `upstream_response_time - upstream_header_time` | 数据传输性能 | 高 |
| `nginx_proxy_time` | Nginx代理时间 | `request_time - upstream_response_time` | 代理性能评估 | 中 |
| `response_size_kb` | 响应大小(KB) | `body_bytes / 1024` | 人性化展示 | 中 |
| `is_slow_request` | 慢请求标记 | `request_time > 0.8 ? 1 : 0` | 快速过滤慢请求 | 高 |
| `is_error` | 错误请求标记 | `status >= 400 ? 1 : 0` | 快速统计错误率 | 高 |
| `is_server_error` | 服务器错误标记 | `status >= 500 ? 1 : 0` | 服务器问题监控 | 高 |
| `time_bucket_5min` | 5分钟时间桶 | `toStartOfFiveMinutes(timestamp)` | 时序聚合优化 | 高 |
| `time_bucket_1hour` | 1小时时间桶 | `toStartOfHour(timestamp)` | 趋势分析优化 | 中 |
| `uri_normalized` | 标准化URI | 去除动态参数的URI | 接口聚合分析 | 高 |
| `backend_efficiency` | 后端处理效率 | `backend_process_time / upstream_response_time` | 后端性能评分 | 中 |

### 📊 实时计算字段（查询时计算）

| 字段名 | 中文名称 | 计算场景 | 实时计算原因 |
|--------|----------|----------|--------------|
| `qps` | 每秒请求数 | 实时监控 | 时间窗口动态变化 |
| `avg_response_time` | 平均响应时间 | 性能报表 | 聚合范围动态变化 |
| `p95_response_time` | 95分位响应时间 | 性能分析 | 分位数计算资源消耗大 |
| `error_rate` | 错误率 | 质量监控 | 基于is_error字段实时计算 |
| `top_slow_apis` | 最慢接口TOP5 | 性能优化 | 排名动态变化 |
| `traffic_distribution` | 流量分布 | 容量规划 | 基于预存储字段聚合 |
| `backend_health_score` | 后端健康评分 | 服务治理 | 综合多指标实时评估 |

### 🗑️ 可选/简化字段

| 字段名 | 简化策略 | 原因 |
|--------|----------|------|
| `connection_id` | 不存储 | 调试价值有限，存储成本高 |
| `connection_requests` | 不存储 | 连接复用分析频率低 |
| `content_length` | 不存储 | body_bytes已足够 |
| `gzip_ratio` | 抽样存储(1%) | 压缩效果分析不需要全量 |
| `cookie` | 不存储 | 隐私风险高，分析价值低 |
| `referer` | 哈希存储 | 保护隐私，支持来源分析 |
| `business_sign` | 条件存储 | 仅self集群存储 |
| `business_timestamp` | 条件存储 | 仅self集群存储 |
| `business_version` | 条件存储 | 仅self集群存储 |
| `business_app_key` | 条件存储 | 仅self集群存储 |

### 
