-- ==========================================
-- ADS增强层 v3.0 - 全维度分析主题表
-- 基于工作介绍需求：支持平台入口下钻、错误链路分析、权限隔离
-- 新增：租户权限、业务流程、安全监控、用户行为等15个主题
-- ==========================================

-- 1. 对应 01.接口性能分析.xlsx - 增强版支持全维度下钻
CREATE TABLE IF NOT EXISTS nginx_analytics.ads_api_performance_analysis_v3 (
    stat_time DateTime,                              -- 统计时间
    time_granularity LowCardinality(String),         -- minute/hour/day/week/month
    
    -- 权限维度(核心)
    tenant_code LowCardinality(String),              -- 租户代码
    team_code LowCardinality(String),                -- 团队代码
    environment LowCardinality(String),              -- 环境标识
    
    -- 平台入口维度(核心下钻维度)
    platform LowCardinality(String),                 -- 平台类型
    platform_category LowCardinality(String),        -- 平台分类
    access_type LowCardinality(String),              -- 接入方式
    access_entry_point LowCardinality(String),       -- 访问入口
    client_channel LowCardinality(String),           -- 客户端渠道
    client_type LowCardinality(String),              -- 客户端类型
    
    -- API业务维度(核心下钻维度)
    api_path String,                                 -- 接口路径
    api_category LowCardinality(String),             -- 接口分类
    api_subcategory LowCardinality(String),          -- 接口子分类
    api_module LowCardinality(String),               -- 功能模块
    api_version LowCardinality(String),              -- API版本
    business_domain LowCardinality(String),          -- 业务域
    business_subdomain LowCardinality(String),       -- 业务子域
    business_operation_type LowCardinality(String),  -- 业务操作类型
    
    -- 请求量指标(基础)
    total_requests UInt64,                           -- 总请求数
    unique_clients UInt64,                           -- 独特客户端数
    unique_users UInt64,                             -- 独特用户数
    unique_sessions UInt64,                          -- 独特会话数
    qps Float64,                                     -- 每秒请求数
    peak_qps Float64,                                -- 峰值每秒请求数
    concurrent_users Float64,                        -- 并发用户数
    
    -- 性能指标详细分解
    avg_response_time Float64,                       -- 平均响应时间
    median_response_time Float64,                    -- 中位数响应时间
    p50_response_time Float64,                       -- 50%分位数
    p90_response_time Float64,                       -- 90%分位数
    p95_response_time Float64,                       -- 95%分位数
    p99_response_time Float64,                       -- 99%分位数
    p999_response_time Float64,                      -- 99.9%分位数
    max_response_time Float64,                       -- 最大响应时间
    min_response_time Float64,                       -- 最小响应时间
    response_time_std Float64,                       -- 响应时间标准差
    
    -- 成功率和错误分析
    success_requests UInt64,                         -- 成功请求数
    error_requests UInt64,                           -- 错误请求数
    timeout_requests UInt64,                         -- 超时请求数
    success_rate Float64,                            -- 成功率
    error_rate Float64,                              -- 错误率
    timeout_rate Float64,                            -- 超时率
    business_success_rate Float64,                   -- 业务成功率
    availability Float64,                            -- 可用性
    reliability Float64,                             -- 可靠性
    
    -- 慢请求分析(多级别)
    slow_requests UInt64,                            -- 慢请求(>3s)
    very_slow_requests UInt64,                       -- 超慢请求(>10s)
    extremely_slow_requests UInt64,                  -- 极慢请求(>30s)
    slow_rate Float64,                               -- 慢请求率
    very_slow_rate Float64,                          -- 超慢请求率
    extremely_slow_rate Float64,                     -- 极慢请求率
    
    -- 用户体验指标
    apdex_score Float64,                             -- Apdex指数(0-1)
    user_satisfaction_score Float64,                 -- 用户满意度(0-100)
    performance_score Float64,                       -- 性能评分(0-100)
    user_experience_level LowCardinality(String),    -- 体验等级(Excellent/Good/Fair/Poor)
    
    -- 业务价值和重要性
    business_value_score Float64,                    -- 业务价值评分
    importance_level LowCardinality(String),         -- 重要性等级
    business_criticality LowCardinality(String),     -- 业务关键性
    revenue_impact Float64,                          -- 收入影响评估
    customer_impact_score Float64,                   -- 客户影响评分
    
    -- 性能趋势分析
    performance_trend Float64,                       -- 性能趋势系数
    qps_trend Float64,                               -- QPS趋势系数
    error_trend Float64,                             -- 错误趋势系数
    vs_previous_period Float64,                      -- 环比变化
    vs_same_period_last_week Float64,                -- 周同比变化
    vs_same_period_last_month Float64,               -- 月同比变化
    
    -- 容量和资源分析
    capacity_utilization Float64,                    -- 容量利用率
    resource_consumption Float64,                    -- 资源消耗
    cost_per_request Float64,                        -- 单个请求成本
    
    -- 元数据
    created_at DateTime DEFAULT now(),               -- 创建时间
    updated_at DateTime DEFAULT now(),               -- 更新时间
    data_quality_score Float64,                      -- 数据质量评分
    sample_size UInt64,                              -- 样本大小
    confidence_level Float64                         -- 置信度
    
) ENGINE = SummingMergeTree()
PARTITION BY (toYYYYMM(stat_time), tenant_code)      -- 按月份+租户分区
ORDER BY (stat_time, tenant_code, platform, access_entry_point, api_category, api_path)
TTL stat_time + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;

-- 2. 对应 02.服务层级分析.xlsx - 增强版支持微服务架构
CREATE TABLE IF NOT EXISTS nginx_analytics.ads_service_level_analysis_v3 (
    stat_time DateTime,
    time_granularity LowCardinality(String),
    
    -- 权限维度
    tenant_code LowCardinality(String),
    team_code LowCardinality(String),
    environment LowCardinality(String),
    
    -- 服务架构维度
    platform LowCardinality(String),
    service_name LowCardinality(String),             -- 服务名称
    service_version String,                          -- 服务版本
    microservice_name LowCardinality(String),        -- 微服务名称
    service_tier LowCardinality(String),             -- 服务层级
    service_mesh_name LowCardinality(String),        -- 服务网格
    cluster_node LowCardinality(String),             -- 集群节点
    datacenter LowCardinality(String),               -- 数据中心
    availability_zone LowCardinality(String),        -- 可用区
    upstream_server String,                          -- 上游服务器
    downstream_service LowCardinality(String),       -- 下游服务
    
    -- 服务健康指标
    total_requests UInt64,
    success_requests UInt64,
    error_requests UInt64,
    timeout_requests UInt64,
    
    -- 服务性能
    avg_response_time Float64,
    p95_response_time Float64,
    avg_upstream_time Float64,
    avg_connect_time Float64,
    
    -- 服务质量
    availability Float64,                            -- 可用性
    reliability Float64,                             -- 可靠性
    health_score Float64,                            -- 服务健康度
    
    -- 容量指标
    max_concurrent_requests UInt32,
    avg_qps Float64,
    peak_qps Float64,
    
    created_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(stat_time)
ORDER BY (stat_time, platform, service_name, cluster_node)
TTL stat_time + INTERVAL 2 YEAR;

-- 3. 对应 03_慢请求分析.xlsx
CREATE TABLE IF NOT EXISTS nginx_analytics.ads_slow_request_analysis (
    stat_time DateTime,                              -- 统计时间
    time_granularity LowCardinality(String),         -- hour/day/week
    platform LowCardinality(String),                 -- 平台维度
    access_type LowCardinality(String),              -- 接入方式
    api_path String,                                 -- 接口路径
    api_module LowCardinality(String),               -- 功能模块
    api_category LowCardinality(String),             -- 接口分类
    business_domain LowCardinality(String),          -- 业务域
    
    -- 慢请求统计
    total_requests UInt64,
    slow_requests UInt64,                            -- >3s
    very_slow_requests UInt64,                       -- >10s
    extremely_slow_requests UInt64,                  -- >30s
    
    -- 慢请求比率
    slow_rate Float64,
    very_slow_rate Float64,
    extremely_slow_rate Float64,
    
    -- 慢请求响应时间分析
    slow_avg_time Float64,
    slow_p95_time Float64,
    slow_max_time Float64,
    
    -- 慢请求原因分析
    upstream_slow_count UInt64,                      -- 上游慢
    network_slow_count UInt64,                       -- 网络慢
    processing_slow_count UInt64,                    -- 处理慢
    
    -- 影响分析
    affected_users UInt64,
    business_impact_score Float64,
    
    created_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree() 
PARTITION BY toYYYYMM(stat_time)
ORDER BY (stat_time, platform, slow_rate, api_path)
TTL stat_time + INTERVAL 1 YEAR;

-- 4. 对应 04.状态码统计.xlsx
CREATE TABLE IF NOT EXISTS nginx_analytics.ads_status_code_analysis (
    stat_time DateTime,                              -- 统计时间
    time_granularity LowCardinality(String),         -- hour/day/week
    platform LowCardinality(String),                 -- 平台维度
    access_type LowCardinality(String),              -- 接入方式
    api_path String,                                 -- 接口路径
    api_module LowCardinality(String),               -- 功能模块
    api_category LowCardinality(String),             -- 接口分类
    business_domain LowCardinality(String),          -- 业务域
    status_code LowCardinality(String),              -- 具体状态码
    status_class LowCardinality(String),             -- 2xx/3xx/4xx/5xx
    
    -- 状态码统计
    request_count UInt64,
    percentage Float64,                              -- 占比
    
    -- 错误详情（针对4xx/5xx）
    error_type LowCardinality(String),               -- Client_Error/Server_Error
    common_error_apis Array(String),                 -- 高频错误接口
    
    -- 趋势分析
    vs_previous_period Float64,                      -- 环比变化
    is_anomaly Bool,                                 -- 是否异常
    
    created_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(stat_time)
ORDER BY (stat_time, platform, status_code, request_count)
TTL stat_time + INTERVAL 1 YEAR;

-- 5. 对应 05.时间维度分析-全部接口.xlsx 和 05_01指定接口
CREATE TABLE IF NOT EXISTS nginx_analytics.ads_time_dimension_analysis (
    stat_time DateTime,
    time_granularity LowCardinality(String),         -- minute/hour/day
    platform LowCardinality(String),
    api_path String,                                 -- 空表示全部接口
    api_category LowCardinality(String),
    
    -- 时间特征
    time_period LowCardinality(String),              -- Dawn/Morning/Noon/Afternoon/Evening/Night
    weekday UInt8,
    is_weekend Bool,
    is_holiday Bool,
    
    -- 流量指标
    total_requests UInt64,
    qps Float64,
    unique_users UInt64,
    
    -- 性能指标
    avg_response_time Float64,
    p95_response_time Float64,
    success_rate Float64,
    
    -- 趋势分析
    traffic_trend Float64,                           -- 流量趋势系数
    performance_trend Float64,                       -- 性能趋势系数
    peak_detection Bool,                             -- 是否流量峰值
    
    -- 对比分析
    vs_same_time_yesterday Float64,
    vs_same_time_last_week Float64,
    vs_avg_same_period Float64,
    
    created_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(stat_time)
ORDER BY (stat_time, platform, api_category, api_path)
TTL stat_time + INTERVAL 2 YEAR;

-- 6. 对应 06_服务稳定性.xlsx
CREATE TABLE IF NOT EXISTS nginx_analytics.ads_service_stability_analysis (
    stat_time DateTime,
    time_granularity LowCardinality(String),
    platform LowCardinality(String),
    service_name LowCardinality(String),
    api_category LowCardinality(String),
    
    -- 稳定性核心指标
    availability Float64,                            -- 可用性(SLA)
    reliability Float64,                             -- 可靠性
    mttr_minutes Float64,                            -- 平均恢复时间
    mttf_hours Float64,                              -- 平均故障间隔时间
    
    -- 错误统计
    total_errors UInt64,
    error_incidents UInt32,                          -- 故障次数
    critical_errors UInt64,                          -- 严重错误
    
    -- 性能稳定性
    response_time_std Float64,                       -- 响应时间标准差
    performance_variance Float64,                    -- 性能方差
    outlier_ratio Float64,                           -- 异常值比例
    
    -- 容量稳定性
    max_concurrent UInt32,
    capacity_utilization Float64,                    -- 容量利用率
    is_capacity_saturated Bool,                      -- 是否接近饱和
    
    -- 稳定性评分
    stability_score Float64,                         -- 综合稳定性评分 0-100
    stability_grade LowCardinality(String),          -- A/B/C/D/F
    
    created_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(stat_time)
ORDER BY (stat_time, platform, stability_score, service_name)
TTL stat_time + INTERVAL 2 YEAR;

-- 7. 对应 08_IP来源分析.xlsx
CREATE TABLE IF NOT EXISTS nginx_analytics.ads_ip_source_analysis (
    stat_time DateTime,
    time_granularity LowCardinality(String),
    platform LowCardinality(String),
    client_ip String,
    ip_classification LowCardinality(String),        -- Internal/External/CDN/Suspicious
    
    -- 地理分布
    country LowCardinality(String),
    province LowCardinality(String), 
    city LowCardinality(String),
    isp LowCardinality(String),
    
    -- 行为指标
    total_requests UInt64,
    unique_apis UInt64,
    session_duration_avg Float64,
    request_frequency Float64,                       -- 请求频率
    
    -- 性能表现
    avg_response_time Float64,
    error_rate Float64,
    success_rate Float64,
    
    -- 风险评估
    risk_score UInt8,                                -- 0-100风险评分
    risk_level LowCardinality(String),               -- Low/Medium/High/Critical
    is_bot Bool,
    bot_probability Float64,
    suspicious_patterns Array(String),               -- 可疑行为模式
    
    -- 安全分析
    attack_attempts UInt32,
    blocked_requests UInt32,
    malicious_score Float64,
    
    created_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(stat_time)  
ORDER BY (stat_time, platform, risk_score, client_ip)
TTL stat_time + INTERVAL 6 MONTH;

-- 8. 对应 10_请求头分析.xlsx
CREATE TABLE IF NOT EXISTS nginx_analytics.ads_request_header_analysis (
    stat_time DateTime,
    time_granularity LowCardinality(String),
    platform LowCardinality(String),
    
    -- User-Agent分析
    browser_name LowCardinality(String),
    browser_version String,
    os_name LowCardinality(String),
    os_version String,
    device_type LowCardinality(String),
    device_model String,
    
    -- SDK分析  
    sdk_type LowCardinality(String),                 -- WST-SDK-iOS/Android等
    sdk_version String,
    app_version String,
    
    -- Referer分析
    referer_domain LowCardinality(String),
    referer_type LowCardinality(String),             -- Search/Social/Direct/External
    search_engine LowCardinality(String),
    
    -- 统计指标
    request_count UInt64,
    unique_users UInt64,
    market_share Float64,                            -- 市场份额
    
    -- 性能关联
    avg_response_time Float64,
    error_rate Float64,
    slow_request_rate Float64,
    
    -- 兼容性分析
    compatibility_issues Array(String),              -- 兼容性问题
    performance_issues Array(String),                -- 性能问题
    
    created_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(stat_time)
ORDER BY (stat_time, platform, browser_name, os_name)
TTL stat_time + INTERVAL 1 YEAR;

-- 9. 对应 11_请求头性能关联分析.xlsx
CREATE TABLE IF NOT EXISTS nginx_analytics.ads_header_performance_correlation (
    stat_time DateTime,
    time_granularity LowCardinality(String),
    platform LowCardinality(String),
    header_combination String,                       -- 头部组合标识
    
    -- 头部特征
    browser_os_combo String,                         -- 浏览器+系统组合
    sdk_platform_combo String,                       -- SDK+平台组合
    device_network_combo String,                     -- 设备+网络组合
    
    -- 性能表现
    request_count UInt64,
    avg_response_time Float64,
    p50_response_time Float64,
    p95_response_time Float64,
    p99_response_time Float64,
    
    -- 性能分级
    excellent_requests UInt64,                       -- <1s
    good_requests UInt64,                            -- 1-3s
    fair_requests UInt64,                            -- 3-6s
    poor_requests UInt64,                            -- 6-10s
    unacceptable_requests UInt64,                    -- >10s
    
    -- 性能评分
    performance_score Float64,                       -- 0-100
    user_experience_grade LowCardinality(String),    -- A/B/C/D/F
    
    -- 关联分析
    correlation_strength Float64,                    -- 关联强度
    optimization_potential Float64,                  -- 优化潜力
    
    -- 建议
    optimization_suggestions Array(String),
    
    created_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(stat_time)
ORDER BY (stat_time, platform, performance_score, header_combination)
TTL stat_time + INTERVAL 1 YEAR;

-- 10. 对应 12_综合报告.xlsx - 执行摘要
CREATE TABLE IF NOT EXISTS nginx_analytics.ads_comprehensive_report (
    report_time DateTime,
    time_period LowCardinality(String),              -- hourly/daily/weekly/monthly
    platform LowCardinality(String),
    
    -- 整体概览
    total_requests UInt64,
    total_users UInt64,
    avg_qps Float64,
    peak_qps Float64,
    data_volume_gb Float64,
    
    -- 核心性能指标
    overall_avg_response_time Float64,
    overall_p95_response_time Float64,
    overall_success_rate Float64,
    overall_availability Float64,
    overall_apdex_score Float64,
    
    -- 业务健康度
    critical_api_health Float64,                     -- 核心API健康度
    business_continuity_score Float64,               -- 业务连续性评分
    user_satisfaction Float64,                       -- 用户满意度
    
    -- 问题汇总
    total_incidents UInt32,                          -- 事件总数
    critical_incidents UInt32,                       -- 严重事件
    high_error_apis Array(String),                   -- 高错误API列表
    slow_apis Array(String),                         -- 慢接口列表
    
    -- 趋势分析
    performance_trend Float64,                       -- 性能趋势
    traffic_growth Float64,                          -- 流量增长率
    error_trend Float64,                             -- 错误趋势
    
    -- 运维效率
    mttr_minutes Float64,
    mttd_minutes Float64,
    sla_achievement Float64,                         -- SLA达成率
    
    -- 容量规划
    current_capacity_usage Float64,
    capacity_forecast_30d Float64,                   -- 30天容量预测
    capacity_alert_level LowCardinality(String),     -- Green/Yellow/Red
    
    -- 安全概览
    security_score Float64,                          -- 安全评分
    attack_incidents UInt32,                         -- 攻击事件
    high_risk_ips UInt32,                            -- 高风险IP数
    
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(report_time)
ORDER BY (report_time, time_period, platform)
TTL report_time + INTERVAL 2 YEAR;

-- 11. 对应 13_接口错误分析.xlsx
CREATE TABLE IF NOT EXISTS nginx_analytics.ads_api_error_analysis (
    stat_time DateTime,                              -- 统计时间
    time_granularity LowCardinality(String),         -- hour/day/week
    platform LowCardinality(String),                 -- 平台维度
    access_type LowCardinality(String),              -- 接入方式
    api_path String,                                 -- 接口路径
    api_module LowCardinality(String),               -- 功能模块
    api_category LowCardinality(String),             -- 接口分类
    business_domain LowCardinality(String),          -- 业务域
    error_type LowCardinality(String),               -- 4xx_Client/5xx_Server/Timeout/Network
    error_code LowCardinality(String),               -- 具体错误码
    
    -- 错误统计
    error_count UInt64,
    total_requests UInt64,
    error_rate Float64,
    
    -- 错误时间分析
    first_error_time DateTime,
    last_error_time DateTime,
    error_duration_minutes UInt32,
    error_burst_count UInt32,                        -- 错误爆发次数
    
    -- 影响分析
    affected_users UInt64,
    business_impact LowCardinality(String),          -- Low/Medium/High/Critical
    revenue_impact Float64,                          -- 收入影响评估
    
    -- 根因分析
    root_cause_category LowCardinality(String),      -- Network/Database/Logic/External
    upstream_error_correlation Float64,              -- 上游错误关联度
    
    -- 错误模式
    error_pattern LowCardinality(String),            -- Spike/Sustained/Intermittent
    error_clustering Array(String),                  -- 错误聚类
    
    created_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(stat_time)
ORDER BY (stat_time, platform, error_rate, api_path)
TTL stat_time + INTERVAL 1 YEAR;

-- 12. 对应错误码下钻分析 - 新增专门的错误分析表
CREATE TABLE IF NOT EXISTS nginx_analytics.ads_error_analysis_detailed_v3 (
    stat_time DateTime,                              -- 统计时间
    time_granularity LowCardinality(String),         -- hour/day/week
    
    -- 权限维度
    tenant_code LowCardinality(String),              -- 租户代码
    team_code LowCardinality(String),                -- 团队代码
    environment LowCardinality(String),              -- 环境标识
    
    -- 平台入口维度
    platform LowCardinality(String),                 -- 平台维度
    access_type LowCardinality(String),              -- 接入方式
    access_entry_point LowCardinality(String),       -- 访问入口
    client_channel LowCardinality(String),           -- 客户端渠道
    api_path String,                                 -- 接口路径
    api_category LowCardinality(String),             -- 接口分类
    business_domain LowCardinality(String),          -- 业务域
    
    -- 错误码下钻维度（核心）
    response_status_code LowCardinality(String),     -- 具体错误码: 400/401/403/404/500/502/503/504
    error_code_group LowCardinality(String),         -- 错误码组: 4xx_client/5xx_server/gateway/upstream
    http_error_class LowCardinality(String),         -- HTTP错误分类: client_error/server_error/redirection
    error_severity_level LowCardinality(String),     -- 严重程度: critical/high/medium/low
    
    -- 错误定位维度
    upstream_server String,                          -- 上游服务器
    upstream_status_code LowCardinality(String),     -- 上游返回的状态码
    error_location LowCardinality(String),           -- 错误发生位置: gateway/service/database
    error_propagation_path String,                   -- 错误传播路径: client->gateway->service->db
    
    -- 业务影响维度
    business_operation_type LowCardinality(String),  -- 业务操作类型: login/payment/query/upload
    user_session_stage LowCardinality(String),       -- 用户会话阶段: first_request/active_session/checkout
    api_importance_level LowCardinality(String),     -- 接口重要性: critical/important/normal/optional
    
    -- 客户端分析维度
    client_ip_type LowCardinality(String),           -- 客户端类型: internal/external/suspicious
    user_agent_category LowCardinality(String),      -- 客户端分类: browser/mobile/api/bot
    user_type LowCardinality(String),                -- 用户类型: normal/vip/bot/crawler
    
    -- 时间模式维度
    time_pattern LowCardinality(String),             -- 时间模式: business_hours/peak_hours/off_hours
    error_burst_indicator LowCardinality(String),    -- 错误爆发指示: single/burst/sustained
    
    -- 核心指标
    error_count UInt64,                              -- 错误次数
    total_requests UInt64,                           -- 总请求数
    error_rate Float64,                              -- 错误率
    unique_error_users UInt64,                       -- 受影响的唯一用户数
    error_sessions UInt64,                           -- 受影响的会话数
    
    -- 业务影响指标
    business_loss_estimate Float64,                  -- 业务损失估算
    user_experience_score Float64,                   -- 用户体验评分
    sla_impact Float64,                              -- SLA影响程度
    
    -- 恢复指标
    mean_time_to_recovery Float64,                   -- 平均恢复时间(分钟)
    error_duration UInt32,                           -- 错误持续时间(分钟)
    resolution_success_rate Float64,                 -- 问题解决成功率
    
    -- 预警指标
    error_trend_score Float64,                       -- 错误趋势评分
    anomaly_score Float64,                           -- 异常评分
    escalation_risk LowCardinality(String),          -- 升级风险: low/medium/high/critical
    
    created_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(stat_time)
ORDER BY (stat_time, response_status_code, platform, access_type, api_path)
TTL stat_time + INTERVAL 1 YEAR;

-- =====================================================
-- 新增ADS主题表 - 支持更丰富的下钻分析
-- =====================================================

-- 14. 平台入口下钻分析表 - 核心下钻维度
CREATE TABLE IF NOT EXISTS nginx_analytics.ads_platform_entry_analysis (
    stat_time DateTime,
    time_granularity LowCardinality(String),
    tenant_code LowCardinality(String),
    
    -- 平台入口组合维度
    platform LowCardinality(String),
    platform_category LowCardinality(String),
    access_entry_point LowCardinality(String),
    client_channel LowCardinality(String),
    client_type LowCardinality(String),
    
    -- 组合统计指标
    total_requests UInt64,
    unique_users UInt64,
    avg_response_time Float64,
    p95_response_time Float64,
    success_rate Float64,
    error_rate Float64,
    conversion_rate Float64,                         -- 转化率
    bounce_rate Float64,                             -- 跳出率
    user_engagement_score Float64,                   -- 用户参与度评分
    
    -- 对比分析
    platform_market_share Float64,                   -- 平台市场份额
    entry_effectiveness_score Float64,               -- 入口有效性评分
    channel_roi Float64,                             -- 渠道投资回报率
    
    created_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
PARTITION BY (toYYYYMM(stat_time), tenant_code)
ORDER BY (stat_time, tenant_code, platform, access_entry_point)
TTL stat_time + INTERVAL 1 YEAR;

-- 15. 业务流程分析表 - 支持业务流程监控
CREATE TABLE IF NOT EXISTS nginx_analytics.ads_business_process_analysis (
    stat_time DateTime,
    time_granularity LowCardinality(String),
    tenant_code LowCardinality(String),
    
    -- 业务流程维度
    business_domain LowCardinality(String),
    business_operation_type LowCardinality(String),
    user_journey_stage LowCardinality(String),
    workflow_step LowCardinality(String),
    process_stage LowCardinality(String),
    
    -- 流程性能指标
    total_processes UInt64,                          -- 总流程数
    completed_processes UInt64,                      -- 完成流程数
    failed_processes UInt64,                         -- 失败流程数
    abandoned_processes UInt64,                      -- 放弃流程数
    completion_rate Float64,                         -- 完成率
    failure_rate Float64,                            -- 失败率
    abandonment_rate Float64,                        -- 放弃率
    
    -- 流程时间分析
    avg_process_duration Float64,                    -- 平均流程时长
    p95_process_duration Float64,                    -- 95%流程时长
    avg_step_duration Float64,                       -- 平均步骤时长
    bottleneck_step String,                          -- 瓶颈步骤
    
    -- 业务价值指标
    business_value_generated Float64,                -- 产生的业务价值
    cost_per_process Float64,                        -- 每个流程成本
    roi_score Float64,                               -- 投资回报率
    
    created_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
PARTITION BY (toYYYYMM(stat_time), tenant_code)
ORDER BY (stat_time, tenant_code, business_domain, business_operation_type)
TTL stat_time + INTERVAL 2 YEAR;

-- 16. 用户行为分析表 - 用户旅程和行为模式
CREATE TABLE IF NOT EXISTS nginx_analytics.ads_user_behavior_analysis (
    stat_time DateTime,
    time_granularity LowCardinality(String),
    tenant_code LowCardinality(String),
    
    -- 用户分类维度
    user_type LowCardinality(String),
    user_tier LowCardinality(String),
    user_segment LowCardinality(String),
    user_journey_stage LowCardinality(String),
    authentication_method LowCardinality(String),
    
    -- 用户行为指标
    active_users UInt64,                             -- 活跃用户数
    new_users UInt64,                                -- 新用户数
    returning_users UInt64,                          -- 回访用户数
    avg_session_duration Float64,                    -- 平均会话时长
    avg_page_views_per_session Float64,              -- 平均会话页面浏览数
    bounce_rate Float64,                             -- 跳出率
    conversion_rate Float64,                         -- 转化率
    user_retention_rate Float64,                     -- 用户留存率
    
    -- 用户体验指标
    avg_user_satisfaction_score Float64,             -- 平均用户满意度
    user_complaint_rate Float64,                     -- 用户投诉率
    support_ticket_rate Float64,                     -- 支持工单率
    
    -- 用户价值分析
    avg_customer_lifetime_value Float64,             -- 平均客户生命周期价值
    avg_revenue_per_user Float64,                    -- 平均用户收入
    user_engagement_score Float64,                   -- 用户参与度评分
    
    created_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
PARTITION BY (toYYYYMM(stat_time), tenant_code)
ORDER BY (stat_time, tenant_code, user_type, user_segment)
TTL stat_time + INTERVAL 1 YEAR;

-- 17. 安全监控分析表 - 安全威胁和风险分析
CREATE TABLE IF NOT EXISTS nginx_analytics.ads_security_monitoring_analysis (
    stat_time DateTime,
    time_granularity LowCardinality(String),
    tenant_code LowCardinality(String),
    
    -- 安全维度
    security_risk_level LowCardinality(String),
    threat_category LowCardinality(String),
    ip_reputation LowCardinality(String),
    attack_signature String,
    
    -- 安全事件统计
    total_security_events UInt64,                    -- 总安全事件数
    high_risk_events UInt64,                         -- 高风险事件数
    blocked_requests UInt64,                         -- 被阻断请求数
    suspicious_activities UInt64,                    -- 可疑活动数
    false_positive_rate Float64,                     -- 误报率
    
    -- 攻击分析
    ddos_attacks UInt64,                             -- DDoS攻击次数
    injection_attempts UInt64,                       -- 注入攻击尝试
    xss_attempts UInt64,                             -- XSS攻击尝试
    brute_force_attempts UInt64,                     -- 暴力破解尝试
    
    -- 安全响应指标
    avg_detection_time Float64,                      -- 平均检测时间
    avg_response_time Float64,                       -- 平均响应时间
    incident_resolution_rate Float64,                -- 事件解决率
    security_posture_score Float64,                  -- 安全态势评分
    
    created_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
PARTITION BY (toYYYYMM(stat_time), tenant_code)
ORDER BY (stat_time, tenant_code, security_risk_level, threat_category)
TTL stat_time + INTERVAL 6 MONTH;

-- 18. 租户权限使用分析表 - 多租户权限监控
CREATE TABLE IF NOT EXISTS nginx_analytics.ads_tenant_permission_analysis (
    stat_time DateTime,
    time_granularity LowCardinality(String),
    
    -- 租户权限维度
    tenant_code LowCardinality(String),
    team_code LowCardinality(String),
    environment LowCardinality(String),
    data_sensitivity Enum8('public'=1, 'internal'=2, 'confidential'=3, 'restricted'=4),
    compliance_zone LowCardinality(String),
    
    -- 权限使用统计
    total_requests UInt64,                           -- 总请求数
    authorized_requests UInt64,                      -- 授权请求数
    unauthorized_requests UInt64,                    -- 未授权请求数
    permission_denied_requests UInt64,               -- 权限拒绝请求数
    
    -- 数据访问统计
    public_data_access UInt64,                       -- 公开数据访问次数
    internal_data_access UInt64,                     -- 内部数据访问次数
    confidential_data_access UInt64,                 -- 机密数据访问次数
    restricted_data_access UInt64,                   -- 受限数据访问次数
    
    -- 合规性指标
    compliance_score Float64,                        -- 合规性评分
    policy_violation_count UInt32,                   -- 策略违规次数
    audit_trail_completeness Float64,                -- 审计轨迹完整性
    data_retention_compliance Float64,               -- 数据保留合规性
    
    -- 成本分析
    total_cost Float64,                              -- 总成本
    cost_per_request Float64,                        -- 每请求成本
    resource_utilization Float64,                    -- 资源利用率
    
    created_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
PARTITION BY (toYYYYMM(stat_time), tenant_code)
ORDER BY (stat_time, tenant_code, team_code, environment)
TTL stat_time + INTERVAL 3 YEAR;  -- 合规要求保留更长时间

-- =====================================================
-- 表注释和说明
-- =====================================================

-- 核心分析主题表 (1-13)：
-- 01. api_performance_analysis_v3: 接口性能分析 - 支持平台入口下钻、租户隔离
-- 02. service_level_analysis_v3: 服务层级分析 - 支持微服务架构、SLA监控
-- 03. slow_request_analysis: 慢请求分析 - 慢请求识别和优化建议
-- 04. status_code_analysis: 状态码统计 - HTTP状态码分布和异常检测  
-- 05. time_dimension_analysis: 时间维度分析 - 全部和指定接口的时间趋势
-- 06. service_stability_analysis: 服务稳定性分析 - SLA和MTTR监控
-- 07. ip_source_analysis: IP来源分析 - 地理分布和安全风险评估
-- 08. request_header_analysis: 请求头分析 - User-Agent和Referer解析
-- 09. header_performance_correlation: 请求头性能关联分析 - 头部与性能关系
-- 10. comprehensive_report: 综合报告 - 执行摘要和整体健康度
-- 11. api_error_analysis: 接口错误分析 - 错误模式和根因分析
-- 12. error_analysis_detailed: 错误码下钻分析 - 多维度错误分析

-- 新增分析主题表 (14-18)：
-- 14. platform_entry_analysis: 平台入口下钻分析 - 支持工作介绍核心需求
-- 15. business_process_analysis: 业务流程分析 - 业务流程监控和优化
-- 16. user_behavior_analysis: 用户行为分析 - 用户旅程和行为模式
-- 17. security_monitoring_analysis: 安全监控分析 - 安全威胁检测和风险评估
-- 18. tenant_permission_analysis: 租户权限分析 - 多租户权限使用监控和合规性