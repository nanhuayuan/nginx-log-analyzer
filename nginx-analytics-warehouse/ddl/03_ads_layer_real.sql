-- ==========================================
-- ADS层 - 对应Self目录的12个分析主题
-- 基于你的实际需求设计
-- ==========================================

-- 1. 对应 01.接口性能分析.xlsx
CREATE TABLE IF NOT EXISTS ads_api_performance_analysis (
    stat_time DateTime,                              -- 统计时间
    time_granularity LowCardinality(String),         -- hour/day/week
    platform LowCardinality(String),                 -- 平台维度
    access_type LowCardinality(String),              -- 接入方式
    api_path String,                                 -- 接口路径
    api_module LowCardinality(String),               -- 功能模块
    api_category LowCardinality(String),             -- 接口分类
    business_domain LowCardinality(String),          -- 业务域
    
    -- 请求量指标
    total_requests UInt64,
    unique_clients UInt64,
    qps Float64,
    
    -- 性能指标（对应Self分析）
    avg_response_time Float64,                       -- 平均响应时间
    p50_response_time Float64,
    p90_response_time Float64,
    p95_response_time Float64,
    p99_response_time Float64,
    max_response_time Float64,
    
    -- 成功率指标
    success_requests UInt64,
    error_requests UInt64,
    success_rate Float64,
    error_rate Float64,
    business_success_rate Float64,                   -- 业务成功率
    
    -- 慢请求分析
    slow_requests UInt64,                            -- >3s
    very_slow_requests UInt64,                       -- >10s
    slow_rate Float64,
    very_slow_rate Float64,
    
    -- 用户体验指标
    apdex_score Float64,                             -- 用户体验指数
    user_satisfaction_score Float64,                 -- 用户满意度
    
    -- 业务价值权重
    business_value_score UInt8,
    importance_level LowCardinality(String),
    
    created_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(stat_time)
ORDER BY (stat_time, platform, api_module, api_path)
TTL stat_time + INTERVAL 2 YEAR;

-- 2. 对应 02.服务层级分析.xlsx  
CREATE TABLE IF NOT EXISTS ads_service_level_analysis (
    stat_time DateTime,
    time_granularity LowCardinality(String),
    platform LowCardinality(String),
    service_name LowCardinality(String),             -- 服务名称
    cluster_node LowCardinality(String),             -- 集群节点
    upstream_server String,                          -- 上游服务
    
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
CREATE TABLE IF NOT EXISTS ads_slow_request_analysis (
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
CREATE TABLE IF NOT EXISTS ads_status_code_analysis (
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
CREATE TABLE IF NOT EXISTS ads_time_dimension_analysis (
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
CREATE TABLE IF NOT EXISTS ads_service_stability_analysis (
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
CREATE TABLE IF NOT EXISTS ads_ip_source_analysis (
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
CREATE TABLE IF NOT EXISTS ads_request_header_analysis (
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
CREATE TABLE IF NOT EXISTS ads_header_performance_correlation (
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
CREATE TABLE IF NOT EXISTS ads_comprehensive_report (
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
CREATE TABLE IF NOT EXISTS ads_api_error_analysis (
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

COMMENT ON TABLE ads_api_performance_analysis IS '01.接口性能分析 - 支持任意时间段、多平台维度分析';
COMMENT ON TABLE ads_service_level_analysis IS '02.服务层级分析 - 服务健康度和性能监控';
COMMENT ON TABLE ads_slow_request_analysis IS '03.慢请求分析 - 慢请求识别和优化建议';
COMMENT ON TABLE ads_status_code_analysis IS '04.状态码统计 - HTTP状态码分布和异常检测';  
COMMENT ON TABLE ads_time_dimension_analysis IS '05.时间维度分析 - 全部和指定接口的时间趋势';
COMMENT ON TABLE ads_service_stability_analysis IS '06.服务稳定性分析 - SLA和MTTR监控';
COMMENT ON TABLE ads_ip_source_analysis IS '08.IP来源分析 - 地理分布和安全风险评估';
COMMENT ON TABLE ads_request_header_analysis IS '10.请求头分析 - User-Agent和Referer解析';
COMMENT ON TABLE ads_header_performance_correlation IS '11.请求头性能关联分析 - 头部与性能关系';
COMMENT ON TABLE ads_comprehensive_report IS '12.综合报告 - 执行摘要和整体健康度';
COMMENT ON TABLE ads_api_error_analysis IS '13.接口错误分析 - 错误模式和根因分析';