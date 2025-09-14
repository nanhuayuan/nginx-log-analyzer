-- ==========================================
-- 增强型DWD设计 v3.0 - 全维度业务分析层
-- 基于工作介绍需求：支持平台入口下钻、错误链路分析、权限控制
-- 新增：多租户权限、安全分析、业务流程追踪、智能分类等
-- ==========================================

-- DWD层：业务强化版支持全维度下钻分析
CREATE TABLE IF NOT EXISTS nginx_analytics.dwd_nginx_enriched_v3 (
    id UInt64, -- 主键ID
    ods_id UInt64, -- ODS层关联ID
    log_time DateTime64(3) CODEC(Delta, ZSTD(1)), -- 日志时间
    
    -- 时间分区维度
    date_partition Date, -- 日期分区
    hour_partition UInt8, -- 小时分区
    minute_partition UInt8, -- 分钟分区
    second_partition UInt8, -- 秒分区
    quarter_partition UInt8, -- 季度分区
    week_partition UInt8, -- 周分区
    
    -- ===== 权限控制维度 =====
    tenant_code LowCardinality(String), -- 租户代码(多租户隔离)
    team_code LowCardinality(String), -- 团队代码(团队权限)
    environment LowCardinality(String), -- 环境标识(dev/test/staging/prod)
    data_sensitivity Enum8('public'=1, 'internal'=2, 'confidential'=3, 'restricted'=4), -- 数据敏感级别
    cost_center LowCardinality(String), -- 成本中心
    business_unit LowCardinality(String), -- 业务单元
    region_code LowCardinality(String), -- 区域代码
    compliance_zone LowCardinality(String), -- 合规区域(GDPR/SOX/PCI等)
    
    -- ===== 网络和客户端信息 =====
    client_ip String CODEC(ZSTD(1)), -- 客户端IP
    client_port UInt32, -- 客户端端口
    xff_ip String CODEC(ZSTD(1)), -- X-Forwarded-For IP
    client_real_ip String CODEC(ZSTD(1)), -- 客户端真实IP
    client_ip_type LowCardinality(String), -- IP类型(internal/external/cdn/proxy)
    client_ip_classification LowCardinality(String), -- IP分类(trusted/untrusted/suspicious/blocked)
    client_country LowCardinality(String), -- 客户端国家
    client_region LowCardinality(String), -- 客户端地区
    client_city LowCardinality(String), -- 客户端城市
    client_isp LowCardinality(String), -- 网络服务商
    client_org LowCardinality(String), -- 客户端组织
    client_asn UInt32, -- 自治系统号
    
    -- ===== 服务端和基础设施信息 =====
    server_name LowCardinality(String), -- 服务器名称
    server_port UInt16, -- 服务器端口
    server_protocol LowCardinality(String), -- 服务器协议
    load_balancer_node LowCardinality(String), -- 负载均衡节点
    edge_location LowCardinality(String), -- 边缘节点
    datacenter LowCardinality(String), -- 数据中心
    availability_zone LowCardinality(String), -- 可用区
    cluster_node LowCardinality(String), -- 集群节点
    instance_id String CODEC(ZSTD(1)), -- 实例ID
    pod_name String CODEC(ZSTD(1)), -- Pod名称
    container_id String CODEC(ZSTD(1)), -- 容器ID
    
    -- ===== HTTP请求信息 =====
    request_method LowCardinality(String), -- 请求方法
    request_uri String CODEC(ZSTD(1)), -- 请求URI
    request_uri_normalized String CODEC(ZSTD(1)), -- 标准化请求URI
    request_full_uri String CODEC(ZSTD(1)), -- 完整请求URI
    request_path String CODEC(ZSTD(1)), -- 请求路径(不含参数)
    query_parameters String CODEC(ZSTD(1)), -- 查询参数
    query_params_count UInt16, -- 查询参数数量
    request_body_size UInt64, -- 请求体大小
    http_protocol_version LowCardinality(String), -- HTTP协议版本
    content_type String CODEC(ZSTD(1)), -- 内容类型
    accept_language String CODEC(ZSTD(1)), -- 接受语言
    accept_encoding String CODEC(ZSTD(1)), -- 接受编码
    
    -- ===== HTTP响应信息 =====
    response_status_code LowCardinality(String), -- 响应状态码
    response_status_class LowCardinality(String), -- 状态码类别(2xx/3xx/4xx/5xx)
    response_body_size UInt64, -- 响应体大小
    response_body_size_kb Float64, -- 响应体大小KB
    response_content_type String CODEC(ZSTD(1)), -- 响应内容类型
    response_content_encoding String CODEC(ZSTD(1)), -- 响应内容编码
    response_cache_control String CODEC(ZSTD(1)), -- 缓存控制头
    response_etag String CODEC(ZSTD(1)), -- ETag头
    total_bytes_sent UInt64, -- 总发送字节数
    total_bytes_sent_kb Float64, -- 总发送字节数KB
    bytes_received UInt64, -- 接收字节数
    
    -- ===== 性能指标详细分解 - 统一毫秒单位 (2025-09-14 单位标准化) =====
    -- 注意：原始日志ar_time是秒单位，ETL转换时需要 * 1000 转换为毫秒存储
    -- 优势：1) 避免小数点提高性能 2) Grafana阈值更直观 3) 符合行业APM标准
    total_request_duration UInt32, -- 总请求时长(毫秒) - 从ar_time*1000转换
    request_processing_time UInt32, -- 请求处理时间(毫秒)
    response_send_time UInt32, -- 响应发送时间(毫秒)
    upstream_connect_time UInt32, -- 上游连接时间(毫秒) - 从upstream_connect_time*1000转换
    upstream_header_time UInt32, -- 上游头部时间(毫秒) - 从upstream_header_time*1000转换
    upstream_response_time UInt32, -- 上游响应时间(毫秒) - 从upstream_response_time*1000转换
    backend_connect_phase UInt32, -- 后端连接阶段(毫秒)
    backend_process_phase UInt32, -- 后端处理阶段(毫秒)
    backend_transfer_phase UInt32, -- 后端传输阶段(毫秒)
    nginx_transfer_phase UInt32, -- Nginx传输阶段(毫秒)
    backend_total_phase UInt32, -- 后端总阶段(毫秒)
    network_phase UInt32, -- 网络阶段(毫秒)
    processing_phase UInt32, -- 处理阶段(毫秒)
    transfer_phase UInt32, -- 传输阶段(毫秒)
    
    -- ===== 性能计算指标 =====
    response_transfer_speed Float64, -- 响应传输速度(bytes/s)
    total_transfer_speed Float64, -- 总传输速度
    nginx_transfer_speed Float64, -- Nginx传输速度
    backend_efficiency Float64, -- 后端效率(%)
    network_overhead Float64, -- 网络开销(%)
    transfer_ratio Float64, -- 传输比率
    connection_cost_ratio Float64, -- 连接成本比率
    processing_efficiency_index Float64, -- 处理效率指数
    performance_score Float64, -- 综合性能评分(0-100)
    latency_percentile Float64, -- 延迟分位数
    
    -- ===== 平台和客户端分析维度(核心下钻维度) =====
    platform LowCardinality(String), -- 平台类型(Android/iOS/HarmonyOS/Web/API)
    platform_version String CODEC(ZSTD(1)), -- 平台版本
    platform_category LowCardinality(String), -- 平台分类(mobile/desktop/tablet/tv/iot)
    app_version String CODEC(ZSTD(1)), -- 应用版本
    app_build_number String CODEC(ZSTD(1)), -- 应用构建号
    device_type LowCardinality(String), -- 设备类型(Mobile/Tablet/Desktop/Bot/IoT)
    device_model String CODEC(ZSTD(1)), -- 设备型号
    device_manufacturer LowCardinality(String), -- 设备制造商
    screen_resolution String CODEC(ZSTD(1)), -- 屏幕分辨率
    browser_type LowCardinality(String), -- 浏览器类型(Chrome/Safari/WebView等)
    browser_version String CODEC(ZSTD(1)), -- 浏览器版本
    browser_engine LowCardinality(String), -- 浏览器引擎(Webkit/Blink/Gecko)
    os_type LowCardinality(String), -- 操作系统(iOS/Android/HarmonyOS/Windows)
    os_version String CODEC(ZSTD(1)), -- 系统版本
    os_architecture LowCardinality(String), -- 系统架构(x86/x64/arm/arm64)
    
    -- ===== SDK和集成方式 =====
    sdk_type LowCardinality(String), -- SDK类型(WST-SDK-iOS/WST-SDK-ANDROID/Native等)
    sdk_version String CODEC(ZSTD(1)), -- SDK版本
    integration_type LowCardinality(String), -- 集成类型(native/hybrid/web/rn/flutter)
    framework_type LowCardinality(String), -- 框架类型(React/Vue/Angular/Flutter等)
    framework_version String CODEC(ZSTD(1)), -- 框架版本
    
    -- ===== 访问来源和入口分析(核心下钻维度) =====
    access_entry_point LowCardinality(String), -- 访问入口(gateway/direct/cdn/proxy)
    entry_source LowCardinality(String), -- 入口来源(Direct/Search/Social/External/Internal/Push)
    entry_source_detail String CODEC(ZSTD(1)), -- 入口来源详情
    client_channel LowCardinality(String), -- 客户端渠道(official/partner/third_party)
    traffic_source LowCardinality(String), -- 流量来源(organic/paid/referral/direct)
    campaign_id String CODEC(ZSTD(1)), -- 营销活动ID
    utm_source String CODEC(ZSTD(1)), -- UTM来源
    utm_medium String CODEC(ZSTD(1)), -- UTM媒介
    utm_campaign String CODEC(ZSTD(1)), -- UTM活动
    utm_content String CODEC(ZSTD(1)), -- UTM内容
    utm_term String CODEC(ZSTD(1)), -- UTM词汇
    
    -- ===== Referer和搜索引擎分析 =====
    referer_url String CODEC(ZSTD(1)), -- 完整来源URL
    referer_domain String CODEC(ZSTD(1)), -- 来源域名
    referer_domain_type LowCardinality(String), -- 来源域名类型(search/social/news/government)
    search_engine LowCardinality(String), -- 搜索引擎(Google/Baidu/Bing等)
    search_keywords String CODEC(ZSTD(1)), -- 搜索关键词
    social_media LowCardinality(String), -- 社交媒体(WeChat/Weibo/QQ等)
    social_media_type LowCardinality(String), -- 社交媒体类型(instant_message/social_network/short_video)
    
    -- ===== Bot和爬虫识别 =====
    bot_type LowCardinality(String), -- 机器人类型(search_engine/social_media/monitoring/malicious)
    bot_name LowCardinality(String), -- 机器人名称(Googlebot/Baiduspider等)
    is_bot Bool, -- 是否为机器人
    bot_probability Float32, -- 机器人概率(0-1)
    crawler_category LowCardinality(String), -- 爬虫分类(legitimate/suspicious/malicious)
    
    -- ===== API和业务领域分析(核心下钻维度) =====
    api_category LowCardinality(String), -- API业务分类(user/order/payment/search/content)
    api_subcategory LowCardinality(String), -- API子分类
    api_module LowCardinality(String), -- 功能模块(gxrz/zgt/search/calendar等)
    api_submodule LowCardinality(String), -- 子功能模块
    api_version LowCardinality(String), -- API版本(rest/v1/v2/graphql等)
    api_endpoint_type LowCardinality(String), -- 端点类型(CRUD操作)
    business_domain LowCardinality(String), -- 业务域(用户中心/搜索/日历等)
    business_subdomain LowCardinality(String), -- 业务子域
    functional_area LowCardinality(String), -- 功能区域(frontend/backend/middleware/database)
    service_tier LowCardinality(String), -- 服务层级(web/api/service/data)
    
    -- ===== 业务操作和流程分析 =====
    business_operation_type LowCardinality(String), -- 业务操作类型(login/register/payment/search/view/download)
    business_operation_subtype LowCardinality(String), -- 业务操作子类型
    user_journey_stage LowCardinality(String), -- 用户旅程阶段(onboarding/active/conversion/retention)
    user_session_stage LowCardinality(String), -- 用户会话阶段(first_request/active_session/checkout/logout)
    transaction_type LowCardinality(String), -- 交易类型(query/mutation/subscription)
    workflow_step LowCardinality(String), -- 工作流步骤
    process_stage LowCardinality(String), -- 流程阶段
    
    -- ===== 接入方式和客户端分类(核心下钻维度) =====
    access_type LowCardinality(String), -- 接入方式(APP_Native/H5_WebView/Browser/API/RPC)
    access_method LowCardinality(String), -- 访问方法(sync/async/streaming)
    client_category LowCardinality(String), -- 客户端分类(Mobile_App/Desktop_Web/Server_API/IoT_Device)
    client_type LowCardinality(String), -- 客户端类型(official/third_party/partner/internal)
    client_classification LowCardinality(String), -- 客户端分类(trusted/verified/unverified/suspicious)
    integration_pattern LowCardinality(String), -- 集成模式(direct/proxy/gateway/mesh)
    
    -- ===== 用户和身份分析 =====
    user_id String CODEC(ZSTD(1)), -- 用户ID
    session_id String CODEC(ZSTD(1)), -- 会话ID
    user_type LowCardinality(String), -- 用户类型(registered/guest/admin/service)
    user_tier LowCardinality(String), -- 用户等级(vip/premium/standard/trial/free)
    user_segment LowCardinality(String), -- 用户群体(enterprise/sme/consumer/developer)
    authentication_method LowCardinality(String), -- 认证方式(password/oauth/sso/biometric/api_key)
    authorization_level LowCardinality(String), -- 授权级别(admin/user/readonly/restricted)
    
    -- ===== 应用和服务信息 =====
    application_name LowCardinality(String), -- 应用名称
    application_version String CODEC(ZSTD(1)), -- 应用版本
    service_name LowCardinality(String), -- 服务名称
    service_version String CODEC(ZSTD(1)), -- 服务版本
    microservice_name LowCardinality(String), -- 微服务名称
    service_mesh_name LowCardinality(String), -- 服务网格名称
    upstream_server String CODEC(ZSTD(1)), -- 上游服务器
    upstream_service LowCardinality(String), -- 上游服务
    downstream_service LowCardinality(String), -- 下游服务
    
    -- ===== 链路跟踪和关联分析 =====
    trace_id String CODEC(ZSTD(1)), -- 链路跟踪ID
    span_id String CODEC(ZSTD(1)), -- Span ID
    parent_span_id String CODEC(ZSTD(1)), -- 父Span ID
    correlation_id String CODEC(ZSTD(1)), -- 关联ID
    request_id String CODEC(ZSTD(1)), -- 请求ID
    transaction_id String CODEC(ZSTD(1)), -- 事务ID
    business_transaction_id String CODEC(ZSTD(1)), -- 业务事务ID
    batch_id String CODEC(ZSTD(1)), -- 批处理ID
    
    -- ===== 缓存和连接信息 =====
    cache_status LowCardinality(String), -- 缓存状态(HIT/MISS/BYPASS/STALE)
    cache_layer LowCardinality(String), -- 缓存层级(L1/L2/CDN/Edge)
    cache_key String CODEC(ZSTD(1)), -- 缓存键
    cache_age UInt32, -- 缓存年龄
    cache_hit_ratio Float32, -- 缓存命中率
    connection_requests UInt32, -- 连接请求数
    connection_id String CODEC(ZSTD(1)), -- 连接ID
    connection_type LowCardinality(String), -- 连接类型(keep_alive/close/upgrade)
    ssl_session_reused Bool, -- SSL会话重用
    
    -- ===== 业务标识和标签 =====
    business_sign LowCardinality(String), -- 业务标识
    feature_flag String CODEC(ZSTD(1)), -- 功能开关
    ab_test_group LowCardinality(String), -- AB测试分组
    experiment_id String CODEC(ZSTD(1)), -- 实验编号
    custom_tags Array(String), -- 自定义标签
    business_tags Array(String), -- 业务标签
    
    -- ===== 请求头和元数据 =====
    user_agent_string String CODEC(ZSTD(1)), -- 用户代理字符串
    custom_headers Map(String, String), -- 自定义请求头
    security_headers Map(String, String), -- 安全头信息
    cookie_count UInt16, -- Cookie数量
    header_size UInt32, -- 请求头大小
    
    -- ===== 日志元信息 =====
    log_source_file LowCardinality(String), -- 日志源文件
    log_format_version LowCardinality(String), -- 日志格式版本
    log_level LowCardinality(String), -- 日志级别
    raw_log_entry String CODEC(ZSTD(1)), -- 原始日志条目
    
    -- ===== 错误分析核心维度(工作介绍重点) =====
    error_code_group LowCardinality(String), -- 错误码组(4xx_client/5xx_server/gateway_timeout/upstream_error)
    http_error_class LowCardinality(String), -- HTTP错误分类(client_error/server_error/redirection/success)
    error_severity_level LowCardinality(String), -- 错误严重程度(critical/high/medium/low/info)
    error_category LowCardinality(String), -- 错误分类(network/application/business/security/infrastructure)
    error_subcategory LowCardinality(String), -- 错误子分类
    error_source LowCardinality(String), -- 错误源(client/gateway/service/database/external)
    error_propagation_path String CODEC(ZSTD(1)), -- 错误传播路径(client->gateway->service->db)
    upstream_status_code LowCardinality(String), -- 上游返回的状态码
    error_correlation_id String CODEC(ZSTD(1)), -- 错误关联ID
    error_chain Array(String), -- 错误链路
    root_cause_analysis String CODEC(ZSTD(1)), -- 根因分析
    
    -- ===== 状态判断标记 =====
    is_success Bool, -- 是否成功(HTTP 2xx)
    is_business_success Bool, -- 业务逻辑成功
    is_error Bool, -- 是否错误(4xx/5xx)
    is_client_error Bool, -- 是否客户端错误(4xx)
    is_server_error Bool, -- 是否服务端错误(5xx)
    is_retry Bool, -- 是否重试
    has_anomaly Bool, -- 是否有异常

    -- 兼容性字段 (保持向后兼容)
    is_slow Bool, -- 兼容性字段，映射到perf_slow
    is_very_slow Bool, -- 兼容性字段，映射到perf_very_slow

    -- ===== 性能分级字段 - 多层次预警体系 (2025-09-14) =====
    -- 基于行业APM最佳实践，提供6级性能分类和5个布尔判断字段
    -- 参考Google Core Web Vitals和主流APM系统标准

    -- 性能分级布尔字段 (添加perf_前缀避免命名冲突)
    perf_attention Bool,      -- 关注级别: >500毫秒(0.5秒) - 开始影响用户体验
    perf_warning Bool,        -- 预警级别: >1000毫秒(1秒) - 明显性能问题
    perf_slow Bool,          -- 慢请求: >3000毫秒(3秒) - 严重性能问题
    perf_very_slow Bool,     -- 非常慢: >10000毫秒(10秒) - 系统异常
    perf_timeout Bool,       -- 超时: >30000毫秒(30秒) - 系统故障级别

    -- 性能等级数值字段 (便于计算和聚合分析)
    performance_level UInt8, -- 性能等级分类:
                            -- 1: excellent (0-200ms) - 极优响应
                            -- 2: good (200-500ms) - 良好响应
                            -- 3: acceptable (500ms-1s) - 可接受响应
                            -- 4: slow (1-3s) - 慢响应，需关注
                            -- 5: critical (3-30s) - 严重问题，需优化
                            -- 6: timeout (>30s) - 超时故障，需紧急处理
    anomaly_type LowCardinality(String), -- 异常类型(performance/security/business/data)
    anomaly_severity LowCardinality(String), -- 异常严重程度
    
    -- ===== 用户体验和业务指标 =====
    user_experience_level LowCardinality(String), -- 用户体验分级(Excellent/Good/Fair/Poor/Unacceptable)
    apdex_classification LowCardinality(String), -- Apdex分类(Satisfied/Tolerating/Frustrated)
    performance_rating LowCardinality(String), -- 性能评级(A/B/C/D/F)
    sla_compliance Bool, -- SLA合规性
    sla_violation_type LowCardinality(String), -- SLA违约类型
    
    -- ===== API重要性和业务价值 =====
    api_importance_level LowCardinality(String), -- API重要性(critical/important/normal/optional)
    business_criticality LowCardinality(String), -- 业务关键性(mission_critical/business_critical/important/standard)
    business_value_score UInt8 DEFAULT 5, -- 业务价值评分(1-10)
    revenue_impact_level LowCardinality(String), -- 收入影响级别(high/medium/low/none)
    customer_impact_level LowCardinality(String), -- 客户影响级别
    
    -- ===== 数据质量和解析 =====
    data_quality_score Float64, -- 数据质量评分(0-100)
    data_completeness Float32, -- 数据完整性(0-1)
    parsing_errors Array(String), -- 解析错误记录
    validation_errors Array(String), -- 校验错误
    enrichment_status LowCardinality(String), -- 数据丰富状态(complete/partial/failed)
    
    -- ===== 安全和风控分析 =====
    security_risk_score UInt8 DEFAULT 0, -- 安全风险评分(0-100)
    security_risk_level LowCardinality(String), -- 安全风险等级(critical/high/medium/low/none)
    threat_category LowCardinality(String), -- 威胁类别(ddos/injection/xss/brute_force/scanner)
    attack_signature String CODEC(ZSTD(1)), -- 攻击特征
    ip_reputation LowCardinality(String), -- IP声誉(trusted/neutral/suspicious/malicious)
    geo_anomaly Bool, -- 地理位置异常
    access_pattern_anomaly Bool, -- 访问模式异常
    rate_limit_hit Bool, -- 触发限流
    blocked_by_waf Bool, -- 被WAF阻断
    fraud_score Float32, -- 欺诈评分(0-1)
    
    -- ===== 网络和地理信息 =====
    network_type LowCardinality(String), -- 网络类型(wifi/4g/5g/broadband/satellite)
    ip_risk_level LowCardinality(String), -- IP风险等级(safe/low/medium/high/critical)
    is_internal_ip Bool, -- 是否内网IP
    is_tor_exit Bool, -- 是否Tor出口节点
    is_proxy Bool, -- 是否代理
    is_vpn Bool, -- 是否VPN
    is_datacenter Bool, -- 是否数据中心IP
    
    -- ===== 时间维度计算字段 =====
    date Date MATERIALIZED toDate(log_time), -- 日期
    hour UInt8 MATERIALIZED toHour(log_time), -- 小时
    minute UInt8 MATERIALIZED toMinute(log_time), -- 分钟
    second UInt8 MATERIALIZED toSecond(log_time), -- 秒
    quarter UInt8 MATERIALIZED toQuarter(log_time), -- 季度
    week UInt8 MATERIALIZED toWeek(log_time), -- 周
    day_of_year UInt16 MATERIALIZED toDayOfYear(log_time), -- 年中的天数
    date_hour String MATERIALIZED concat(toString(date), '_', toString(hour)), -- 日期小时
    date_hour_minute String MATERIALIZED concat(toString(date), '_', toString(hour), '_', toString(minute)), -- 日期小时分钟
    weekday UInt8 MATERIALIZED toDayOfWeek(log_time), -- 星期几
    is_weekend Bool MATERIALIZED weekday IN (6, 7), -- 是否周末
    is_holiday Bool DEFAULT false, -- 是否节假日
    
    -- ===== 时间模式分类 =====
    time_period LowCardinality(String) MATERIALIZED -- 时间段分类
        multiIf(
            hour < 6, 'Dawn',           -- 凌晨 0-6
            hour < 9, 'EarlyMorning',   -- 早晨 6-9
            hour < 12, 'Morning',       -- 上午 9-12  
            hour < 14, 'Noon',          -- 中午 12-14
            hour < 17, 'Afternoon',     -- 下午 14-17
            hour < 19, 'EarlyEvening',  -- 傍晚 17-19
            hour < 22, 'Evening',       -- 晚上 19-22
            'Night'                     -- 夜间 22-24
        ),
    
    business_hours_type LowCardinality(String) MATERIALIZED -- 工作时间类型
        multiIf(
            weekday <= 5 AND hour >= 9 AND hour <= 17, 'business_hours',
            weekday <= 5 AND (hour >= 7 AND hour < 9 OR hour > 17 AND hour <= 20), 'extended_hours',
            weekday > 5 AND hour >= 10 AND hour <= 18, 'weekend_hours',
            'off_hours'
        ),
    
    traffic_pattern LowCardinality(String) MATERIALIZED -- 流量模式
        multiIf(
            (weekday <= 5 AND (hour >= 9 AND hour <= 11 OR hour >= 14 AND hour <= 16)), 'peak',
            (weekday <= 5 AND hour >= 8 AND hour <= 18), 'normal',
            'off_peak'
        ),
    
    -- ===== 系统字段 =====
    created_at DateTime DEFAULT now(), -- 创建时间
    updated_at DateTime DEFAULT now(), -- 更新时间
    data_version UInt16 DEFAULT 3, -- 数据版本
    last_processed_at DateTime DEFAULT now(), -- 最后处理时间
    processing_flags Array(String) DEFAULT [], -- 处理标记
    
    -- ===== 扩展字段 =====
    custom_dimensions Map(String, String), -- 自定义维度
    custom_metrics Map(String, Float64), -- 自定义指标
    metadata Map(String, String) -- 元数据
    
) ENGINE = MergeTree()
PARTITION BY (date_partition, tenant_code, environment)  -- 按日期+租户+环境三重分区
ORDER BY (date_partition, tenant_code, environment, platform, api_category, hour_partition, log_time)
SETTINGS index_granularity = 8192;

-- =====================================================
-- 高性能索引设计 - 优化查询性能
-- =====================================================

-- 权限控制相关索引
ALTER TABLE nginx_analytics.dwd_nginx_enriched_v3 
ADD INDEX IF NOT EXISTS idx_tenant_team_env (tenant_code, team_code, environment) TYPE bloom_filter(0.01) GRANULARITY 1,
ADD INDEX IF NOT EXISTS idx_data_sensitivity (data_sensitivity, compliance_zone) TYPE set(0) GRANULARITY 2;

-- 平台入口分析索引（核心下钻维度）  
ALTER TABLE nginx_analytics.dwd_nginx_enriched_v3
ADD INDEX IF NOT EXISTS idx_platform_access (platform, access_entry_point) TYPE bloom_filter(0.01) GRANULARITY 1,
ADD INDEX IF NOT EXISTS idx_access_type_channel (access_type, client_channel) TYPE bloom_filter(0.01) GRANULARITY 1,
ADD INDEX IF NOT EXISTS idx_client_classification (client_classification, client_type) TYPE bloom_filter(0.01) GRANULARITY 1;

-- API业务分析索引（核心下钻维度）
ALTER TABLE nginx_analytics.dwd_nginx_enriched_v3
ADD INDEX IF NOT EXISTS idx_api_business (api_category, business_domain) TYPE bloom_filter(0.01) GRANULARITY 1,
ADD INDEX IF NOT EXISTS idx_business_operation (business_operation_type, api_importance_level) TYPE bloom_filter(0.01) GRANULARITY 1,
ADD INDEX IF NOT EXISTS idx_service_tier (service_name, service_tier) TYPE bloom_filter(0.01) GRANULARITY 1;

-- 错误分析索引（工作介绍重点）
ALTER TABLE nginx_analytics.dwd_nginx_enriched_v3
ADD INDEX IF NOT EXISTS idx_error_analysis (error_code_group, error_severity_level) TYPE bloom_filter(0.01) GRANULARITY 1,
ADD INDEX IF NOT EXISTS idx_error_classification (http_error_class, error_category) TYPE bloom_filter(0.01) GRANULARITY 1,
ADD INDEX IF NOT EXISTS idx_error_source (error_source, upstream_status_code) TYPE set(0) GRANULARITY 2;

-- 性能分析索引
ALTER TABLE nginx_analytics.dwd_nginx_enriched_v3
ADD INDEX IF NOT EXISTS idx_performance_slow (is_slow, is_very_slow) TYPE set(0) GRANULARITY 3,
ADD INDEX IF NOT EXISTS idx_response_time (total_request_duration) TYPE minmax GRANULARITY 4,
ADD INDEX IF NOT EXISTS idx_performance_rating (performance_rating, user_experience_level) TYPE set(0) GRANULARITY 2;

-- 安全分析索引
ALTER TABLE nginx_analytics.dwd_nginx_enriched_v3
ADD INDEX IF NOT EXISTS idx_security_risk (security_risk_level, ip_reputation) TYPE bloom_filter(0.01) GRANULARITY 1,
ADD INDEX IF NOT EXISTS idx_threat_analysis (threat_category, attack_signature) TYPE bloom_filter(0.01) GRANULARITY 2;

-- 时间模式分析索引
ALTER TABLE nginx_analytics.dwd_nginx_enriched_v3
ADD INDEX IF NOT EXISTS idx_time_pattern (time_period, business_hours_type) TYPE bloom_filter(0.01) GRANULARITY 1,
ADD INDEX IF NOT EXISTS idx_traffic_pattern (traffic_pattern, is_weekend) TYPE set(0) GRANULARITY 2;

-- 用户分析索引
ALTER TABLE nginx_analytics.dwd_nginx_enriched_v3
ADD INDEX IF NOT EXISTS idx_user_analysis (user_type, user_tier) TYPE bloom_filter(0.01) GRANULARITY 1,
ADD INDEX IF NOT EXISTS idx_user_journey (user_journey_stage, user_session_stage) TYPE bloom_filter(0.01) GRANULARITY 1;

-- 链路追踪索引
ALTER TABLE nginx_analytics.dwd_nginx_enriched_v3
ADD INDEX IF NOT EXISTS idx_trace_id (trace_id) TYPE bloom_filter(0.001) GRANULARITY 1,
ADD INDEX IF NOT EXISTS idx_correlation_id (correlation_id) TYPE bloom_filter(0.001) GRANULARITY 1;

-- =====================================================
-- 投影索引设计 - 预聚合优化
-- =====================================================

-- 平台入口性能投影
ALTER TABLE nginx_analytics.dwd_nginx_enriched_v3 
ADD PROJECTION IF NOT EXISTS proj_platform_performance (
    SELECT 
        tenant_code,
        platform,
        access_entry_point,
        api_category,
        toStartOfHour(log_time) as hour,
        count() as requests,
        avg(total_request_duration) as avg_response_time,
        quantile(0.95)(total_request_duration) as p95_response_time,
        countIf(is_success) as success_count,
        countIf(is_error) as error_count
    GROUP BY tenant_code, platform, access_entry_point, api_category, hour
);

-- 错误分析投影
ALTER TABLE nginx_analytics.dwd_nginx_enriched_v3
ADD PROJECTION IF NOT EXISTS proj_error_analysis (
    SELECT
        tenant_code,
        error_code_group,
        error_severity_level,
        business_operation_type,
        toStartOfHour(log_time) as hour,
        count() as error_count,
        uniq(client_ip) as unique_ips,
        uniq(trace_id) as unique_traces
    GROUP BY tenant_code, error_code_group, error_severity_level, business_operation_type, hour
);

-- 业务域分析投影  
ALTER TABLE nginx_analytics.dwd_nginx_enriched_v3
ADD PROJECTION IF NOT EXISTS proj_business_domain (
    SELECT
        tenant_code,
        business_domain,
        api_category,
        business_operation_type,
        toStartOfHour(log_time) as hour,
        count() as requests,
        avg(total_request_duration) as avg_response_time,
        sum(business_value_score) as total_business_value
    GROUP BY tenant_code, business_domain, api_category, business_operation_type, hour
);

-- =====================================================
-- 表注释和文档
-- =====================================================
-- DWD增强层v3.0-全维度业务分析表，支持平台入口下钻、错误链路分析、权限控制、安全监控等多维度业务需求
-- 核心特性：1)多租户权限隔离 2)平台入口精细化分析 3)错误传播路径追踪 4)业务流程阶段监控 5)安全威胁检测 6)性能瓶颈定位  
-- 兼容Self系统全部功能需求

-- 字段分组说明：
-- 权限控制维度: tenant_code, team_code, environment, data_sensitivity等
-- 平台入口维度: platform, access_entry_point, client_channel, traffic_source等  
-- API业务维度: api_category, business_domain, business_operation_type等
-- 错误分析维度: error_code_group, error_severity_level, error_propagation_path等
-- 性能分析维度: total_request_duration, performance_score, user_experience_level等
-- 安全分析维度: security_risk_score, threat_category, ip_reputation等
-- 用户分析维度: user_type, user_journey_stage, authentication_method等
-- 时间分析维度: time_period, business_hours_type, traffic_pattern等