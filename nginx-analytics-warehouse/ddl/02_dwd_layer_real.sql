-- ==========================================
-- 基于现有DWD设计的改进版 - 面向业务分析
-- 参考: schema_design_v2_fixed.sql 中的 dwd_nginx_enriched
-- ==========================================

-- DWD层：在现有基础上增强业务维度
CREATE TABLE IF NOT EXISTS nginx_analytics.dwd_nginx_enriched_v2 (
    id UInt64, -- 主键ID
    ods_id UInt64, -- ODS层关联ID
    log_time DateTime64(3) CODEC(Delta, ZSTD(1)), -- 日志时间
    date_partition Date, -- 日期分区
    hour_partition UInt8, -- 小时分区
    minute_partition UInt8, -- 分钟分区
    second_partition UInt8, -- 秒分区
    
    client_ip String CODEC(ZSTD(1)), -- 客户端IP
    client_port UInt32, -- 客户端端口
    xff_ip String CODEC(ZSTD(1)), -- X-Forwarded-For IP
    server_name LowCardinality(String), -- 服务器名称
    request_method LowCardinality(String), -- 请求方法
    request_uri String CODEC(ZSTD(1)), -- 请求URI
    request_uri_normalized String CODEC(ZSTD(1)), -- 标准化请求URI
    request_full_uri String CODEC(ZSTD(1)), -- 完整请求URI
    query_parameters String CODEC(ZSTD(1)), -- 查询参数
    http_protocol_version LowCardinality(String), -- HTTP协议版本
    
    response_status_code LowCardinality(String), -- 响应状态码
    response_body_size UInt64, -- 响应体大小
    response_body_size_kb Float64, -- 响应体大小KB
    total_bytes_sent UInt64, -- 总发送字节数
    total_bytes_sent_kb Float64, -- 总发送字节数KB
    total_request_duration Float64, -- 总请求时长
    upstream_connect_time Float64, -- 上游连接时间
    upstream_header_time Float64, -- 上游头部时间
    upstream_response_time Float64, -- 上游响应时间
    backend_connect_phase Float64, -- 后端连接阶段
    backend_process_phase Float64, -- 后端处理阶段
    backend_transfer_phase Float64, -- 后端传输阶段
    nginx_transfer_phase Float64, -- Nginx传输阶段
    backend_total_phase Float64, -- 后端总阶段
    network_phase Float64, -- 网络阶段
    processing_phase Float64, -- 处理阶段
    transfer_phase Float64, -- 传输阶段
    response_transfer_speed Float64, -- 响应传输速度
    total_transfer_speed Float64, -- 总传输速度
    nginx_transfer_speed Float64, -- Nginx传输速度
    backend_efficiency Float64, -- 后端效率
    network_overhead Float64, -- 网络开销
    transfer_ratio Float64, -- 传输比率
    connection_cost_ratio Float64, -- 连接成本比率
    processing_efficiency_index Float64, -- 处理效率指数
    
    platform LowCardinality(String), -- 平台类型Android/iOS/HarmonyOS/Web等
    platform_version String CODEC(ZSTD(1)), -- 平台版本
    app_version String CODEC(ZSTD(1)), -- 应用版本
    device_type LowCardinality(String), -- 设备类型Mobile/Tablet/Desktop/Bot
    browser_type LowCardinality(String), -- 浏览器类型Chrome/Safari/WebView等
    os_type LowCardinality(String), -- 操作系统iOS/Android/HarmonyOS/Windows
    os_version String CODEC(ZSTD(1)), -- 系统版本
    
    sdk_type LowCardinality(String), -- SDK类型WST-SDK-iOS/WST-SDK-ANDROID等
    sdk_version String CODEC(ZSTD(1)), -- SDK版本
    
    bot_type LowCardinality(String), -- 机器人类型
    entry_source LowCardinality(String), -- 入口来源Direct/Search/Social/External/Internal
    referer_domain String CODEC(ZSTD(1)), -- 来源域名
    search_engine LowCardinality(String), -- 搜索引擎
    social_media LowCardinality(String), -- 社交媒体
    
    api_category LowCardinality(String), -- API业务分类
    api_module LowCardinality(String), -- 功能模块gxrz/zgt/search/calendar等
    api_version LowCardinality(String), -- API版本rest/v1/v2等
    business_domain LowCardinality(String), -- 业务域用户中心/搜索/日历等
    
    access_type LowCardinality(String), -- 接入方式APP_Native/H5_WebView/Browser等
    client_category LowCardinality(String), -- 客户端分类Mobile_App/Desktop_Web等
    
    application_name LowCardinality(String), -- 应用名称
    service_name LowCardinality(String), -- 服务名称
    trace_id String CODEC(ZSTD(1)), -- 链路跟踪ID
    business_sign LowCardinality(String), -- 业务标识
    cluster_node LowCardinality(String), -- 集群节点
    upstream_server String CODEC(ZSTD(1)), -- 上游服务器
    connection_requests UInt32, -- 连接请求数
    cache_status LowCardinality(String), -- 缓存状态
    
    referer_url String CODEC(ZSTD(1)), -- 来源URL
    user_agent_string String CODEC(ZSTD(1)), -- 用户代理字符串
    log_source_file LowCardinality(String), -- 日志源文件
    
    is_success Bool, -- 是否成功HTTP 2xx
    is_business_success Bool, -- 业务逻辑成功
    is_slow Bool, -- 是否慢请求>3s
    is_very_slow Bool, -- 是否超慢请求>10s
    is_error Bool, -- 是否错误4xx/5xx
    is_client_error Bool, -- 是否客户端错误4xx
    is_server_error Bool, -- 是否服务端错误5xx
    has_anomaly Bool, -- 是否有异常
    anomaly_type LowCardinality(String), -- 异常类型
    
    user_experience_level LowCardinality(String), -- 用户体验分级Excellent/Good/Fair/Poor/Unacceptable
    apdex_classification LowCardinality(String), -- Apdex分类Satisfied/Tolerating/Frustrated
    
    api_importance LowCardinality(String), -- API重要性Critical/High/Medium/Low
    business_value_score UInt8 DEFAULT 5, -- 业务价值评分1-10
    
    data_quality_score Float64, -- 数据质量评分
    parsing_errors Array(String), -- 解析错误记录
    
    client_region LowCardinality(String), -- 客户端地区
    client_isp LowCardinality(String), -- 客户端ISP
    ip_risk_level LowCardinality(String), -- IP风险等级
    is_internal_ip Bool, -- 是否内网IP
    
    date Date MATERIALIZED toDate(log_time), -- 日期
    hour UInt8 MATERIALIZED toHour(log_time), -- 小时
    minute UInt8 MATERIALIZED toMinute(log_time), -- 分钟
    second UInt8 MATERIALIZED toSecond(log_time), -- 秒
    date_hour String MATERIALIZED concat(toString(date), '_', toString(hour)), -- 日期小时
    date_hour_minute String MATERIALIZED concat(toString(date), '_', toString(hour), '_', toString(minute)), -- 日期小时分钟
    weekday UInt8 MATERIALIZED toDayOfWeek(log_time), -- 星期几
    is_weekend Bool MATERIALIZED weekday IN (6, 7), -- 是否周末
    time_period LowCardinality(String) MATERIALIZED -- 时间段分类
        multiIf(
            hour < 6, 'Dawn',           -- 凌晨 0-6
            hour < 12, 'Morning',       -- 上午 6-12  
            hour < 14, 'Noon',          -- 中午 12-14
            hour < 18, 'Afternoon',     -- 下午 14-18
            hour < 22, 'Evening',       -- 晚上 18-22
            'Night'                     -- 夜间 22-24
        ),
    
    created_at DateTime DEFAULT now(), -- 创建时间
    updated_at DateTime DEFAULT now() -- 更新时间
) ENGINE = MergeTree()
PARTITION BY (date_partition, platform)              -- 按日期+平台双分区
ORDER BY (date_partition, platform, api_category, hour_partition, log_time)
SETTINGS index_granularity = 8192;

-- 注意：高性能索引和投影需要在表创建完成后单独执行
-- 可以使用单独的脚本或手动执行以下命令来添加索引优化：

-- ===== 高性能索引设计（需要单独执行）=====
-- ALTER TABLE nginx_analytics.dwd_nginx_enriched_v2 ADD INDEX idx_platform (platform) TYPE set(0) GRANULARITY 3;
-- ALTER TABLE nginx_analytics.dwd_nginx_enriched_v2 ADD INDEX idx_api_category (api_category) TYPE set(0) GRANULARITY 3;
-- ALTER TABLE nginx_analytics.dwd_nginx_enriched_v2 ADD INDEX idx_response_time (total_request_duration) TYPE minmax GRANULARITY 4;

-- ===== 投影索引（需要单独执行）=====
-- ALTER TABLE nginx_analytics.dwd_nginx_enriched_v2 ADD PROJECTION proj_platform_hourly_performance (...);
-- ALTER TABLE nginx_analytics.dwd_nginx_enriched_v2 ADD PROJECTION proj_api_module_stats (...);

-- 表注释：DWD明细层-业务强化版，支持多平台多维度分析，保持Self功能兼容性