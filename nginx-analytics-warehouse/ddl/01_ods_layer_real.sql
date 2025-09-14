-- ==========================================
-- 增强型ODS层设计 v2.0 - 支持全维度分析
-- 参考: sample_nginx_logs\2025-04-23\access186.log
-- 新增平台入口下钻、权限控制、安全分析等维度支持
-- ==========================================

-- 创建数据库
CREATE DATABASE IF NOT EXISTS nginx_analytics;

-- ODS原始日志表 - 增强版支持更丰富的分析维度
CREATE TABLE IF NOT EXISTS nginx_analytics.ods_nginx_raw (
    id UInt64,                                    -- ID主键
    log_time DateTime64(3) CODEC(Delta, ZSTD(1)), -- 日志时间
    
    -- 服务端信息
    server_name LowCardinality(String),           -- 服务器名称(http_host)
    server_port UInt16,                           -- 服务器端口
    server_protocol LowCardinality(String),       -- 服务器协议(HTTP/HTTPS)
    load_balancer_node LowCardinality(String),    -- 负载均衡节点
    edge_location LowCardinality(String),         -- 边缘节点位置
    
    -- 客户端信息
    client_ip String CODEC(ZSTD(1)),              -- 客户端IP(remote_addr)
    client_port UInt32,                           -- 客户端端口(remote_port)
    xff_ip String CODEC(ZSTD(1)),                 -- 真实IP(RealIp/X-Forwarded-For)
    client_real_ip String CODEC(ZSTD(1)),         -- 客户端真实IP
    forwarded_proto LowCardinality(String),       -- 转发协议
    forwarded_host String CODEC(ZSTD(1)),         -- 转发主机
    remote_user String CODEC(ZSTD(1)),            -- 远程用户(remote_user)
    
    -- HTTP请求信息
    request_method LowCardinality(String),        -- 请求方法(从request字段解析)
    request_uri String CODEC(ZSTD(1)),            -- 请求URI(从request字段解析)
    request_full_uri String CODEC(ZSTD(1)),       -- 完整请求URI
    request_path String CODEC(ZSTD(1)),           -- 请求路径(不含参数)
    query_string String CODEC(ZSTD(1)),           -- 查询字符串
    query_params_count UInt16,                    -- 查询参数个数
    request_body_size UInt64,                     -- 请求体大小
    content_type String CODEC(ZSTD(1)),           -- 内容类型
    content_encoding String CODEC(ZSTD(1)),       -- 内容编码
    accept_language String CODEC(ZSTD(1)),        -- 接受语言
    accept_encoding String CODEC(ZSTD(1)),        -- 接受编码
    http_protocol LowCardinality(String),         -- HTTP协议版本
    
    -- HTTP响应信息  
    response_status_code LowCardinality(String),  -- 响应状态码
    response_body_size UInt64,                    -- 响应体大小
    response_content_type String CODEC(ZSTD(1)),  -- 响应内容类型
    response_content_encoding String CODEC(ZSTD(1)), -- 响应内容编码
    response_cache_control String CODEC(ZSTD(1)), -- 缓存控制头
    response_etag String CODEC(ZSTD(1)),          -- ETag头
    
    -- 请求头信息
    response_referer String CODEC(ZSTD(1)),       -- 来源页面(Referer)
    user_agent String CODEC(ZSTD(1)),             -- 用户代理
    authorization_type LowCardinality(String),    -- 认证类型
    custom_headers Map(String, String),           -- 自定义请求头
    cookie_count UInt16,                          -- Cookie数量
    
    -- 性能指标
    upstream_addr String CODEC(ZSTD(1)),          -- 上游服务地址
    upstream_connect_time Float64,                -- 上游连接时间
    upstream_header_time Float64,                 -- 上游响应头时间
    upstream_response_time Float64,               -- 上游响应时间
    total_request_time Float64,                   -- 总请求时间(ar_time主要性能指标)
    request_processing_time Float64,              -- 请求处理时间
    response_send_time Float64,                   -- 响应发送时间
    total_bytes_sent UInt64,                      -- 发送字节总数
    bytes_received UInt64,                        -- 接收字节数
    
    -- 连接信息
    connection_requests UInt32,                   -- 连接请求数
    connection_id String CODEC(ZSTD(1)),          -- 连接ID
    ssl_protocol LowCardinality(String),          -- SSL协议版本
    ssl_cipher String CODEC(ZSTD(1)),             -- SSL加密套件
    ssl_session_reused Bool,                      -- SSL会话重用
    
    -- 业务标识
    trace_id String CODEC(ZSTD(1)),               -- 跟踪ID
    span_id String CODEC(ZSTD(1)),                -- Span ID
    correlation_id String CODEC(ZSTD(1)),         -- 关联ID
    request_id String CODEC(ZSTD(1)),             -- 请求ID
    session_id String CODEC(ZSTD(1)),             -- 会话ID
    user_id String CODEC(ZSTD(1)),                -- 用户ID
    business_sign LowCardinality(String),         -- 业务标识
    transaction_id String CODEC(ZSTD(1)),         -- 事务ID
    
    -- 应用信息
    application_name LowCardinality(String),      -- 应用名称
    application_version String CODEC(ZSTD(1)),    -- 应用版本
    service_name LowCardinality(String),          -- 服务名称
    service_version String CODEC(ZSTD(1)),        -- 服务版本
    api_version String CODEC(ZSTD(1)),            -- API版本
    environment LowCardinality(String),           -- 环境(dev/test/staging/prod)
    
    -- 缓存信息
    cache_status LowCardinality(String),          -- 缓存状态(HIT/MISS/BYPASS)
    cache_key String CODEC(ZSTD(1)),              -- 缓存键
    cache_age UInt32,                             -- 缓存年龄
    cache_control String CODEC(ZSTD(1)),          -- 缓存控制
    
    -- 基础设施信息
    cluster_node LowCardinality(String),          -- 集群节点
    datacenter LowCardinality(String),            -- 数据中心
    availability_zone LowCardinality(String),     -- 可用区
    instance_id String CODEC(ZSTD(1)),            -- 实例ID
    pod_name String CODEC(ZSTD(1)),               -- Pod名称(K8s)
    container_id String CODEC(ZSTD(1)),           -- 容器ID
    
    -- 日志元信息
    log_source_file LowCardinality(String),       -- 日志源文件
    log_format_version LowCardinality(String),    -- 日志格式版本
    log_level LowCardinality(String),             -- 日志级别
    raw_log_line String CODEC(ZSTD(1)),           -- 原始日志行
    
    -- 地理信息(可选，如果有GeoIP解析)
    client_country LowCardinality(String),        -- 客户端国家
    client_region LowCardinality(String),         -- 客户端地区
    client_city LowCardinality(String),           -- 客户端城市
    client_isp LowCardinality(String),            -- 客户端ISP
    client_org LowCardinality(String),            -- 客户端组织
    
    -- 安全相关
    security_headers Map(String, String),         -- 安全相关头信息
    rate_limit_remaining UInt32,                  -- 剩余请求配额
    rate_limit_reset UInt32,                      -- 配额重置时间
    blocked_reason String CODEC(ZSTD(1)),         -- 被阻断原因
    
    -- 系统字段
    created_at DateTime DEFAULT now(),            -- 创建时间
    updated_at DateTime DEFAULT now(),            -- 更新时间
    data_version UInt16 DEFAULT 1,                -- 数据版本
    
    -- 分区字段
    date_partition Date MATERIALIZED toDate(log_time), -- 日期分区
    hour_partition UInt8 MATERIALIZED toHour(log_time), -- 小时分区
    minute_partition UInt8 MATERIALIZED toMinute(log_time) -- 分钟分区
) ENGINE = MergeTree()
PARTITION BY (date_partition, environment)  -- 按日期和环境双分区
ORDER BY (date_partition, hour_partition, server_name, client_ip, log_time)
SETTINGS index_granularity = 8192;

-- 样例数据格式参考:
-- http_host:zgtapp.zwfw.gxzf.gov.cn remote_addr:"100.100.8.44" remote_port:"10305" 
-- remote_user:"-" time:"2025-04-23T00:00:02+08:00" 
-- request:"GET /group1/M00/06/B3/rBAWN2f-ZIKAJI2vAAIkLKrgt-I560.png HTTP/1.1" 
-- code:"200" body:"140332" http_referer:"-" ar_time:"0.325" RealIp:"100.100.8.44" 
-- agent:"zgt-ios/1.4.1 (iPhone; iOS 15.4.1; Scale/3.00)"

-- 表注释：Nginx原始日志ODS层，基于实际日志格式设计，保持数据完整性