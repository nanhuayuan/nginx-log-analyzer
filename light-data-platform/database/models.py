# -*- coding: utf-8 -*-
"""
数据库模型定义
"""

from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text, Boolean, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os

Base = declarative_base()

# ===============================
# ODS层: 原始数据存储
# ===============================

class OdsNginxLog(Base):
    """ODS层 - 原始nginx日志数据"""
    __tablename__ = 'ods_nginx_log'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # 原始字段
    timestamp = Column(DateTime, nullable=False, index=True)
    client_ip = Column(String(45), index=True)  # 支持IPv6
    request_method = Column(String(10))
    request_full_uri = Column(Text, index=True)
    request_protocol = Column(String(20))
    response_status_code = Column(String(10), index=True)
    response_body_size_kb = Column(Float)
    total_bytes_sent_kb = Column(Float)
    referer = Column(Text)
    user_agent = Column(Text, index=True)
    
    # 性能指标
    total_request_duration = Column(Float, index=True)
    upstream_response_time = Column(Float)
    upstream_connect_time = Column(Float)
    upstream_header_time = Column(Float)
    
    # 扩展字段
    application_name = Column(String(100), index=True)
    service_name = Column(String(100), index=True)
    
    # 元数据
    source_file = Column(String(255))
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # 索引
    __table_args__ = (
        Index('idx_timestamp_status', 'timestamp', 'response_status_code'),
        Index('idx_uri_timestamp', 'request_full_uri', 'timestamp'),
    )

# ===============================
# DWD层: 数据仓库明细层
# ===============================

class DwdNginxEnriched(Base):
    """DWD层 - 清洗和标签化后的数据"""
    __tablename__ = 'dwd_nginx_enriched'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    ods_id = Column(Integer, index=True)  # 关联ODS记录
    
    # 基础字段(清洗后)
    timestamp = Column(DateTime, nullable=False, index=True)
    date_partition = Column(String(10), index=True)  # YYYY-MM-DD
    hour_partition = Column(Integer, index=True)     # 0-23
    
    client_ip = Column(String(45), index=True)
    request_uri = Column(String(500), index=True)  # 清洗后的URI
    request_method = Column(String(10))
    response_status_code = Column(String(10), index=True)
    response_time = Column(Float, index=True)
    response_size_kb = Column(Float)
    
    # 维度标签
    platform = Column(String(50), index=True)       # iOS_SDK, Android_SDK等
    platform_version = Column(String(50), index=True)  # 应用版本
    entry_source = Column(String(50), index=True)   # Internal, External等
    api_category = Column(String(50), index=True)   # User_Auth, Business_Core等
    
    # 业务标签
    application_name = Column(String(100), index=True)
    service_name = Column(String(100), index=True)
    is_success = Column(Boolean, index=True)        # 是否成功请求
    is_slow = Column(Boolean, index=True)           # 是否慢请求
    
    # 数据质量标记
    data_quality_score = Column(Float, default=1.0) # 数据质量评分
    has_anomaly = Column(Boolean, default=False)    # 异常标记
    anomaly_type = Column(String(100))              # 异常类型
    
    # 元数据
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 索引
    __table_args__ = (
        Index('idx_date_platform', 'date_partition', 'platform'),
        Index('idx_uri_platform', 'request_uri', 'platform'),
        Index('idx_time_quality', 'timestamp', 'data_quality_score'),
    )

# ===============================
# DWS层: 数据仓库汇总层  
# ===============================

class DwsPlatformHourly(Base):
    """DWS层 - 平台维度小时级聚合"""
    __tablename__ = 'dws_platform_hourly'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # 分区字段
    date_partition = Column(String(10), nullable=False, index=True)
    hour_partition = Column(Integer, nullable=False, index=True)
    platform = Column(String(50), nullable=False, index=True)
    
    # 聚合指标
    total_requests = Column(Integer, default=0)
    success_requests = Column(Integer, default=0)  
    error_requests = Column(Integer, default=0)
    slow_requests = Column(Integer, default=0)
    
    # 响应时间统计
    avg_response_time = Column(Float, default=0.0)
    p50_response_time = Column(Float, default=0.0)
    p95_response_time = Column(Float, default=0.0)
    p99_response_time = Column(Float, default=0.0)
    max_response_time = Column(Float, default=0.0)
    
    # 计算指标
    success_rate = Column(Float, default=0.0)       # 成功率
    error_rate = Column(Float, default=0.0)         # 错误率
    slow_rate = Column(Float, default=0.0)          # 慢请求率
    
    # 流量统计
    unique_ips = Column(Integer, default=0)
    total_response_size_mb = Column(Float, default=0.0)
    
    # 元数据
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 唯一索引
    __table_args__ = (
        Index('idx_unique_platform_hour', 'date_partition', 'hour_partition', 'platform', unique=True),
    )

class DwsApiHourly(Base):
    """DWS层 - API维度小时级聚合"""
    __tablename__ = 'dws_api_hourly'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # 分区字段
    date_partition = Column(String(10), nullable=False, index=True)
    hour_partition = Column(Integer, nullable=False, index=True)
    request_uri = Column(String(500), nullable=False, index=True)
    platform = Column(String(50), nullable=False, index=True)
    
    # 聚合指标
    total_requests = Column(Integer, default=0)
    success_requests = Column(Integer, default=0)
    error_requests = Column(Integer, default=0) 
    slow_requests = Column(Integer, default=0)
    
    # 响应时间统计
    avg_response_time = Column(Float, default=0.0)
    p95_response_time = Column(Float, default=0.0)
    max_response_time = Column(Float, default=0.0)
    
    # 计算指标
    success_rate = Column(Float, default=0.0)
    error_rate = Column(Float, default=0.0)
    slow_rate = Column(Float, default=0.0)
    
    # 元数据
    api_category = Column(String(50), index=True)
    application_name = Column(String(100))
    service_name = Column(String(100))
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 唯一索引
    __table_args__ = (
        Index('idx_unique_api_hour', 'date_partition', 'hour_partition', 'request_uri', 'platform', unique=True),
    )

# ===============================
# ADS层: 应用数据服务层
# ===============================

class AdsAnomalyLog(Base):
    """ADS层 - 异常检测日志"""
    __tablename__ = 'ads_anomaly_log'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # 异常基本信息
    anomaly_time = Column(DateTime, nullable=False, index=True)
    anomaly_type = Column(String(50), nullable=False, index=True)  # error_rate, response_time, traffic
    severity = Column(String(20), index=True)  # low, medium, high, critical
    
    # 异常维度
    platform = Column(String(50), index=True)
    request_uri = Column(String(500), index=True)
    
    # 异常详情
    current_value = Column(Float)
    baseline_value = Column(Float)
    deviation_ratio = Column(Float)  # 偏离比例
    
    # 描述和建议
    description = Column(Text)
    suggestion = Column(Text)
    
    # 状态管理
    status = Column(String(20), default='open', index=True)  # open, investigating, resolved
    resolved_at = Column(DateTime)
    resolved_by = Column(String(100))
    
    created_at = Column(DateTime, default=datetime.utcnow)

class AdsDashboardMetric(Base):
    """ADS层 - 仪表板指标"""
    __tablename__ = 'ads_dashboard_metric'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # 指标标识
    metric_date = Column(String(10), nullable=False, index=True)  # YYYY-MM-DD
    metric_name = Column(String(100), nullable=False, index=True)
    dimension_value = Column(String(100), index=True)  # 维度值，如iOS_SDK
    
    # 指标值
    metric_value = Column(Float)
    metric_target = Column(Float)  # 目标值
    
    # 对比指标
    previous_value = Column(Float)   # 昨日同期
    change_ratio = Column(Float)     # 变化比例
    
    # 元数据
    metric_unit = Column(String(20))  # 单位: %, ms, count等
    description = Column(Text)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # 唯一索引
    __table_args__ = (
        Index('idx_unique_dashboard_metric', 'metric_date', 'metric_name', 'dimension_value', unique=True),
    )

# ===============================
# 数据库初始化
# ===============================

def init_db(database_path='database/nginx_analytics.db'):
    """初始化数据库"""
    
    # 确保数据库目录存在
    os.makedirs(os.path.dirname(database_path), exist_ok=True)
    
    # 创建数据库引擎
    engine = create_engine(f'sqlite:///{database_path}', echo=False)
    
    # 创建所有表
    Base.metadata.create_all(engine)
    
    # 创建Session类
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    
    print(f"数据库初始化完成: {database_path}")
    print("创建的表:")
    for table in Base.metadata.tables:
        print(f"  - {table}")
    
    return engine, SessionLocal

def get_session(database_path='database/nginx_analytics.db'):
    """获取数据库会话"""
    engine = create_engine(f'sqlite:///{database_path}', echo=False)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal()

if __name__ == "__main__":
    # 测试数据库初始化
    init_db()