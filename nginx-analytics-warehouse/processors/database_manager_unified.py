#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一数据库管理器 - 基于v1.1架构设计
Unified Database Manager - Based on v1.1 Architecture Design

统一管理：
1. 表创建和结构管理
2. 物化视图创建和管理  
3. 数据写入协调
4. 架构验证和监控
5. DDL版本控制
"""

import logging
import time
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path
import clickhouse_connect
from clickhouse_connect.driver.exceptions import ClickHouseError

class DatabaseManagerUnified:
    """统一数据库管理器 - 整合表、视图、数据写入管理"""
    
    def __init__(self, host: str = 'localhost', port: int = 8123, 
                 database: str = 'nginx_analytics', user: str = 'analytics_user', 
                 password: str = 'analytics_password_change_in_prod'):
        """
        初始化统一数据库管理器
        
        Args:
            host: ClickHouse服务器地址
            port: ClickHouse HTTP端口
            database: 数据库名称
            user: 用户名
            password: 密码
        """
        self.config = {
            'host': host,
            'port': port,
            'username': user,
            'password': password,
            'database': database
        }
        self.database = database
        self.client = None
        self.logger = logging.getLogger(__name__)
        
        # DDL文件路径配置
        self.ddl_dir = Path(__file__).parent.parent / "ddl"
        
        # 17个完整物化视图配置 - v5.0增强版支持全维度分析
        self.materialized_views = self._initialize_view_definitions()
        
    def _initialize_view_definitions(self) -> Dict[str, Dict[str, Any]]:
        """
        初始化17个物化视图定义 - v5.0增强版支持全维度分析
        
        Returns:
            Dict: 物化视图配置字典
        """
        return {
            # 核心物化视图 (1-13) - 升级版本支持v3表结构
            'mv_api_performance_hourly_v3': {
                'target_table': 'ads_api_performance_analysis_v3',
                'description': '01.接口性能分析v3 - 支持平台入口下钻、租户权限隔离',
                'priority': 1,
                'dependencies': ['dwd_nginx_enriched_v3']
            },
            'mv_service_level_hourly_v3': {
                'target_table': 'ads_service_level_analysis_v3',
                'description': '02.服务层级分析v3 - 支持微服务架构、多环境监控',
                'priority': 2,
                'dependencies': ['dwd_nginx_enriched_v3']
            },
            'mv_slow_request_hourly': {
                'target_table': 'ads_slow_request_analysis',
                'description': '03.慢请求分析 - 支持瓶颈类型和根因定位',
                'priority': 2,
                'dependencies': ['dwd_nginx_enriched_v3']
            },
            'mv_status_code_hourly': {
                'target_table': 'ads_status_code_analysis',
                'description': '04.状态码统计 - 支持错误分类和影响评估',
                'priority': 2,
                'dependencies': ['dwd_nginx_enriched_v3']
            },
            'mv_time_dimension_hourly': {
                'target_table': 'ads_time_dimension_analysis',
                'description': '05.时间维度分析 - 支持QPS趋势和性能监控',
                'priority': 1,
                'dependencies': ['dwd_nginx_enriched_v3']
            },
            'mv_error_analysis_hourly': {
                'target_table': 'ads_error_analysis_detailed_v3',
                'description': '06.错误码下钻分析v3 - 支持错误链路追踪、多维度错误分析',
                'priority': 1,
                'dependencies': ['dwd_nginx_enriched_v3']
            },
            'mv_request_header_hourly': {
                'target_table': 'ads_request_header_analysis',
                'description': '07.请求头分析 - 支持客户端行为和用户体验分析',
                'priority': 3,
                'dependencies': ['dwd_nginx_enriched_v3']
            },
            'mv_api_error_analysis_hourly': {
                'target_table': 'ads_api_error_analysis',
                'description': '08.API错误分析 - 错误类型分类与影响分析',
                'priority': 2,
                'dependencies': ['dwd_nginx_enriched_v3']
            },
            'mv_ip_source_analysis_hourly': {
                'target_table': 'ads_ip_source_analysis',
                'description': '09.IP来源分析 - 风险评分与异常行为检测',
                'priority': 3,
                'dependencies': ['dwd_nginx_enriched_v3']
            },
            'mv_service_stability_analysis_hourly': {
                'target_table': 'ads_service_stability_analysis',
                'description': '10.服务稳定性分析 - SLA计算与稳定性评级',
                'priority': 1,
                'dependencies': ['dwd_nginx_enriched_v3']
            },
            'mv_header_performance_correlation_hourly': {
                'target_table': 'ads_header_performance_correlation',
                'description': '11.请求头性能关联分析 - 用户代理与性能关联',
                'priority': 3,
                'dependencies': ['dwd_nginx_enriched_v3']
            },
            'mv_comprehensive_report_hourly': {
                'target_table': 'ads_comprehensive_report',
                'description': '12.综合报告 - 系统健康评分与容量分析',
                'priority': 1,
                'dependencies': ['dwd_nginx_enriched_v3']
            },
            
            # 新增业务主题物化视图 (13-17) - v5.0新特性
            'mv_platform_entry_analysis_hourly': {
                'target_table': 'ads_platform_entry_analysis',
                'description': '13.平台入口下钻分析 - 核心下钻维度，支持平台+入口组合分析',
                'priority': 1,
                'dependencies': ['dwd_nginx_enriched_v3']
            },
            'mv_business_process_analysis_hourly': {
                'target_table': 'ads_business_process_analysis',
                'description': '14.业务流程分析 - 业务流程监控、用户旅程追踪',
                'priority': 2,
                'dependencies': ['dwd_nginx_enriched_v3']
            },
            'mv_user_behavior_analysis_hourly': {
                'target_table': 'ads_user_behavior_analysis',
                'description': '15.用户行为分析 - 用户旅程、行为模式、转化分析',
                'priority': 2,
                'dependencies': ['dwd_nginx_enriched_v3']
            },
            'mv_security_monitoring_analysis_hourly': {
                'target_table': 'ads_security_monitoring_analysis',
                'description': '16.安全监控分析 - 安全威胁检测、风险评估、攻击分析',
                'priority': 1,
                'dependencies': ['dwd_nginx_enriched_v3']
            },
            'mv_tenant_permission_analysis_hourly': {
                'target_table': 'ads_tenant_permission_analysis',
                'description': '17.租户权限分析 - 多租户权限监控、合规性分析',
                'priority': 1,
                'dependencies': ['dwd_nginx_enriched_v3']
            }
        }
    
    def connect(self) -> bool:
        """连接到ClickHouse"""
        try:
            self.client = clickhouse_connect.get_client(**self.config)
            self.client.ping()
            self.logger.info(f"成功连接到ClickHouse: {self.config['host']}:{self.config['port']}")
            return True
        except Exception as e:
            self.logger.error(f"连接ClickHouse失败: {str(e)}")
            self.client = None
            return False
    
    def connect_for_rebuild(self) -> bool:
        """为重建操作连接到ClickHouse（使用系统数据库）"""
        try:
            # 创建系统连接配置（连接到default数据库）
            system_config = self.config.copy()
            system_config['database'] = 'default'
            
            self.client = clickhouse_connect.get_client(**system_config)
            self.client.ping()
            self.logger.info(f"成功连接到ClickHouse系统数据库: {system_config['host']}:{system_config['port']}")
            return True
        except Exception as e:
            self.logger.error(f"连接ClickHouse系统数据库失败: {str(e)}")
            self.client = None
            return False
    
    def close(self):
        """关闭ClickHouse连接"""
        if self.client:
            try:
                self.client.close()
                self.logger.info("ClickHouse连接已关闭")
            except Exception as e:
                self.logger.error(f"关闭连接时出错: {str(e)}")
            finally:
                self.client = None
    
    def initialize_complete_architecture(self) -> Dict[str, Any]:
        """
        一键初始化完整数据库架构
        
        Returns:
            Dict: 初始化结果统计
        """
        if not self.connect():
            return {'success': False, 'error': 'ClickHouse连接失败'}
        
        results = {
            'success': True,
            'phases': {},
            'total_duration': 0,
            'errors': []
        }
        
        start_time = time.time()
        
        try:
            # 第一阶段：创建基础表
            self.logger.info("🔧 第一阶段：创建基础表结构")
            phase1_result = self._execute_ddl_phase("基础表创建", [
                "01_ods_layer_real.sql",
                "02_dwd_layer_real.sql", 
                "03_ads_layer_real.sql"
            ])
            results['phases']['基础表创建'] = phase1_result
            
            if not phase1_result['success']:
                results['success'] = False
                results['errors'].extend(phase1_result['errors'])
                return results
            
            # 第二阶段：创建物化视图
            self.logger.info("📊 第二阶段：创建物化视图")
            phase2_result = self._create_all_materialized_views()
            results['phases']['物化视图创建'] = phase2_result
            
            if not phase2_result['success']:
                results['success'] = False
                results['errors'].extend(phase2_result['errors'])
            
            # 第三阶段：验证架构完整性
            self.logger.info("✅ 第三阶段：验证架构完整性")
            phase3_result = self.validate_architecture()
            results['phases']['架构验证'] = phase3_result
            
            results['total_duration'] = time.time() - start_time
            
            # 生成初始化报告
            self._generate_initialization_report(results)
            
            return results
            
        except Exception as e:
            self.logger.error(f"架构初始化失败: {str(e)}")
            results['success'] = False
            results['errors'].append(str(e))
            return results
        finally:
            self.close()
    
    def _execute_ddl_phase(self, phase_name: str, ddl_files: List[str]) -> Dict[str, Any]:
        """
        执行DDL阶段
        
        Args:
            phase_name: 阶段名称
            ddl_files: DDL文件列表
            
        Returns:
            Dict: 阶段执行结果
        """
        result = {
            'success': True,
            'phase_name': phase_name,
            'files_processed': 0,
            'errors': [],
            'duration': 0
        }
        
        start_time = time.time()
        
        for ddl_file in ddl_files:
            file_path = self.ddl_dir / ddl_file
            
            if not file_path.exists():
                error_msg = f"DDL文件不存在: {file_path}"
                self.logger.error(error_msg)
                result['errors'].append(error_msg)
                result['success'] = False
                continue
                
            try:
                self.logger.info(f"执行DDL文件: {ddl_file}")
                with open(file_path, 'r', encoding='utf-8') as f:
                    ddl_content = f.read()
                
                # 分割并执行SQL语句
                statements = self._split_sql_statements(ddl_content)
                for i, statement in enumerate(statements):
                    if statement.strip():
                        try:
                            self.client.command(statement)
                        except Exception as e:
                            error_msg = f"{ddl_file}[语句{i+1}]: {str(e)}"
                            self.logger.error(error_msg)
                            result['errors'].append(error_msg)
                            result['success'] = False
                
                result['files_processed'] += 1
                self.logger.info(f"✅ {ddl_file} 执行完成")
                
            except Exception as e:
                error_msg = f"执行{ddl_file}失败: {str(e)}"
                self.logger.error(error_msg)
                result['errors'].append(error_msg)
                result['success'] = False
        
        result['duration'] = time.time() - start_time
        return result
    
    def _create_all_materialized_views(self) -> Dict[str, Any]:
        """
        创建所有7个物化视图 - 按优先级顺序
        
        Returns:
            Dict: 创建结果统计
        """
        result = {
            'success': True,
            'views_created': 0,
            'views_failed': 0,
            'errors': [],
            'duration': 0
        }
        
        start_time = time.time()
        
        # 使用新的物化视图SQL文件
        mv_sql_file = self.ddl_dir / "04_materialized_views_corrected.sql"
        
        if not mv_sql_file.exists():
            result['success'] = False
            result['errors'].append(f"物化视图SQL文件不存在: {mv_sql_file}")
            return result
        
        try:
            with open(mv_sql_file, 'r', encoding='utf-8') as f:
                mv_sql_content = f.read()
            
            # 分割物化视图创建语句
            statements = self._split_sql_statements(mv_sql_content)
            
            for statement in statements:
                if 'CREATE MATERIALIZED VIEW' in statement:
                    try:
                        # 提取视图名称
                        view_name = self._extract_view_name(statement)
                        self.logger.info(f"创建物化视图: {view_name}")
                        
                        self.client.command(statement)
                        result['views_created'] += 1
                        self.logger.info(f"✅ {view_name} 创建成功")
                        
                    except Exception as e:
                        result['views_failed'] += 1
                        error_msg = f"创建物化视图失败: {str(e)}"
                        self.logger.error(error_msg)
                        result['errors'].append(error_msg)
                        result['success'] = False
        
        except Exception as e:
            result['success'] = False
            result['errors'].append(f"读取物化视图SQL文件失败: {str(e)}")
        
        result['duration'] = time.time() - start_time
        return result
    
    def validate_architecture(self) -> Dict[str, Any]:
        """
        验证架构完整性
        
        Returns:
            Dict: 验证结果
        """
        result = {
            'success': True,
            'tables_validated': 0,
            'views_validated': 0,
            'field_mapping_issues': [],
            'errors': []
        }
        
        try:
            # 验证基础表存在 - v5.0增强版架构
            essential_tables = [
                'ods_nginx_raw',
                'dwd_nginx_enriched_v3',
                'ads_api_performance_analysis_v3',
                'ads_service_level_analysis_v3',
                'ads_slow_request_analysis',
                'ads_status_code_analysis',
                'ads_time_dimension_analysis',
                'ads_error_analysis_detailed_v3',
                'ads_request_header_analysis',
                # v5.0核心新增表
                'ads_platform_entry_analysis',
                'ads_security_monitoring_analysis',
                'ads_tenant_permission_analysis'
            ]
            
            for table in essential_tables:
                if self._table_exists(table):
                    result['tables_validated'] += 1
                else:
                    error_msg = f"关键表不存在: {table}"
                    result['errors'].append(error_msg)
                    result['success'] = False
            
            # 验证物化视图存在
            for view_name in self.materialized_views.keys():
                if self._view_exists(view_name):
                    result['views_validated'] += 1
                else:
                    error_msg = f"物化视图不存在: {view_name}"
                    result['errors'].append(error_msg)
                    result['success'] = False
            
            # 验证字段映射（简化版）
            result['field_mapping_issues'] = self._validate_field_mapping()
            
        except Exception as e:
            result['success'] = False
            result['errors'].append(f"架构验证失败: {str(e)}")
        
        return result
    
    def get_architecture_status(self) -> Dict[str, Any]:
        """
        获取当前架构状态
        
        Returns:
            Dict: 架构状态报告
        """
        if not self.connect():
            return {'success': False, 'error': 'ClickHouse连接失败'}
        
        try:
            status = {
                'database': self.database,
                'connection_status': 'connected',
                'tables': {},
                'materialized_views': {},
                'data_counts': {},
                'architecture_health': 'unknown'
            }
            
            # 检查基础表状态 - v5.0增强版
            for table_name in ['ods_nginx_raw', 'dwd_nginx_enriched_v3']:
                status['tables'][table_name] = {
                    'exists': self._table_exists(table_name),
                    'record_count': self._get_table_count(table_name) if self._table_exists(table_name) else 0
                }
            
            # 检查ADS表状态 - v5.0全量18个主题表
            ads_tables = [
                'ads_api_performance_analysis_v3',
                'ads_service_level_analysis_v3', 
                'ads_slow_request_analysis',
                'ads_status_code_analysis',
                'ads_time_dimension_analysis',
                'ads_error_analysis_detailed_v3',
                'ads_request_header_analysis',
                'ads_api_error_analysis',
                'ads_ip_source_analysis',
                'ads_service_stability_analysis',
                'ads_header_performance_correlation',
                'ads_comprehensive_report',
                # v5.0新增业务主题表
                'ads_platform_entry_analysis',
                'ads_business_process_analysis',
                'ads_user_behavior_analysis',
                'ads_security_monitoring_analysis',
                'ads_tenant_permission_analysis'
            ]
            
            for table_name in ads_tables:
                status['tables'][table_name] = {
                    'exists': self._table_exists(table_name),
                    'record_count': self._get_table_count(table_name) if self._table_exists(table_name) else 0
                }
            
            # 检查物化视图状态
            for view_name, config in self.materialized_views.items():
                target_table = config['target_table']
                status['materialized_views'][view_name] = {
                    'exists': self._view_exists(view_name),
                    'target_table': target_table,
                    'target_records': self._get_table_count(target_table) if self._table_exists(target_table) else 0,
                    'description': config['description']
                }
            
            # 计算架构健康度
            total_tables = len(status['tables'])
            existing_tables = sum(1 for t in status['tables'].values() if t['exists'])
            total_views = len(status['materialized_views'])
            existing_views = sum(1 for v in status['materialized_views'].values() if v['exists'])
            
            if existing_tables == total_tables and existing_views == total_views:
                status['architecture_health'] = 'healthy'
            elif existing_tables >= total_tables * 0.8 and existing_views >= total_views * 0.6:
                status['architecture_health'] = 'partial'
            else:
                status['architecture_health'] = 'degraded'
            
            return status
            
        except Exception as e:
            self.logger.error(f"获取架构状态失败: {str(e)}")
            return {'success': False, 'error': str(e)}
        finally:
            self.close()
    
    def print_architecture_report(self):
        """打印架构状态报告"""
        status = self.get_architecture_status()
        
        if not status.get('success', True):
            print(f"❌ 获取架构状态失败: {status.get('error')}")
            return
        
        print("=" * 80)
        print(f"📊 数据库架构状态报告 - {status['database']}")
        print("=" * 80)
        print(f"连接状态: ✅ {status['connection_status']}")
        print(f"架构健康度: {'✅ ' + status['architecture_health'] if status['architecture_health'] == 'healthy' else '⚠️  ' + status['architecture_health']}")
        
        # 基础表状态
        print(f"\n📋 基础表状态:")
        for table_name, info in status['tables'].items():
            status_icon = "✅" if info['exists'] else "❌"
            record_count = f"{info['record_count']:,} 条" if info['record_count'] > 0 else "无数据"
            print(f"   {status_icon} {table_name}: {record_count}")
        
        # 物化视图状态
        print(f"\n🔄 物化视图状态:")
        for view_name, info in status['materialized_views'].items():
            status_icon = "✅" if info['exists'] else "❌"
            target_records = f"{info['target_records']:,} 条" if info['target_records'] > 0 else "无数据"
            print(f"   {status_icon} {view_name} → {info['target_table']}")
            print(f"      📝 {info['description']}")
            print(f"      📊 目标表数据: {target_records}")
        
        print("=" * 80)
    
    def force_rebuild(self) -> Dict[str, Any]:
        """强制重建整个架构（删除后重新创建）"""
        # 对于重建操作，需要连接到系统数据库
        if not self.connect_for_rebuild():
            return {'success': False, 'message': '数据库连接失败'}
        
        print("⚠️  即将删除整个数据库架构！")
        confirm = input("输入 'YES' 确认删除并重建: ").strip()
        
        if confirm != 'YES':
            return {'success': False, 'message': '用户取消操作'}
        
        try:
            # 删除数据库
            self.logger.info(f"删除数据库: {self.database}")
            self.client.command(f"DROP DATABASE IF EXISTS {self.database}")
            print(f"🗑️  数据库 {self.database} 已删除")
            
            # 重新创建数据库
            self.logger.info(f"创建数据库: {self.database}")
            self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            print(f"🏗️  数据库 {self.database} 已重新创建")
            
            # 断开连接并重新连接到新数据库
            self.close()
            if not self.connect():
                return {'success': False, 'message': '重新连接数据库失败'}
            
            # 重新初始化架构
            return self.initialize_complete_architecture()
            
        except Exception as e:
            error_msg = f"强制重建失败: {str(e)}"
            self.logger.error(error_msg)
            return {'success': False, 'message': error_msg}
    
    def clean_all_data(self) -> Dict[str, Any]:
        """清理所有数据（保留表结构）"""
        if not self.connect():
            return {'success': False, 'message': '数据库连接失败'}
        
        print("⚠️  即将清空所有表数据（保留表结构）！")
        confirm = input("输入 'YES' 确认清空数据: ").strip()
        
        if confirm != 'YES':
            return {'success': False, 'message': '用户取消操作'}
        
        try:
            # 获取所有表
            tables_query = f"SHOW TABLES FROM {self.database}"
            result = self.client.query(tables_query)
            tables = [row[0] for row in result.result_rows]
            
            cleaned_count = 0
            errors = []
            
            for table in tables:
                try:
                    self.client.command(f"TRUNCATE TABLE {self.database}.{table}")
                    cleaned_count += 1
                    print(f"🧹 已清空: {table}")
                except Exception as e:
                    errors.append(f"清空 {table} 失败: {str(e)}")
                    self.logger.error(f"清空表 {table} 失败: {str(e)}")
            
            message = f"数据清理完成: {cleaned_count}/{len(tables)} 个表"
            return {
                'success': len(errors) == 0,
                'message': message,
                'cleaned_tables': cleaned_count,
                'total_tables': len(tables),
                'errors': errors
            }
            
        except Exception as e:
            error_msg = f"清理数据失败: {str(e)}"
            self.logger.error(error_msg)
            return {'success': False, 'message': error_msg}
    
    def interactive_menu(self):
        """交互式菜单"""
        while True:
            print("\n" + "="*80)
            print("🏛️   ClickHouse 统一数据库管理工具 v5.0 - 增强版")
            print("="*80)
            print("1. 🚀 初始化完整架构（创建所有表和物化视图）")
            print("2. 📊 检查架构状态（显示表和视图状态）") 
            print("3. 🔍 验证架构完整性（检查字段映射和数据质量）")
            print("4. 🔄 强制重建架构（删除数据库后重新创建）")
            print("5. 🧹 清理所有数据（保留表结构，清空数据）")
            print("6. 📋 单独执行DDL文件")
            print("7. 🔧 创建单个物化视图")
            print("8. 🔄 从v2架构升级到v5架构（兼容性迁移）")
            print("9. 📋 检查v2和v5架构兼容性状态")
            print("0. 👋 退出")
            print("-"*80)
            
            try:
                choice = input("请选择操作 [0-9]: ").strip()
                
                if choice == '0':
                    print("👋 再见！")
                    break
                elif choice == '1':
                    print("🚀 开始初始化完整架构...")
                    result = self.initialize_complete_architecture()
                    self._print_result(result)
                    
                elif choice == '2':
                    self.print_architecture_report()
                    
                elif choice == '3':
                    print("🔍 验证架构完整性...")
                    if self.connect():
                        result = self.validate_architecture()
                        self._print_result(result)
                        
                elif choice == '4':
                    result = self.force_rebuild()
                    self._print_result(result)
                    
                elif choice == '5':
                    result = self.clean_all_data()
                    self._print_result(result)
                    
                elif choice == '6':
                    self._execute_single_ddl_file()
                    
                elif choice == '7':
                    self._create_single_materialized_view()
                elif choice == '8':
                    self._upgrade_from_v2_to_v5()
                elif choice == '9':
                    self._check_compatibility_status()
                else:
                    print("❌ 无效选择，请重新输入")
                    
                if choice != '0':
                    input("\n按回车键继续...")
                    
            except KeyboardInterrupt:
                print("\n👋 再见！")
                break
            except Exception as e:
                print(f"❌ 操作失败: {e}")
                input("按回车键继续...")
    
    def _execute_single_ddl_file(self):
        """执行单个DDL文件"""
        ddl_files = ['01_ods_layer_real.sql', '02_dwd_layer_real.sql', 
                     '03_ads_layer_real.sql', '04_materialized_views_corrected.sql']
        
        print("\n📄 可用的DDL文件:")
        for i, file in enumerate(ddl_files, 1):
            file_path = self.ddl_dir / file
            status = "✅ 存在" if file_path.exists() else "❌ 不存在"
            print(f"   {i}. {file} ({status})")
        
        try:
            choice = int(input(f"\n选择文件 [1-{len(ddl_files)}]: ").strip())
            if 1 <= choice <= len(ddl_files):
                selected_file = ddl_files[choice - 1]
                
                if not self.connect():
                    print("❌ 数据库连接失败")
                    return
                
                # 确保数据库存在
                self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")
                
                result = self._execute_ddl_phase(f"执行{selected_file}", [selected_file])
                self._print_result(result)
                
            else:
                print("❌ 无效选择")
        except ValueError:
            print("❌ 请输入数字")
    
    def _create_single_materialized_view(self):
        """创建单个物化视图"""
        print("\n🔧 可用的物化视图:")
        views = list(self.materialized_views.keys())
        
        for i, view in enumerate(views, 1):
            config = self.materialized_views[view]
            status = "✅ 运行中" if (self.connect() and self._view_exists(view)) else "❌ 未创建"
            print(f"   {i}. {view} → {config['target_table']} ({status})")
            print(f"      📝 {config['description']}")
        
        try:
            choice = int(input(f"\n选择视图 [1-{len(views)}]: ").strip())
            if 1 <= choice <= len(views):
                selected_view = views[choice - 1]
                config = self.materialized_views[selected_view]
                
                if not self.connect():
                    print("❌ 数据库连接失败")
                    return
                
                try:
                    # 删除已存在的视图
                    self.client.command(f"DROP VIEW IF EXISTS {self.database}.{selected_view}")
                    
                    # 从物化视图SQL文件中提取对应的创建语句
                    mv_sql_file = self.ddl_dir / "04_materialized_views_corrected.sql"
                    if not mv_sql_file.exists():
                        print(f"❌ 物化视图SQL文件不存在: {mv_sql_file}")
                        return
                    
                    with open(mv_sql_file, 'r', encoding='utf-8') as f:
                        mv_sql_content = f.read()
                    
                    # 查找对应的物化视图创建语句
                    statements = self._split_sql_statements(mv_sql_content)
                    view_found = False
                    
                    for statement in statements:
                        if f'CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.{selected_view}' in statement:
                            self.client.command(statement)
                            print(f"✅ 物化视图 {selected_view} 创建成功")
                            view_found = True
                            break
                    
                    if not view_found:
                        print(f"❌ 在SQL文件中未找到物化视图定义: {selected_view}")
                    
                except Exception as e:
                    print(f"❌ 创建物化视图失败: {str(e)}")
                    
            else:
                print("❌ 无效选择")
        except ValueError:
            print("❌ 请输入数字")
    
    def _print_result(self, result: Dict[str, Any]):
        """打印操作结果"""
        if result.get('success', False):
            print(f"✅ 操作成功完成")
            if 'message' in result:
                print(f"📝 {result['message']}")
            if 'total_duration' in result:
                print(f"⏱️  耗时: {result['total_duration']:.2f} 秒")
        else:
            print(f"❌ 操作失败")
            if 'message' in result:
                print(f"📝 {result['message']}")
            if 'errors' in result:
                print("🐛 错误详情:")
                for i, error in enumerate(result['errors'][:5], 1):
                    print(f"   {i}. {error}")
                if len(result['errors']) > 5:
                    print(f"   ... 还有 {len(result['errors']) - 5} 个错误")
    
    # ==================== 工具方法 ====================
    
    def _split_sql_statements(self, sql_content: str) -> List[str]:
        """分割SQL语句"""
        # 简化的SQL分割逻辑，按分号分割
        statements = []
        current_statement = ""
        
        for line in sql_content.split('\n'):
            line = line.strip()
            if line and not line.startswith('--'):
                current_statement += line + "\n"
                if line.endswith(';'):
                    statements.append(current_statement.strip())
                    current_statement = ""
        
        if current_statement.strip():
            statements.append(current_statement.strip())
        
        return statements
    
    def _extract_view_name(self, statement: str) -> str:
        """从CREATE MATERIALIZED VIEW语句中提取视图名"""
        try:
            # 查找 "nginx_analytics.mv_" 模式
            import re
            match = re.search(r'nginx_analytics\.(mv_\w+)', statement)
            if match:
                return match.group(1)
            return "unknown_view"
        except:
            return "unknown_view"
    
    def _table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        try:
            query = f"EXISTS TABLE {self.database}.{table_name}"
            result = self.client.query(query)
            return bool(result.first_row[0])
        except:
            return False
    
    def _view_exists(self, view_name: str) -> bool:
        """检查物化视图是否存在"""
        try:
            query = f"""
            SELECT count() FROM system.tables 
            WHERE database = '{self.database}' 
              AND name = '{view_name}' 
              AND engine = 'MaterializedView'
            """
            result = self.client.query(query)
            return result.first_row[0] > 0
        except:
            return False
    
    def _get_table_count(self, table_name: str) -> int:
        """获取表记录数"""
        try:
            query = f"SELECT count() FROM {self.database}.{table_name}"
            result = self.client.query(query)
            return int(result.first_row[0])
        except:
            return 0
    
    def _validate_field_mapping(self) -> List[str]:
        """验证字段映射（简化版）"""
        issues = []
        
        # 检查DWD表是否有必需字段
        required_fields = [
            'log_time', 'platform', 'access_type', 'request_uri',
            'total_request_duration', 'response_status_code', 
            'is_success', 'is_error', 'is_slow'
        ]
        
        try:
            query = f"DESCRIBE TABLE {self.database}.dwd_nginx_enriched_v3"
            result = self.client.query(query)
            existing_fields = [row[0] for row in result.result_rows]
            
            for field in required_fields:
                if field not in existing_fields:
                    issues.append(f"DWD表缺少必需字段: {field}")
                    
        except Exception as e:
            issues.append(f"无法验证DWD表字段: {str(e)}")
        
        return issues
    
    def _upgrade_from_v2_to_v5(self):
        """从v2架构升级到v5架构"""
        print("\n🔄 开始从v2架构升级到v5架构...")
        
        if not self.connect():
            return
            
        try:
            # 1. 检查v2表是否存在
            v2_table = 'dwd_nginx_enriched_v2'
            if not self._table_exists(v2_table):
                print(f"❌ 未发现v2表 {v2_table}，无法执行升级")
                return
                
            v2_count = self._get_table_count(v2_table)
            print(f"📊 发现v2表 {v2_table}，包含 {v2_count:,} 条记录")
            
            # 2. 创建v5新架构
            print("\n📋 创建v5增强架构...")
            init_result = self.initialize_complete_architecture()
            if not init_result['success']:
                print("❌ v5架构创建失败")
                return
                
            print("✅ v5架构创建成功")
            
            # 3. 数据兼容性检查
            print("\n🔍 执行数据兼容性检查...")
            compatibility_result = self._check_field_compatibility(v2_table, 'dwd_nginx_enriched_v3')
            
            if compatibility_result['compatible']:
                print("✅ 字段结构兼容，支持数据迁移")
            else:
                print("⚠️  检测到字段差异，将使用智能填充")
                
            # 4. 提供迁移选择
            print("\n📝 升级完成选项:")
            print("1. v2和v5架构并存 (推荐，零风险)")
            print("2. 执行数据迁移 (将v2数据迁移到v3表)")
            print("3. 仅创建v5架构，保持v2不变")
            
            choice = input("请选择 [1-3]: ").strip()
            
            if choice == '2':
                confirm = input("⚠️  确认执行数据迁移？这将消耗较多时间 [y/N]: ").strip().lower()
                if confirm == 'y':
                    self._migrate_data_v2_to_v3(v2_table)
                else:
                    print("✅ 已取消数据迁移，v2和v5架构并存")
            else:
                print("✅ v5架构创建完成，与v2架构并存")
                
        except Exception as e:
            self.logger.error(f"架构升级失败: {str(e)}")
            print(f"❌ 升级失败: {str(e)}")
        finally:
            self.close()
            
    def _check_compatibility_status(self):
        """检查v2和v5架构兼容性状态"""
        print("\n📋 检查v2和v5架构兼容性状态...")
        
        if not self.connect():
            return
            
        try:
            status_report = {
                'v2_architecture': {'exists': False, 'tables': {}, 'record_counts': {}},
                'v5_architecture': {'exists': False, 'tables': {}, 'record_counts': {}},
                'compatibility': {'overall_status': 'unknown', 'details': []}
            }
            
            # 检查v2架构
            print("\n🔍 检查v2架构状态...")
            v2_tables = ['dwd_nginx_enriched_v2', 'ads_api_performance_analysis', 'ads_service_level_analysis']
            v2_exists = 0
            
            for table in v2_tables:
                exists = self._table_exists(table)
                count = self._get_table_count(table) if exists else 0
                status_report['v2_architecture']['tables'][table] = exists
                status_report['v2_architecture']['record_counts'][table] = count
                if exists:
                    v2_exists += 1
                    print(f"   ✅ {table}: {count:,} 条记录")
                else:
                    print(f"   ❌ {table}: 不存在")
                    
            status_report['v2_architecture']['exists'] = v2_exists > 0
            
            # 检查v5架构
            print("\n🔍 检查v5架构状态...")
            v5_tables = ['dwd_nginx_enriched_v3', 'ads_api_performance_analysis_v3', 'ads_platform_entry_analysis']
            v5_exists = 0
            
            for table in v5_tables:
                exists = self._table_exists(table)
                count = self._get_table_count(table) if exists else 0
                status_report['v5_architecture']['tables'][table] = exists
                status_report['v5_architecture']['record_counts'][table] = count
                if exists:
                    v5_exists += 1
                    print(f"   ✅ {table}: {count:,} 条记录")
                else:
                    print(f"   ❌ {table}: 不存在")
                    
            status_report['v5_architecture']['exists'] = v5_exists > 0
            
            # 兼容性分析
            print("\n📊 兼容性状态总结:")
            if status_report['v2_architecture']['exists'] and status_report['v5_architecture']['exists']:
                print("✅ v2和v5架构并存，支持平滑迁移")
                status_report['compatibility']['overall_status'] = 'coexist'
            elif status_report['v5_architecture']['exists']:
                print("✅ v5架构已就绪，可以开始使用新功能")
                status_report['compatibility']['overall_status'] = 'v5_ready'
            elif status_report['v2_architecture']['exists']:
                print("⚠️  仅有v2架构，建议升级到v5以获得增强功能")
                status_report['compatibility']['overall_status'] = 'v2_only'
            else:
                print("❌ 未发现任何架构，请先初始化数据库")
                status_report['compatibility']['overall_status'] = 'none'
                
            return status_report
            
        except Exception as e:
            self.logger.error(f"兼容性检查失败: {str(e)}")
            print(f"❌ 检查失败: {str(e)}")
            return None
        finally:
            self.close()

    def _check_field_compatibility(self, v2_table: str, v3_table: str) -> Dict[str, Any]:
        """检查v2和v3表的字段兼容性"""
        try:
            # 获取v2表字段
            v2_query = f"DESCRIBE TABLE {self.database}.{v2_table}"
            v2_result = self.client.query(v2_query)
            v2_fields = {row[0]: row[1] for row in v2_result.result_rows}
            
            # 获取v3表字段  
            v3_query = f"DESCRIBE TABLE {self.database}.{v3_table}"
            v3_result = self.client.query(v3_query)
            v3_fields = {row[0]: row[1] for row in v3_result.result_rows}
            
            # 兼容性分析
            common_fields = set(v2_fields.keys()) & set(v3_fields.keys())
            v2_only = set(v2_fields.keys()) - set(v3_fields.keys()) 
            v3_only = set(v3_fields.keys()) - set(v2_fields.keys())
            
            return {
                'compatible': len(v2_only) == 0,  # v2字段是否都包含在v3中
                'common_fields': list(common_fields),
                'v2_only_fields': list(v2_only),
                'v3_new_fields': list(v3_only),
                'compatibility_rate': len(common_fields) / len(v2_fields) if v2_fields else 0
            }
            
        except Exception as e:
            self.logger.error(f"字段兼容性检查失败: {str(e)}")
            return {'compatible': False, 'error': str(e)}

    def _migrate_data_v2_to_v3(self, v2_table: str):
        """迁移v2数据到v3表"""
        print(f"\n🔄 开始数据迁移: {v2_table} → dwd_nginx_enriched_v3")
        
        try:
            # 这里可以实现数据迁移逻辑
            # 由于数据量可能很大，建议分批处理
            print("⚠️  数据迁移功能正在开发中...")
            print("💡 建议当前保持v2和v5架构并存，通过ETL逐步切换")
            
        except Exception as e:
            self.logger.error(f"数据迁移失败: {str(e)}")
            print(f"❌ 数据迁移失败: {str(e)}")

    def _generate_initialization_report(self, results: Dict[str, Any]):
        """生成初始化报告"""
        report_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        report_content = f"""
# 数据库架构初始化报告

**初始化时间**: {report_time}
**总耗时**: {results['total_duration']:.2f} 秒
**初始化状态**: {'✅ 成功' if results['success'] else '❌ 失败'}

## 各阶段执行结果

"""
        
        for phase_name, phase_result in results['phases'].items():
            status_icon = "✅" if phase_result['success'] else "❌"
            report_content += f"""
### {status_icon} {phase_name}
- 耗时: {phase_result.get('duration', 0):.2f} 秒
- 处理文件: {phase_result.get('files_processed', 0)} 个
- 创建视图: {phase_result.get('views_created', 0)} 个
- 失败项目: {phase_result.get('views_failed', len(phase_result.get('errors', [])))} 个
"""
            
            if phase_result.get('errors'):
                report_content += "\n**错误信息**:\n"
                for error in phase_result['errors'][:5]:  # 只显示前5个错误
                    report_content += f"- {error}\n"
        
        # 保存报告
        report_file = Path(f"database_init_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md")
        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(report_content)
            self.logger.info(f"初始化报告已保存: {report_file}")
        except Exception as e:
            self.logger.error(f"保存初始化报告失败: {str(e)}")


def main():
    """主函数 - 支持命令行和交互式两种模式"""
    import sys
    
    # 支持命令行参数
    if len(sys.argv) > 1:
        arg = sys.argv[1].lower()
        manager = DatabaseManagerUnified()
        
        try:
            if arg in ['init', '1']:
                print("🚀 开始初始化完整架构...")
                result = manager.initialize_complete_architecture()
                manager._print_result(result)
                success = result.get('success', False)
                sys.exit(0 if success else 1)
                
            elif arg in ['status', '2']:
                manager.print_architecture_report()
                sys.exit(0)
                
            elif arg in ['validate', '3']:
                print("🔍 验证架构完整性...")
                if manager.connect():
                    result = manager.validate_architecture()
                    manager._print_result(result)
                    success = result.get('success', False)
                    sys.exit(0 if success else 1)
                else:
                    print("❌ 数据库连接失败")
                    sys.exit(1)
                
            elif arg in ['rebuild', '4']:
                print("🔄 开始强制重建架构...")
                result = manager.force_rebuild()
                manager._print_result(result)
                success = result.get('success', False)
                sys.exit(0 if success else 1)
                
            elif arg in ['clean', '5']:
                result = manager.clean_all_data()
                manager._print_result(result)
                success = result.get('success', False)
                sys.exit(0 if success else 1)
                
            else:
                print(f"❌ 未知参数: {arg}")
                print("可用参数:")
                print("  init/1     - 初始化完整架构")
                print("  status/2   - 检查架构状态")
                print("  validate/3 - 验证架构完整性")
                print("  rebuild/4  - 强制重建架构")
                print("  clean/5    - 清理所有数据")
                print("\n💡 不带参数运行可进入交互式模式")
                sys.exit(1)
        finally:
            manager.close()
    else:
        # 交互式模式
        print("💡 进入交互式模式...")
        manager = DatabaseManagerUnified()
        try:
            manager.interactive_menu()
        finally:
            manager.close()


if __name__ == "__main__":
    main()