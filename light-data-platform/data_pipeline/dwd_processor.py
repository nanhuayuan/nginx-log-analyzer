# -*- coding: utf-8 -*-
"""
DWD层数据处理器 - 数据富化和标签化
"""

import os
import sys
from pathlib import Path
from datetime import datetime
import argparse

# 添加项目路径到系统路径
sys.path.append(str(Path(__file__).parent.parent))

from database.models import OdsNginxLog, DwdNginxEnriched, get_session, init_db
from utils.data_enricher import DataEnricher
from config.settings import DIMENSIONS

class DwdProcessor:
    """DWD层数据处理器"""
    
    def __init__(self, database_path='database/nginx_analytics.db'):
        self.database_path = database_path
        self.enricher = DataEnricher(DIMENSIONS)
        
    def process_ods_to_dwd(self, batch_size: int = 1000, max_records: int = None) -> int:
        """将ODS层数据富化处理到DWD层"""
        
        session = get_session(self.database_path)
        
        try:
            # 查询ODS数据总数
            total_ods = session.query(OdsNginxLog).count()
            print(f"ODS层共有 {total_ods} 条数据")
            
            if total_ods == 0:
                print("ODS层无数据，请先加载数据")
                return 0
            
            # 清理DWD层历史数据(可选)
            # session.query(DwdNginxEnriched).delete()
            # session.commit()
            
            # 分批处理ODS数据
            processed_count = 0
            skip_count = 0
            
            # 限制处理记录数(测试用)
            limit = min(max_records or total_ods, total_ods)
            
            for offset in range(0, limit, batch_size):
                batch_ods = session.query(OdsNginxLog).offset(offset).limit(batch_size).all()
                
                if not batch_ods:
                    break
                
                enriched_records = []
                
                for ods_record in batch_ods:
                    try:
                        # 检查是否已经处理过
                        existing = session.query(DwdNginxEnriched).filter(
                            DwdNginxEnriched.ods_id == ods_record.id
                        ).first()
                        
                        if existing:
                            skip_count += 1
                            continue
                        
                        # 转换为字典格式
                        record_dict = {
                            'timestamp': ods_record.timestamp.strftime('%Y-%m-%d %H:%M:%S') if ods_record.timestamp else '',
                            'client_ip': ods_record.client_ip or '',
                            'request_full_uri': ods_record.request_full_uri or '',
                            'response_status_code': ods_record.response_status_code or '',
                            'total_request_duration': ods_record.total_request_duration or 0.0,
                            'response_body_size_kb': ods_record.response_body_size_kb or 0.0,
                            'user_agent': ods_record.user_agent or '',
                            'referer': ods_record.referer or '',
                            'application_name': ods_record.application_name or '',
                            'service_name': ods_record.service_name or '',
                            'request_method': ods_record.request_method or ''
                        }
                        
                        # 数据富化
                        enriched_dict = self.enricher.enrich_record(record_dict)
                        
                        # 创建DWD记录
                        dwd_record = DwdNginxEnriched(
                            ods_id=ods_record.id,
                            timestamp=ods_record.timestamp,
                            date_partition=enriched_dict['date_partition'],
                            hour_partition=enriched_dict['hour_partition'],
                            
                            client_ip=ods_record.client_ip,
                            request_uri=enriched_dict['request_uri'][:500],  # 截断长度
                            request_method=ods_record.request_method,
                            response_status_code=ods_record.response_status_code,
                            response_time=enriched_dict['response_time'],
                            response_size_kb=enriched_dict['response_size_kb'],
                            
                            platform=enriched_dict['platform'],
                            platform_version=enriched_dict['platform_version'],
                            entry_source=enriched_dict['entry_source'],
                            api_category=enriched_dict['api_category'],
                            
                            application_name=ods_record.application_name,
                            service_name=ods_record.service_name,
                            is_success=enriched_dict['is_success'],
                            is_slow=enriched_dict['is_slow'],
                            
                            data_quality_score=enriched_dict['data_quality_score'],
                            has_anomaly=enriched_dict['has_anomaly'],
                            anomaly_type=enriched_dict['anomaly_type']
                        )
                        
                        enriched_records.append(dwd_record)
                        
                    except Exception as e:
                        print(f"处理记录失败 (ODS ID: {ods_record.id}): {e}")
                        continue
                
                # 批量插入DWD层
                if enriched_records:
                    session.add_all(enriched_records)
                    session.commit()
                    processed_count += len(enriched_records)
                    
                    print(f"已处理 {processed_count + skip_count}/{limit} 条记录 (新增: {processed_count}, 跳过: {skip_count})")
            
            print(f"\nDWD层数据处理完成")
            print(f"新增处理: {processed_count} 条")
            print(f"跳过重复: {skip_count} 条")
            
            return processed_count
            
        except Exception as e:
            session.rollback()
            raise Exception(f"DWD处理失败: {e}")
        finally:
            session.close()
    
    def get_dwd_statistics(self) -> dict:
        """获取DWD层数据统计"""
        session = get_session(self.database_path)
        
        try:
            from sqlalchemy import func
            
            # 基础统计
            total_records = session.query(DwdNginxEnriched).count()
            
            # 平台分布
            platform_dist = session.query(
                DwdNginxEnriched.platform,
                func.count(DwdNginxEnriched.id).label('count')
            ).group_by(DwdNginxEnriched.platform).all()
            
            # 入口来源分布
            entry_source_dist = session.query(
                DwdNginxEnriched.entry_source,
                func.count(DwdNginxEnriched.id).label('count')
            ).group_by(DwdNginxEnriched.entry_source).all()
            
            # API分类分布
            api_category_dist = session.query(
                DwdNginxEnriched.api_category,
                func.count(DwdNginxEnriched.id).label('count')
            ).group_by(DwdNginxEnriched.api_category).all()
            
            # 成功率统计
            success_count = session.query(DwdNginxEnriched).filter(
                DwdNginxEnriched.is_success == True
            ).count()
            
            # 慢请求统计
            slow_count = session.query(DwdNginxEnriched).filter(
                DwdNginxEnriched.is_slow == True
            ).count()
            
            # 异常记录统计
            anomaly_count = session.query(DwdNginxEnriched).filter(
                DwdNginxEnriched.has_anomaly == True
            ).count()
            
            # 数据质量评分统计
            avg_quality = session.query(func.avg(DwdNginxEnriched.data_quality_score)).scalar() or 0.0
            
            return {
                'total_records': total_records,
                'success_rate': (success_count / total_records * 100) if total_records > 0 else 0,
                'slow_rate': (slow_count / total_records * 100) if total_records > 0 else 0,
                'anomaly_rate': (anomaly_count / total_records * 100) if total_records > 0 else 0,
                'avg_quality_score': round(avg_quality, 3),
                'platform_distribution': dict(platform_dist),
                'entry_source_distribution': dict(entry_source_dist),
                'api_category_distribution': dict(api_category_dist)
            }
            
        finally:
            session.close()
    
    def analyze_dimensions(self) -> dict:
        """多维度分析"""
        session = get_session(self.database_path)
        
        try:
            from sqlalchemy import func
            
            # 简化的平台分析
            platforms = session.query(DwdNginxEnriched.platform).distinct().all()
            platform_analysis = []
            
            for (platform,) in platforms:
                total = session.query(DwdNginxEnriched).filter(DwdNginxEnriched.platform == platform).count()
                success = session.query(DwdNginxEnriched).filter(
                    DwdNginxEnriched.platform == platform,
                    DwdNginxEnriched.is_success == True
                ).count()
                avg_time = session.query(func.avg(DwdNginxEnriched.response_time)).filter(
                    DwdNginxEnriched.platform == platform
                ).scalar() or 0.0
                
                platform_analysis.append({
                    'platform': platform,
                    'total': total,
                    'success': success,
                    'avg_response_time': avg_time
                })
            
            # 入口来源分析
            entry_sources = session.query(DwdNginxEnriched.entry_source).distinct().all()
            entry_analysis = []
            
            for (entry_source,) in entry_sources:
                total = session.query(DwdNginxEnriched).filter(DwdNginxEnriched.entry_source == entry_source).count()
                slow = session.query(DwdNginxEnriched).filter(
                    DwdNginxEnriched.entry_source == entry_source,
                    DwdNginxEnriched.is_slow == True
                ).count()
                avg_time = session.query(func.avg(DwdNginxEnriched.response_time)).filter(
                    DwdNginxEnriched.entry_source == entry_source
                ).scalar() or 0.0
                
                entry_analysis.append({
                    'entry_source': entry_source,
                    'total': total,
                    'slow_requests': slow,
                    'avg_response_time': avg_time
                })
            
            # API分类分析
            api_categories = session.query(DwdNginxEnriched.api_category).distinct().all()
            api_analysis = []
            
            for (api_category,) in api_categories:
                total = session.query(DwdNginxEnriched).filter(DwdNginxEnriched.api_category == api_category).count()
                errors = session.query(DwdNginxEnriched).filter(
                    DwdNginxEnriched.api_category == api_category,
                    DwdNginxEnriched.is_success == False
                ).count()
                anomalies = session.query(DwdNginxEnriched).filter(
                    DwdNginxEnriched.api_category == api_category,
                    DwdNginxEnriched.has_anomaly == True
                ).count()
                
                api_analysis.append({
                    'api_category': api_category,
                    'total': total,
                    'errors': errors,
                    'anomalies': anomalies
                })
            
            return {
                'platform_analysis': [
                    {
                        'platform': row['platform'],
                        'total_requests': row['total'],
                        'success_requests': row['success'],
                        'success_rate': round((row['success'] / row['total'] * 100) if row['total'] > 0 else 0, 2),
                        'avg_response_time': round(row['avg_response_time'] or 0, 3)
                    }
                    for row in platform_analysis
                ],
                'entry_source_analysis': [
                    {
                        'entry_source': row['entry_source'],
                        'total_requests': row['total'],
                        'avg_response_time': round(row['avg_response_time'] or 0, 3),
                        'slow_requests': row['slow_requests'],
                        'slow_rate': round((row['slow_requests'] / row['total'] * 100) if row['total'] > 0 else 0, 2)
                    }
                    for row in entry_analysis
                ],
                'api_category_analysis': [
                    {
                        'api_category': row['api_category'],
                        'total_requests': row['total'],
                        'error_requests': row['errors'],
                        'error_rate': round((row['errors'] / row['total'] * 100) if row['total'] > 0 else 0, 2),
                        'anomaly_requests': row['anomalies'],
                        'anomaly_rate': round((row['anomalies'] / row['total'] * 100) if row['total'] > 0 else 0, 2)
                    }
                    for row in api_analysis
                ]
            }
            
        finally:
            session.close()

def main():
    """命令行入口"""
    parser = argparse.ArgumentParser(description='DWD层数据处理器')
    parser.add_argument('--process', action='store_true', help='处理ODS到DWD')
    parser.add_argument('--stats', action='store_true', help='显示DWD统计')
    parser.add_argument('--analyze', action='store_true', help='多维度分析')
    parser.add_argument('--batch-size', type=int, default=1000, help='批处理大小')
    parser.add_argument('--max-records', type=int, help='最大处理记录数(测试用)')
    
    args = parser.parse_args()
    
    processor = DwdProcessor()
    
    # 处理数据
    if args.process:
        try:
            count = processor.process_ods_to_dwd(args.batch_size, args.max_records)
            print(f"成功处理 {count} 条数据到DWD层")
        except Exception as e:
            print(f"处理失败: {e}")
            return
    
    # 显示统计
    if args.stats:
        try:
            stats = processor.get_dwd_statistics()
            print("\n=== DWD层数据统计 ===")
            print(f"总记录数: {stats['total_records']:,}")
            print(f"成功率: {stats['success_rate']:.1f}%")
            print(f"慢请求率: {stats['slow_rate']:.1f}%")
            print(f"异常率: {stats['anomaly_rate']:.1f}%")
            print(f"平均数据质量评分: {stats['avg_quality_score']}")
            
            print("\n平台分布:")
            for platform, count in stats['platform_distribution'].items():
                print(f"  {platform}: {count:,}")
            
            print("\n入口来源分布:")
            for source, count in stats['entry_source_distribution'].items():
                print(f"  {source}: {count:,}")
            
            print("\nAPI分类分布:")
            for category, count in stats['api_category_distribution'].items():
                print(f"  {category}: {count:,}")
                
        except Exception as e:
            print(f"获取统计失败: {e}")
    
    # 多维度分析
    if args.analyze:
        try:
            analysis = processor.analyze_dimensions()
            
            print("\n=== 多维度分析 ===")
            
            print("\n## 平台维度分析")
            for item in analysis['platform_analysis']:
                print(f"  {item['platform']}: {item['total_requests']}请求, 成功率{item['success_rate']}%, 平均响应{item['avg_response_time']}s")
            
            print("\n## 入口来源分析") 
            for item in analysis['entry_source_analysis']:
                print(f"  {item['entry_source']}: {item['total_requests']}请求, 慢请求率{item['slow_rate']}%, 平均响应{item['avg_response_time']}s")
            
            print("\n## API分类分析")
            for item in analysis['api_category_analysis']:
                print(f"  {item['api_category']}: {item['total_requests']}请求, 错误率{item['error_rate']}%, 异常率{item['anomaly_rate']}%")
                
        except Exception as e:
            print(f"多维度分析失败: {e}")
    
    # 默认显示统计
    if not any([args.process, args.stats, args.analyze]):
        print("请指定操作: --process, --stats, 或 --analyze")

if __name__ == "__main__":
    main()