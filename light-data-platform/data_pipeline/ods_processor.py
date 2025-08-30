# -*- coding: utf-8 -*-
"""
ODS层数据处理器 - 从CSV加载原始数据
"""

import os
import sys
import pandas as pd
from datetime import datetime
from pathlib import Path
import argparse

# 添加项目路径到系统路径
sys.path.append(str(Path(__file__).parent.parent))

from database.models import OdsNginxLog, get_session, init_db
from config.settings import DATA_SOURCE

class OdsProcessor:
    """ODS层数据处理器"""
    
    def __init__(self, database_path='database/nginx_analytics.db'):
        self.database_path = database_path
        
    def load_csv_to_ods(self, csv_path: str, batch_size: int = 1000) -> int:
        """从CSV文件加载数据到ODS层"""
        
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV文件不存在: {csv_path}")
        
        print(f"开始加载CSV数据到ODS层: {csv_path}")
        
        # 读取CSV数据
        try:
            df = pd.read_csv(csv_path)
            print(f"成功读取CSV，共 {len(df)} 行数据")
        except Exception as e:
            raise Exception(f"读取CSV文件失败: {e}")
        
        # 获取数据库会话
        session = get_session(self.database_path)
        
        try:
            total_inserted = 0
            
            # 分批处理数据
            for i in range(0, len(df), batch_size):
                batch_df = df.iloc[i:i+batch_size]
                
                batch_records = []
                for _, row in batch_df.iterrows():
                    try:
                        # CSV字段映射 - 适配现有CSV格式
                        field_mapping = {
                            'timestamp': row.get('raw_time', row.get('arrival_time', '')),
                            'client_ip': row.get('client_ip_address', ''),
                            'request_method': row.get('http_method', ''),
                            'request_full_uri': row.get('request_full_uri', ''),
                            'request_protocol': row.get('http_protocol_version', ''),
                            'response_status_code': row.get('response_status_code', ''),
                            'response_body_size_kb': row.get('response_body_size_kb', 0),
                            'total_bytes_sent_kb': row.get('total_bytes_sent_kb', 0),
                            'referer': row.get('referer_url', ''),
                            'user_agent': row.get('user_agent_string', ''),
                            'total_request_duration': row.get('total_request_duration', 0),
                            'upstream_response_time': row.get('upstream_response_time', 0),
                            'upstream_connect_time': row.get('upstream_connect_time', 0),
                            'upstream_header_time': row.get('upstream_header_time', 0),
                            'application_name': row.get('application_name', ''),
                            'service_name': row.get('service_name', '')
                        }
                        
                        # 数据类型转换和清洗
                        record = OdsNginxLog(
                            timestamp=pd.to_datetime(field_mapping['timestamp']) if field_mapping['timestamp'] else None,
                            client_ip=str(field_mapping['client_ip'])[:45],
                            request_method=str(field_mapping['request_method'])[:10],
                            request_full_uri=str(field_mapping['request_full_uri']),
                            request_protocol=str(field_mapping['request_protocol'])[:20],
                            response_status_code=str(field_mapping['response_status_code'])[:10],
                            response_body_size_kb=self._safe_float(field_mapping['response_body_size_kb']),
                            total_bytes_sent_kb=self._safe_float(field_mapping['total_bytes_sent_kb']),
                            referer=str(field_mapping['referer']),
                            user_agent=str(field_mapping['user_agent']),
                            total_request_duration=self._safe_float(field_mapping['total_request_duration']),
                            upstream_response_time=self._safe_float(field_mapping['upstream_response_time']),
                            upstream_connect_time=self._safe_float(field_mapping['upstream_connect_time']),
                            upstream_header_time=self._safe_float(field_mapping['upstream_header_time']),
                            application_name=str(field_mapping['application_name'])[:100],
                            service_name=str(field_mapping['service_name'])[:100],
                            source_file=os.path.basename(csv_path)
                        )
                        batch_records.append(record)
                        
                    except Exception as e:
                        print(f"处理行数据时出错 (行 {i + len(batch_records)}): {e}")
                        continue
                
                # 批量插入数据库
                if batch_records:
                    session.add_all(batch_records)
                    session.commit()
                    total_inserted += len(batch_records)
                    
                    print(f"已插入 {total_inserted}/{len(df)} 行数据 ({total_inserted/len(df)*100:.1f}%)")
                    
            print(f"ODS层数据加载完成，共插入 {total_inserted} 行数据")
            return total_inserted
            
        except Exception as e:
            session.rollback()
            raise Exception(f"数据库操作失败: {e}")
        finally:
            session.close()
    
    def _safe_float(self, value) -> float:
        """安全转换为浮点数"""
        try:
            if pd.isna(value) or value == '' or value is None:
                return 0.0
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    
    def get_ods_statistics(self) -> dict:
        """获取ODS层数据统计"""
        session = get_session(self.database_path)
        
        try:
            # 总记录数
            total_records = session.query(OdsNginxLog).count()
            
            # 时间范围
            min_time = session.query(OdsNginxLog.timestamp).order_by(OdsNginxLog.timestamp.asc()).first()
            max_time = session.query(OdsNginxLog.timestamp).order_by(OdsNginxLog.timestamp.desc()).first()
            
            # 状态码分布
            from sqlalchemy import func
            status_distribution = session.query(
                OdsNginxLog.response_status_code,
                func.count(OdsNginxLog.id).label('count')
            ).group_by(OdsNginxLog.response_status_code).limit(10).all()
            
            # 平台分布（简单统计）
            ios_count = session.query(OdsNginxLog).filter(
                OdsNginxLog.user_agent.like('%ios%')
            ).count()
            
            android_count = session.query(OdsNginxLog).filter(
                OdsNginxLog.user_agent.like('%android%')
            ).count()
            
            return {
                'total_records': total_records,
                'time_range': {
                    'start': min_time[0] if min_time else None,
                    'end': max_time[0] if max_time else None
                },
                'platform_distribution': {
                    'ios_related': ios_count,
                    'android_related': android_count,
                    'other': total_records - ios_count - android_count
                },
                'status_distribution': dict(status_distribution) if status_distribution else {}
            }
            
        finally:
            session.close()
    
    def clean_old_data(self, days_to_keep: int = 30):
        """清理过期数据"""
        from datetime import timedelta
        
        cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)
        session = get_session(self.database_path)
        
        try:
            deleted_count = session.query(OdsNginxLog).filter(
                OdsNginxLog.timestamp < cutoff_date
            ).delete()
            
            session.commit()
            print(f"清理了 {deleted_count} 条过期数据 (保留 {days_to_keep} 天)")
            return deleted_count
            
        except Exception as e:
            session.rollback()
            raise Exception(f"清理数据失败: {e}")
        finally:
            session.close()

def main():
    """命令行入口"""
    parser = argparse.ArgumentParser(description='ODS层数据处理器')
    parser.add_argument('--csv-path', type=str, help='CSV文件路径')
    parser.add_argument('--init-db', action='store_true', help='初始化数据库')
    parser.add_argument('--stats', action='store_true', help='显示数据统计')
    parser.add_argument('--clean', type=int, help='清理N天前的数据')
    parser.add_argument('--batch-size', type=int, default=1000, help='批处理大小')
    
    args = parser.parse_args()
    
    # 初始化数据库
    if args.init_db:
        print("初始化数据库...")
        init_db()
        return
    
    # 创建处理器
    processor = OdsProcessor()
    
    # 加载CSV数据
    if args.csv_path:
        if not os.path.exists(args.csv_path):
            print(f"错误: CSV文件不存在: {args.csv_path}")
            return
        
        try:
            count = processor.load_csv_to_ods(args.csv_path, args.batch_size)
            print(f"成功加载 {count} 条数据")
        except Exception as e:
            print(f"加载失败: {e}")
            return
    
    # 显示统计信息
    if args.stats:
        try:
            stats = processor.get_ods_statistics()
            print("\\n=== ODS层数据统计 ===")
            print(f"总记录数: {stats['total_records']:,}")
            
            if stats['time_range']['start'] and stats['time_range']['end']:
                print(f"时间范围: {stats['time_range']['start']} ~ {stats['time_range']['end']}")
            
            print("\\n平台分布:")
            for platform, count in stats['platform_distribution'].items():
                print(f"  {platform}: {count:,}")
                
            print("\\n状态码分布:")
            for status, count in stats['status_distribution'].items():
                print(f"  {status}: {count:,}")
                
        except Exception as e:
            print(f"获取统计信息失败: {e}")
    
    # 清理数据
    if args.clean:
        try:
            count = processor.clean_old_data(args.clean)
            print(f"已清理 {count} 条过期数据")
        except Exception as e:
            print(f"清理失败: {e}")
    
    # 如果没有指定任何操作，使用默认CSV路径
    if not any([args.csv_path, args.stats, args.clean]):
        default_csv = DATA_SOURCE['default_csv_path']
        if os.path.exists(default_csv):
            print(f"使用默认CSV文件: {default_csv}")
            try:
                count = processor.load_csv_to_ods(str(default_csv), args.batch_size)
                print(f"成功加载 {count} 条数据")
                
                # 显示统计
                stats = processor.get_ods_statistics()
                print(f"\\n数据库中共有 {stats['total_records']:,} 条记录")
                
            except Exception as e:
                print(f"处理失败: {e}")
        else:
            print(f"默认CSV文件不存在: {default_csv}")
            print("请使用 --csv-path 指定CSV文件路径")

if __name__ == "__main__":
    main()