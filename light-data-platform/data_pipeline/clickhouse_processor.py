# -*- coding: utf-8 -*-
"""
ClickHouse数据处理器 - 直接从CSV加载到ClickHouse
"""

import os
import sys
import pandas as pd
from datetime import datetime
from pathlib import Path
import argparse
import clickhouse_connect

# 添加项目路径到系统路径
sys.path.append(str(Path(__file__).parent.parent))

from utils.data_enricher import DataEnricher
from config.settings import DIMENSIONS

class ClickHouseProcessor:
    """ClickHouse数据处理器"""
    
    def __init__(self, host='localhost', port=8123, username='web_user', password='web_password', database='nginx_analytics'):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.client = None
        self.enricher = DataEnricher(DIMENSIONS)
        
    def connect(self):
        """连接ClickHouse"""
        try:
            self.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                database=self.database
            )
            # 设置会话时区为东8区，确保时间显示正确
            self.client.command("SET session_timezone = 'Asia/Shanghai'")
            print(f"成功连接到ClickHouse: {self.host}:{self.port}")
            return True
        except Exception as e:
            print(f"连接ClickHouse失败: {e}")
            return False
    
    def load_csv_to_clickhouse(self, csv_path: str, batch_size: int = 1000) -> int:
        """从CSV文件直接加载数据到ClickHouse"""
        
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV文件不存在: {csv_path}")
        
        if not self.client:
            if not self.connect():
                raise Exception("无法连接到ClickHouse")
        
        print(f"开始加载CSV数据到ClickHouse: {csv_path}")
        
        # 读取CSV数据
        try:
            df = pd.read_csv(csv_path)
            print(f"成功读取CSV，共 {len(df)} 行数据")
        except Exception as e:
            raise Exception(f"读取CSV文件失败: {e}")
        
        try:
            total_inserted = 0
            
            # 分批处理数据
            for i in range(0, len(df), batch_size):
                batch_df = df.iloc[i:i+batch_size]
                
                ods_records = []
                dwd_records = []
                
                for idx, row in batch_df.iterrows():
                    try:
                        # 生成ID
                        record_id = int(f"{int(datetime.now().timestamp())}{idx % 10000}")
                        
                        # CSV字段映射 - 扩展支持Self功能需要的所有字段
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
                            'service_name': row.get('service_name', ''),
                            
                            # 扩展字段：时间维度
                            'date': row.get('date', ''),
                            'hour': row.get('hour', 0),
                            'minute': row.get('minute', 0),
                            'second': row.get('second', 0),
                            'date_hour': row.get('date_hour', ''),
                            'date_hour_minute': row.get('date_hour_minute', ''),
                            
                            # 扩展字段：到达时间维度
                            'arrival_timestamp': row.get('arrival_timestamp', ''),
                            'arrival_date': row.get('arrival_date', ''),
                            'arrival_hour': row.get('arrival_hour', 0),
                            
                            # 扩展字段：请求详细信息
                            'query_parameters': row.get('query_parameters', ''),
                            
                            # 扩展字段：HTTP生命周期阶段时间
                            'phase_upstream_connect': row.get('phase_upstream_connect', 0),
                            'phase_upstream_header': row.get('phase_upstream_header', 0),
                            'phase_upstream_body': row.get('phase_upstream_body', 0),
                            'phase_client_transfer': row.get('phase_client_transfer', 0),
                            
                            # 扩展字段：高级时间指标
                            'backend_connect_phase': row.get('backend_connect_phase', 0),
                            'backend_process_phase': row.get('backend_process_phase', 0),
                            'backend_transfer_phase': row.get('backend_transfer_phase', 0),
                            'nginx_transfer_phase': row.get('nginx_transfer_phase', 0),
                            'backend_total_phase': row.get('backend_total_phase', 0),
                            'network_phase': row.get('network_phase', 0)
                        }
                        
                        # 处理时间戳 - 保持原始时间不做时区转换
                        if field_mapping['timestamp']:
                            # 直接解析字符串时间，不进行时区转换
                            timestamp = datetime.strptime(field_mapping['timestamp'], '%Y-%m-%d %H:%M:%S')
                        else:
                            timestamp = datetime.now()
                        
                        # ODS记录
                        ods_record = [
                            record_id,
                            timestamp,
                            self._safe_string(field_mapping['client_ip'], 45),
                            self._safe_string(field_mapping['request_method'], 10),
                            self._safe_string(field_mapping['request_full_uri']),
                            self._safe_string(field_mapping['request_protocol'], 20),
                            self._safe_string(field_mapping['response_status_code'], 10),
                            self._safe_float(field_mapping['response_body_size_kb']),
                            self._safe_float(field_mapping['total_bytes_sent_kb']),
                            self._safe_string(field_mapping['referer']),
                            self._safe_string(field_mapping['user_agent']),
                            self._safe_float(field_mapping['total_request_duration']),
                            self._safe_float(field_mapping['upstream_response_time']),
                            self._safe_float(field_mapping['upstream_connect_time']),
                            self._safe_float(field_mapping['upstream_header_time']),
                            self._safe_string(field_mapping['application_name'], 100),
                            self._safe_string(field_mapping['service_name'], 100),
                            os.path.basename(csv_path),
                            datetime.now()
                        ]
                        ods_records.append(ods_record)
                        
                        # 数据富化为DWD
                        record_dict = {
                            'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                            'client_ip': field_mapping['client_ip'],
                            'request_full_uri': field_mapping['request_full_uri'],
                            'response_status_code': field_mapping['response_status_code'],
                            'total_request_duration': field_mapping['total_request_duration'],
                            'response_body_size_kb': field_mapping['response_body_size_kb'],
                            'user_agent': field_mapping['user_agent'],
                            'referer': field_mapping['referer'],
                            'application_name': field_mapping['application_name'],
                            'service_name': field_mapping['service_name'],
                            'request_method': field_mapping['request_method']
                        }
                        
                        enriched_dict = self.enricher.enrich_record(record_dict)
                        
                        # DWD记录 - 包含扩展字段
                        dwd_record = [
                            record_id + 1000000,  # DWD ID
                            record_id,  # ODS ID
                            timestamp,
                            self._safe_string(enriched_dict.get('date_partition', '')),
                            enriched_dict.get('hour_partition', 0),
                            self._safe_string(field_mapping['client_ip'], 45),
                            self._safe_string(enriched_dict.get('request_uri', ''), 500),
                            self._safe_string(field_mapping['request_method'], 10),
                            self._safe_string(field_mapping['response_status_code'], 10),
                            self._safe_float(enriched_dict.get('response_time', 0)),
                            self._safe_float(enriched_dict.get('response_size_kb', 0)),
                            self._safe_string(enriched_dict.get('platform', '')),
                            self._safe_string(enriched_dict.get('platform_version', '')),
                            self._safe_string(enriched_dict.get('entry_source', '')),
                            self._safe_string(enriched_dict.get('api_category', '')),
                            self._safe_string(field_mapping['application_name'], 100),
                            self._safe_string(field_mapping['service_name'], 100),
                            bool(enriched_dict.get('is_success', False)),
                            bool(enriched_dict.get('is_slow', False)),
                            self._safe_float(enriched_dict.get('data_quality_score', 0)),
                            bool(enriched_dict.get('has_anomaly', False)),
                            self._safe_string(enriched_dict.get('anomaly_type', '')),
                            datetime.now(),
                            datetime.now(),
                            
                            # 扩展字段 - Self功能需要
                            self._safe_float(field_mapping['total_request_duration']),
                            self._safe_float(field_mapping['upstream_response_time']),
                            self._safe_float(field_mapping['upstream_header_time']),
                            self._safe_float(field_mapping['upstream_connect_time']),
                            self._safe_float(field_mapping['total_bytes_sent_kb']),
                            self._safe_string(field_mapping['request_full_uri'], 1000),
                            self._safe_string(field_mapping['query_parameters'], 500),
                            self._safe_string(field_mapping['request_protocol'], 20),
                            self._safe_string(field_mapping['referer'], 500),
                            self._safe_string(field_mapping['user_agent'], 500),
                            timestamp.date() if timestamp else None,  # date
                            timestamp.hour if timestamp else 0,      # hour
                            timestamp.minute if timestamp else 0,    # minute
                            timestamp.second if timestamp else 0,    # second
                            timestamp.strftime('%Y-%m-%d %H') if timestamp else '',  # date_hour
                            timestamp.strftime('%Y-%m-%d %H:%M') if timestamp else ''  # date_hour_minute
                        ]
                        dwd_records.append(dwd_record)
                        
                    except Exception as e:
                        print(f"处理行数据时出错 (行 {idx}): {e}")
                        continue
                
                # 批量插入ODS层
                if ods_records:
                    self.client.insert('ods_nginx_log', ods_records,
                                     column_names=[
                                         'id', 'timestamp', 'client_ip', 'request_method',
                                         'request_full_uri', 'request_protocol', 'response_status_code',
                                         'response_body_size_kb', 'total_bytes_sent_kb', 'referer',
                                         'user_agent', 'total_request_duration', 'upstream_response_time',
                                         'upstream_connect_time', 'upstream_header_time', 'application_name',
                                         'service_name', 'source_file', 'created_at'
                                     ])
                
                # 批量插入DWD层 - 包含扩展字段
                if dwd_records:
                    self.client.insert('dwd_nginx_enriched', dwd_records,
                                     column_names=[
                                         'id', 'ods_id', 'timestamp', 'date_partition', 'hour_partition',
                                         'client_ip', 'request_uri', 'request_method', 'response_status_code',
                                         'response_time', 'response_size_kb', 'platform', 'platform_version',
                                         'entry_source', 'api_category', 'application_name', 'service_name',
                                         'is_success', 'is_slow', 'data_quality_score', 'has_anomaly',
                                         'anomaly_type', 'created_at', 'updated_at',
                                         
                                         # 扩展字段
                                         'total_request_duration', 'upstream_response_time', 'upstream_header_time',
                                         'upstream_connect_time', 'total_bytes_sent_kb', 'request_full_uri',
                                         'query_parameters', 'http_protocol_version', 'referer_url', 'user_agent_string',
                                         'date', 'hour', 'minute', 'second', 'date_hour', 'date_hour_minute'
                                     ])
                
                total_inserted += len(ods_records)
                print(f"已插入 {total_inserted}/{len(df)} 行数据 ({total_inserted/len(df)*100:.1f}%)")
            
            print(f"ClickHouse数据加载完成，共插入 {total_inserted} 行数据")
            return total_inserted
            
        except Exception as e:
            raise Exception(f"ClickHouse操作失败: {e}")
    
    def _safe_float(self, value) -> float:
        """安全转换为浮点数"""
        try:
            if pd.isna(value) or value == '' or value is None:
                return 0.0
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    
    def _safe_string(self, value, max_length: int = None) -> str:
        """安全转换为字符串，处理None值"""
        try:
            if pd.isna(value) or value is None:
                result = ''
            else:
                result = str(value)
            
            if max_length and len(result) > max_length:
                result = result[:max_length]
            
            return result
        except (ValueError, TypeError):
            return ''
    
    def get_statistics(self, start_time=None, end_time=None) -> dict:
        """获取ClickHouse数据统计"""
        # 为了避免并发查询问题，每次统计都创建新的客户端连接
        import clickhouse_connect
        try:
            # 创建独立的客户端连接
            client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                database=self.database
            )
            # 设置会话时区为东8区
            client.command("SET session_timezone = 'Asia/Shanghai'")
        except Exception as e:
            raise Exception(f"创建ClickHouse连接失败: {e}")
        
        try:
            # 构建时间过滤条件
            time_filter = ""
            if start_time and end_time:
                # 数据以UTC字符串形式存储，需要使用toString转换进行比较
                time_filter = f"WHERE toString(log_time) >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}' AND toString(log_time) <= '{end_time.strftime('%Y-%m-%d %H:%M:%S')}'"
            
            # 总记录数
            total_records = client.command(f"SELECT count(*) FROM dwd_nginx_enriched {time_filter}")
            
            # 时间范围（V2系统使用log_time字段）
            time_range = client.query(f"SELECT min(log_time), max(log_time) FROM dwd_nginx_enriched {time_filter}").first_row
            
            # 平台分布
            platform_dist = client.query(f"SELECT platform, count(*) as cnt FROM dwd_nginx_enriched {time_filter} GROUP BY platform ORDER BY cnt DESC").result_rows
            
            # 入口来源分布
            entry_source_dist = client.query(f"SELECT entry_source, count(*) as cnt FROM dwd_nginx_enriched {time_filter} GROUP BY entry_source ORDER BY cnt DESC").result_rows
            
            # API分类分布
            api_category_dist = client.query(f"SELECT api_category, count(*) as cnt FROM dwd_nginx_enriched {time_filter} GROUP BY api_category ORDER BY cnt DESC").result_rows
            
            # 成功率统计
            success_where = f"{time_filter} AND is_success = true" if time_filter else "WHERE is_success = true"
            success_count = client.command(f"SELECT count(*) FROM dwd_nginx_enriched {success_where}")
            
            # 慢请求统计
            slow_where = f"{time_filter} AND is_slow = true" if time_filter else "WHERE is_slow = true"
            slow_count = client.command(f"SELECT count(*) FROM dwd_nginx_enriched {slow_where}")
            
            # 异常记录统计
            anomaly_where = f"{time_filter} AND has_anomaly = true" if time_filter else "WHERE has_anomaly = true"
            anomaly_count = client.command(f"SELECT count(*) FROM dwd_nginx_enriched {anomaly_where}")
            
            # 数据质量评分统计
            avg_quality_result = client.query(f"SELECT avg(data_quality_score) FROM dwd_nginx_enriched {time_filter}").first_row
            avg_quality = avg_quality_result[0] if avg_quality_result and avg_quality_result[0] else 0
            
            result = {
                'total_records': total_records,
                'success_rate': (success_count / total_records * 100) if total_records > 0 else 0,
                'slow_rate': (slow_count / total_records * 100) if total_records > 0 else 0,
                'anomaly_rate': (anomaly_count / total_records * 100) if total_records > 0 else 0,
                'avg_quality_score': round(avg_quality or 0, 3),
                'time_range': {
                    'start': time_range[0] if time_range and time_range[0] else None,
                    'end': time_range[1] if time_range and time_range[1] else None
                },
                'platform_distribution': dict(platform_dist) if platform_dist else {},
                'entry_source_distribution': dict(entry_source_dist) if entry_source_dist else {},
                'api_category_distribution': dict(api_category_dist) if api_category_dist else {}
            }
            
            # 关闭独立的客户端连接
            client.close()
            return result
            
        except Exception as e:
            # 确保在异常情况下也关闭连接
            try:
                client.close()
            except:
                pass
            raise Exception(f"获取统计失败: {e}")
    
    def analyze_dimensions(self) -> dict:
        """多维度分析"""
        if not self.client:
            if not self.connect():
                raise Exception("无法连接到ClickHouse")
        
        try:
            # 平台分析
            platform_analysis = self.client.query("""
                SELECT 
                    platform,
                    count(*) as total_requests,
                    sum(case when is_success then 1 else 0 end) as success_requests,
                    avg(total_request_duration) as avg_response_time
                FROM dwd_nginx_enriched 
                GROUP BY platform
                ORDER BY total_requests DESC
            """).result_rows
            
            # 入口来源分析
            entry_analysis = self.client.query("""
                SELECT 
                    entry_source,
                    count(*) as total_requests,
                    avg(total_request_duration) as avg_response_time,
                    sum(case when is_slow then 1 else 0 end) as slow_requests
                FROM dwd_nginx_enriched 
                GROUP BY entry_source
                ORDER BY total_requests DESC
            """).result_rows
            
            # API分类分析
            api_analysis = self.client.query("""
                SELECT 
                    api_category,
                    count(*) as total_requests,
                    sum(case when not is_success then 1 else 0 end) as error_requests,
                    sum(case when has_anomaly then 1 else 0 end) as anomaly_requests
                FROM dwd_nginx_enriched 
                GROUP BY api_category
                ORDER BY total_requests DESC
            """).result_rows
            
            return {
                'platform_analysis': [
                    {
                        'platform': row[0],
                        'total_requests': row[1],
                        'success_requests': row[2],
                        'success_rate': round((row[2] / row[1] * 100) if row[1] > 0 else 0, 2),
                        'avg_response_time': round(row[3] or 0, 3)
                    }
                    for row in platform_analysis
                ],
                'entry_source_analysis': [
                    {
                        'entry_source': row[0],
                        'total_requests': row[1],
                        'avg_response_time': round(row[2] or 0, 3),
                        'slow_requests': row[3],
                        'slow_rate': round((row[3] / row[1] * 100) if row[1] > 0 else 0, 2)
                    }
                    for row in entry_analysis
                ],
                'api_category_analysis': [
                    {
                        'api_category': row[0],
                        'total_requests': row[1],
                        'error_requests': row[2],
                        'error_rate': round((row[2] / row[1] * 100) if row[1] > 0 else 0, 2),
                        'anomaly_requests': row[3],
                        'anomaly_rate': round((row[3] / row[1] * 100) if row[1] > 0 else 0, 2)
                    }
                    for row in api_analysis
                ]
            }
        except Exception as e:
            raise Exception(f"多维度分析失败: {e}")

def main():
    """命令行入口"""
    parser = argparse.ArgumentParser(description='ClickHouse数据处理器')
    parser.add_argument('--csv-path', type=str, help='CSV文件路径')
    parser.add_argument('--stats', action='store_true', help='显示数据统计')
    parser.add_argument('--analyze', action='store_true', help='多维度分析')
    parser.add_argument('--batch-size', type=int, default=1000, help='批处理大小')
    parser.add_argument('--host', default='localhost', help='ClickHouse主机')
    parser.add_argument('--port', type=int, default=8123, help='ClickHouse端口')
    
    args = parser.parse_args()
    
    processor = ClickHouseProcessor(host=args.host, port=args.port)
    
    # 加载CSV数据
    if args.csv_path:
        if not os.path.exists(args.csv_path):
            print(f"错误: CSV文件不存在: {args.csv_path}")
            return
        
        try:
            count = processor.load_csv_to_clickhouse(args.csv_path, args.batch_size)
            print(f"成功加载 {count} 条数据")
        except Exception as e:
            print(f"加载失败: {e}")
            return
    
    # 显示统计信息
    if args.stats:
        try:
            stats = processor.get_statistics()
            print("\n=== ClickHouse数据统计 ===")
            print(f"总记录数: {stats['total_records']:,}")
            print(f"成功率: {stats['success_rate']:.1f}%")
            print(f"慢请求率: {stats['slow_rate']:.1f}%")
            print(f"异常率: {stats['anomaly_rate']:.1f}%")
            print(f"平均数据质量评分: {stats['avg_quality_score']}")
            
            if stats['time_range']['start']:
                print(f"时间范围: {stats['time_range']['start']} ~ {stats['time_range']['end']}")
            
            print("\n平台分布:")
            for platform, count in stats['platform_distribution'].items():
                print(f"  {platform}: {count:,}")
                
        except Exception as e:
            print(f"获取统计信息失败: {e}")
    
    # 多维度分析
    if args.analyze:
        try:
            analysis = processor.analyze_dimensions()
            
            print("\n=== ClickHouse多维度分析 ===")
            
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
    
    # 默认加载配置文件中的CSV
    if not any([args.csv_path, args.stats, args.analyze]):
        from config.settings import DATA_SOURCE
        default_csv = DATA_SOURCE['default_csv_path']
        if os.path.exists(default_csv):
            print(f"使用默认CSV文件: {default_csv}")
            try:
                count = processor.load_csv_to_clickhouse(str(default_csv), args.batch_size)
                print(f"成功加载 {count} 条数据")
                
                # 显示统计
                stats = processor.get_statistics()
                print(f"\nClickHouse中共有 {stats['total_records']:,} 条记录")
                
            except Exception as e:
                print(f"处理失败: {e}")
        else:
            print(f"默认CSV文件不存在: {default_csv}")
            print("请使用 --csv-path 指定CSV文件路径")

if __name__ == "__main__":
    main()