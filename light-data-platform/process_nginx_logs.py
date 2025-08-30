# -*- coding: utf-8 -*-
"""
Nginx日志处理统一入口
支持全量和增量处理，状态管理和错误恢复
"""

import os
import sys
import json
import argparse
from datetime import datetime, date
from pathlib import Path

# 添加项目路径
sys.path.append(str(Path(__file__).parent.parent))
sys.path.append(str(Path(__file__).parent))

from scripts.clickhouse_pipeline import ClickHousePipeline
from scripts.incremental_manager import IncrementalManager
from self.self_00_02_utils import log_info

class NginxLogManager:
    """Nginx日志处理管理器"""
    
    def __init__(self):
        self.pipeline = ClickHousePipeline()
        self.increment_manager = IncrementalManager()
    
    def process_logs(self, log_dir: str, target_date: date = None, mode: str = 'incremental') -> dict:
        """处理nginx日志"""
        log_info("="*60)
        log_info(f"🚀 开始Nginx日志处理")
        log_info(f"📁 日志目录: {log_dir}")
        log_info(f"📅 目标日期: {target_date or '全部'}")
        log_info(f"🔄 处理模式: {mode}")
        log_info("="*60)
        
        try:
            # 检查日志目录
            if not os.path.exists(log_dir):
                raise Exception(f"日志目录不存在: {log_dir}")
            
            # 执行数据管道
            result = self.pipeline.process_nginx_logs_to_clickhouse(log_dir, target_date, mode)
            
            # 输出结果摘要
            self._print_processing_summary(result)
            
            return result
            
        except Exception as e:
            log_info(f"❌ 处理失败: {e}", level="ERROR")
            return {'status': 'error', 'message': str(e)}
    
    def show_status(self, target_date: date = None) -> dict:
        """显示处理状态"""
        log_info("📊 获取处理状态...")
        
        try:
            if target_date:
                # 显示指定日期状态
                status = self.increment_manager.get_date_status(target_date)
                self._print_date_status(status)
            else:
                # 显示总体状态
                summary = self.increment_manager.get_processing_summary()
                pipeline_status = self.pipeline.get_pipeline_status()
                
                self._print_overall_status(summary, pipeline_status)
            
            return {'status': 'success'}
            
        except Exception as e:
            log_info(f"❌ 获取状态失败: {e}", level="ERROR")
            return {'status': 'error', 'message': str(e)}
    
    def reset_failed(self, target_date: date = None) -> dict:
        """重置失败的文件"""
        log_info("🔄 重置失败文件...")
        
        try:
            self.increment_manager.reset_failed_files(target_date)
            log_info("✅ 失败文件重置完成")
            return {'status': 'success'}
            
        except Exception as e:
            log_info(f"❌ 重置失败: {e}", level="ERROR")
            return {'status': 'error', 'message': str(e)}
    
    def _print_processing_summary(self, result: dict):
        """打印处理结果摘要"""
        log_info("="*60)
        log_info("📋 处理结果摘要")
        log_info("="*60)
        
        status = result.get('status', 'unknown')
        if status == 'completed':
            log_info(f"✅ 处理状态: 完成")
            log_info(f"📈 处理记录数: {result.get('processed', 0):,}")
            log_info(f"📁 成功文件: {result.get('success_files', 0)}")
            log_info(f"❌ 失败文件: {result.get('failed_files', 0)}")
            log_info(f"📊 总文件数: {result.get('total_files', 0)}")
            
            success_rate = 0
            if result.get('total_files', 0) > 0:
                success_rate = result.get('success_files', 0) / result.get('total_files', 0) * 100
            log_info(f"🎯 成功率: {success_rate:.1f}%")
            
        elif status == 'no_files':
            log_info("⚠️  未找到需要处理的日志文件")
        elif status == 'up_to_date':
            log_info("✅ 所有文件都是最新的，无需处理")
        elif status == 'error':
            log_info(f"❌ 处理错误: {result.get('message', '未知错误')}")
        
        log_info("="*60)
    
    def _print_date_status(self, status: dict):
        """打印指定日期状态"""
        log_info("="*50)
        log_info(f"📅 日期 {status['date']} 处理状态")
        log_info("="*50)
        log_info(f"📊 状态: {status['status']}")
        log_info(f"📁 文件总数: {status['files_count']}")
        log_info(f"✅ 已完成: {status['completed_count']}")
        log_info(f"❌ 失败: {status['failed_count']}")
        log_info(f"🔄 处理中: {status['processing_count']}")
        log_info(f"📈 总记录数: {status['total_records']:,}")
        
        # 显示文件详情
        if status.get('files'):
            log_info("\n📋 文件详情:")
            for file_info in status['files']:
                status_icon = "✅" if file_info.get('status') == 'completed' else ("❌" if file_info.get('status') == 'failed' else "🔄")
                log_info(f"  {status_icon} {file_info.get('file', 'unknown')} - {file_info.get('records_count', 0)} 条记录")
        
        log_info("="*50)
    
    def _print_overall_status(self, summary: dict, pipeline_status: dict):
        """打印总体状态"""
        log_info("="*60)
        log_info("🌐 系统总体状态")
        log_info("="*60)
        
        # 处理状态摘要
        log_info("📊 处理状态摘要:")
        log_info(f"  📅 总处理天数: {summary.get('total_dates', 0)}")
        log_info(f"  ✅ 完成天数: {summary.get('completed_dates', 0)}")
        log_info(f"  🎯 完成率: {summary.get('completion_rate', 0):.1f}%")
        log_info(f"  📁 总文件数: {summary.get('total_files', 0)}")
        log_info(f"  ✅ 成功文件: {summary.get('completed_files', 0)}")
        log_info(f"  ❌ 失败文件: {summary.get('failed_files', 0)}")
        log_info(f"  🎯 文件成功率: {summary.get('file_success_rate', 0):.1f}%")
        log_info(f"  📈 总记录数: {summary.get('total_records', 0):,}")
        
        # ClickHouse状态
        if 'clickhouse_stats' in pipeline_status:
            stats = pipeline_status['clickhouse_stats']
            log_info("\n🏪 ClickHouse数据库状态:")
            log_info(f"  📈 记录总数: {stats.get('total_records', 0):,}")
            log_info(f"  ✅ 成功率: {stats.get('success_rate', 0):.1f}%")
            log_info(f"  🐌 慢请求率: {stats.get('slow_rate', 0):.1f}%")
            log_info(f"  ⚠️  异常率: {stats.get('anomaly_rate', 0):.1f}%")
            log_info(f"  📊 平均质量分: {stats.get('avg_quality_score', 0):.2f}")
            
            # 平台分布
            if stats.get('platform_distribution'):
                log_info("\n📱 平台分布:")
                for platform, count in stats['platform_distribution'].items():
                    log_info(f"  {platform}: {count:,} 条记录")
        
        # 健康状态
        health = pipeline_status.get('pipeline_health', 'unknown')
        health_icon = "🟢" if health == 'healthy' else ("🟡" if health == 'no_data' else "🔴")
        log_info(f"\n🏥 管道健康状态: {health_icon} {health}")
        
        log_info("="*60)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='Nginx日志处理管理器 v1.0.0',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 增量处理所有日志
  python process_nginx_logs.py --log-dir /path/to/nginx-logs
  
  # 全量处理指定日期
  python process_nginx_logs.py --log-dir /path/to/nginx-logs --date 2025-08-29 --mode full
  
  # 查看处理状态
  python process_nginx_logs.py --status
  
  # 查看指定日期状态
  python process_nginx_logs.py --status --date 2025-08-29
  
  # 重置失败文件
  python process_nginx_logs.py --reset-failed
        """
    )
    
    # 主要操作参数
    parser.add_argument('--log-dir', '-d', type=str, help='nginx日志根目录')
    parser.add_argument('--date', type=str, help='处理指定日期 (YYYY-MM-DD)')
    parser.add_argument('--mode', type=str, choices=['full', 'incremental'], 
                       default='incremental', help='处理模式 (default: incremental)')
    
    # 状态和管理参数
    parser.add_argument('--status', action='store_true', help='显示处理状态')
    parser.add_argument('--reset-failed', action='store_true', help='重置失败的文件')
    
    # 输出参数
    parser.add_argument('--json', action='store_true', help='以JSON格式输出结果')
    parser.add_argument('--verbose', '-v', action='store_true', help='详细输出')
    
    args = parser.parse_args()
    
    # 创建管理器
    manager = NginxLogManager()
    
    # 解析日期
    target_date = None
    if args.date:
        try:
            target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        except ValueError:
            print(f"❌ 日期格式错误: {args.date}, 应该是 YYYY-MM-DD")
            sys.exit(1)
    
    # 执行操作
    result = None
    
    if args.status:
        # 显示状态
        result = manager.show_status(target_date)
        
    elif args.reset_failed:
        # 重置失败文件
        result = manager.reset_failed(target_date)
        
    else:
        # 处理日志
        if not args.log_dir:
            print("❌ 请指定日志目录 --log-dir")
            parser.print_help()
            sys.exit(1)
            
        result = manager.process_logs(args.log_dir, target_date, args.mode)
    
    # 输出结果
    if args.json and result:
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    
    # 退出码
    if result and result.get('status') in ['success', 'completed', 'up_to_date']:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()