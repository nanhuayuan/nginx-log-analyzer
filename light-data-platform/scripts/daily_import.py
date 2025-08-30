# -*- coding: utf-8 -*-
"""
日常数据导入脚本
支持增量导入、重复检测、数据备份
"""

import os
import sys
import shutil
from datetime import datetime, timedelta
from pathlib import Path
import argparse

# 添加项目路径
sys.path.append(str(Path(__file__).parent.parent))

from data_pipeline.ods_processor import OdsProcessor
from data_pipeline.dwd_processor import DwdProcessor
from config.settings import DATA_SOURCE

class DailyImportManager:
    """日常数据导入管理器"""
    
    def __init__(self):
        self.ods_processor = OdsProcessor()
        self.dwd_processor = DwdProcessor()
        self.backup_dir = Path("backups")
        self.backup_dir.mkdir(exist_ok=True)
    
    def backup_database(self):
        """备份数据库"""
        db_path = Path("database/nginx_analytics.db")
        if db_path.exists():
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = self.backup_dir / f"nginx_analytics_backup_{timestamp}.db"
            shutil.copy2(db_path, backup_path)
            print(f"数据库已备份到: {backup_path}")
            return backup_path
        return None
    
    def import_csv_data(self, csv_path: str, enable_backup: bool = True):
        """导入CSV数据"""
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV文件不存在: {csv_path}")
        
        print(f"开始导入数据: {csv_path}")
        
        # 备份数据库
        if enable_backup:
            backup_path = self.backup_database()
        
        try:
            # 获取导入前统计
            before_stats = self.ods_processor.get_ods_statistics()
            print(f"导入前记录数: {before_stats['total_records']}")
            
            # 导入ODS数据
            ods_count = self.ods_processor.load_csv_to_ods(csv_path)
            print(f"新增ODS记录: {ods_count}")
            
            # 富化到DWD层
            if ods_count > 0:
                dwd_count = self.dwd_processor.process_ods_to_dwd()
                print(f"新增DWD记录: {dwd_count}")
            
            # 获取导入后统计
            after_stats = self.ods_processor.get_ods_statistics()
            print(f"导入后记录数: {after_stats['total_records']}")
            
            return True
            
        except Exception as e:
            print(f"导入失败: {e}")
            # 如果失败且有备份，询问是否恢复
            if enable_backup and backup_path and backup_path.exists():
                response = input("是否从备份恢复数据库? (y/n): ")
                if response.lower() == 'y':
                    self.restore_from_backup(backup_path)
            raise e
    
    def restore_from_backup(self, backup_path: Path):
        """从备份恢复数据库"""
        db_path = Path("database/nginx_analytics.db")
        shutil.copy2(backup_path, db_path)
        print(f"已从备份恢复: {backup_path}")
    
    def cleanup_old_backups(self, keep_days: int = 7):
        """清理旧备份文件"""
        cutoff_date = datetime.now() - timedelta(days=keep_days)
        
        for backup_file in self.backup_dir.glob("nginx_analytics_backup_*.db"):
            # 从文件名提取时间戳
            try:
                timestamp_str = backup_file.stem.split("_")[-2] + backup_file.stem.split("_")[-1]
                file_date = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")
                
                if file_date < cutoff_date:
                    backup_file.unlink()
                    print(f"删除旧备份: {backup_file}")
                    
            except (ValueError, IndexError):
                print(f"无法解析备份文件时间戳: {backup_file}")
    
    def get_import_statistics(self):
        """获取导入统计信息"""
        ods_stats = self.ods_processor.get_ods_statistics()
        dwd_stats = self.dwd_processor.get_dwd_statistics()
        
        return {
            'database_size_mb': Path("database/nginx_analytics.db").stat().st_size / (1024*1024),
            'ods_records': ods_stats['total_records'],
            'dwd_records': dwd_stats['total_records'],
            'data_quality_score': dwd_stats['avg_quality_score'],
            'time_range': ods_stats.get('time_range', {}),
            'platform_distribution': dwd_stats['platform_distribution']
        }
    
    def auto_import_from_directory(self, watch_dir: str, pattern: str = "*.csv"):
        """监控目录自动导入"""
        watch_path = Path(watch_dir)
        if not watch_path.exists():
            print(f"监控目录不存在: {watch_dir}")
            return
        
        # 查找新的CSV文件
        csv_files = list(watch_path.glob(pattern))
        
        if not csv_files:
            print(f"在 {watch_dir} 中未发现CSV文件")
            return
        
        # 按修改时间排序，处理最新的文件
        csv_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        
        for csv_file in csv_files:
            try:
                print(f"\n处理文件: {csv_file}")
                self.import_csv_data(str(csv_file))
                
                # 移动已处理的文件到processed目录
                processed_dir = watch_path / "processed"
                processed_dir.mkdir(exist_ok=True)
                processed_file = processed_dir / f"{csv_file.stem}_processed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                shutil.move(csv_file, processed_file)
                print(f"文件已移动到: {processed_file}")
                
            except Exception as e:
                print(f"处理文件失败 {csv_file}: {e}")

def main():
    """命令行入口"""
    parser = argparse.ArgumentParser(description='日常数据导入管理器')
    parser.add_argument('--csv-path', type=str, help='指定CSV文件路径')
    parser.add_argument('--watch-dir', type=str, help='监控目录路径')
    parser.add_argument('--stats', action='store_true', help='显示数据库统计')
    parser.add_argument('--backup', action='store_true', help='仅备份数据库')
    parser.add_argument('--cleanup', type=int, default=7, help='清理N天前的备份文件')
    parser.add_argument('--no-backup', action='store_true', help='导入时不备份')
    
    args = parser.parse_args()
    
    manager = DailyImportManager()
    
    try:
        if args.stats:
            # 显示统计信息
            stats = manager.get_import_statistics()
            print("\n=== 数据库统计信息 ===")
            print(f"数据库大小: {stats['database_size_mb']:.2f} MB")
            print(f"ODS记录数: {stats['ods_records']:,}")
            print(f"DWD记录数: {stats['dwd_records']:,}")
            print(f"数据质量评分: {stats['data_quality_score']}")
            
            if stats['time_range'].get('start'):
                print(f"数据时间范围: {stats['time_range']['start']} ~ {stats['time_range']['end']}")
            
            print("\n平台分布:")
            for platform, count in stats['platform_distribution'].items():
                print(f"  {platform}: {count:,}")
        
        elif args.backup:
            # 仅备份数据库
            manager.backup_database()
        
        elif args.csv_path:
            # 导入指定CSV文件
            manager.import_csv_data(args.csv_path, not args.no_backup)
            
            # 显示导入后统计
            stats = manager.get_import_statistics()
            print(f"\n导入完成，当前数据库: {stats['database_size_mb']:.2f} MB, {stats['dwd_records']:,} 条记录")
        
        elif args.watch_dir:
            # 监控目录自动导入
            manager.auto_import_from_directory(args.watch_dir)
        
        else:
            # 默认使用配置文件中的CSV路径
            default_csv = DATA_SOURCE['default_csv_path']
            if os.path.exists(default_csv):
                print(f"使用默认CSV文件: {default_csv}")
                manager.import_csv_data(str(default_csv), not args.no_backup)
            else:
                print("请指定CSV文件路径或监控目录")
                print("使用 --help 查看详细参数")
        
        # 清理旧备份
        if args.cleanup > 0:
            manager.cleanup_old_backups(args.cleanup)
            
    except KeyboardInterrupt:
        print("\n导入已取消")
    except Exception as e:
        print(f"执行失败: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()