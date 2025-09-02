#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Docker Volume管理脚本
用于管理数据持久化存储卷
"""

import subprocess
import sys
import argparse
from pathlib import Path

class VolumeManager:
    """Docker Volume管理器"""
    
    def __init__(self):
        self.volumes = [
            'nginx-analytics-warehouse_clickhouse_data',
            'nginx-analytics-warehouse_clickhouse_logs', 
            'nginx-analytics-warehouse_grafana_data',
            'nginx-analytics-warehouse_grafana_logs',
            'nginx-analytics-warehouse_postgres_data',
            'nginx-analytics-warehouse_redis_data',
            'nginx-analytics-warehouse_superset_home'
        ]
    
    def list_volumes(self):
        """列出所有相关的Docker volumes"""
        print("=== 数据持久化存储卷列表 ===")
        
        try:
            # 获取所有volumes
            result = subprocess.run(['docker', 'volume', 'ls'], 
                                  capture_output=True, text=True, check=True)
            
            print("已创建的存储卷:")
            volume_lines = result.stdout.strip().split('\n')[1:]  # 跳过标题行
            found_volumes = []
            
            for line in volume_lines:
                parts = line.split()
                if len(parts) >= 2:
                    volume_name = parts[1]
                    if any(vol in volume_name for vol in ['clickhouse', 'grafana', 'postgres', 'redis', 'superset']):
                        found_volumes.append(volume_name)
                        size_info = self._get_volume_size(volume_name)
                        print(f"  ✓ {volume_name} {size_info}")
            
            if not found_volumes:
                print("  (无相关存储卷，请先启动服务)")
                
            return found_volumes
            
        except subprocess.CalledProcessError as e:
            print(f"❌ 获取volume列表失败: {e}")
            return []
    
    def _get_volume_size(self, volume_name):
        """获取volume大小信息"""
        try:
            # 使用docker system df获取volume大小
            result = subprocess.run(['docker', 'system', 'df', '-v'], 
                                  capture_output=True, text=True, check=True)
            
            lines = result.stdout.split('\n')
            for line in lines:
                if volume_name in line:
                    parts = line.split()
                    if len(parts) >= 3:
                        return f"({parts[2]})"
            return "(未知大小)"
            
        except:
            return ""
    
    def inspect_volume(self, volume_name):
        """检查指定volume的详细信息"""
        try:
            result = subprocess.run(['docker', 'volume', 'inspect', volume_name],
                                  capture_output=True, text=True, check=True)
            
            print(f"\n=== {volume_name} 详细信息 ===")
            print(result.stdout)
            
        except subprocess.CalledProcessError as e:
            print(f"❌ 检查volume失败: {e}")
    
    def backup_volumes(self, backup_dir):
        """备份所有数据卷"""
        backup_path = Path(backup_dir)
        backup_path.mkdir(parents=True, exist_ok=True)
        
        print(f"=== 备份数据到: {backup_path.absolute()} ===")
        
        volumes = self.list_volumes()
        success_count = 0
        
        for volume in volumes:
            try:
                backup_file = backup_path / f"{volume}.tar"
                
                print(f"备份 {volume}...")
                
                # 使用临时容器备份volume
                cmd = [
                    'docker', 'run', '--rm',
                    '-v', f'{volume}:/data',
                    '-v', f'{backup_path.absolute()}:/backup',
                    'alpine:latest',
                    'tar', 'czf', f'/backup/{volume}.tar.gz', '-C', '/data', '.'
                ]
                
                result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                
                print(f"  ✓ 备份完成: {volume}.tar.gz")
                success_count += 1
                
            except subprocess.CalledProcessError as e:
                print(f"  ❌ 备份失败 {volume}: {e}")
        
        print(f"\n备份完成: {success_count}/{len(volumes)} 个卷成功备份")
        return success_count == len(volumes)
    
    def restore_volumes(self, backup_dir):
        """从备份恢复数据卷"""
        backup_path = Path(backup_dir)
        
        if not backup_path.exists():
            print(f"❌ 备份目录不存在: {backup_path}")
            return False
        
        print(f"=== 从备份恢复数据: {backup_path.absolute()} ===")
        print("⚠️  警告: 这将覆盖现有数据!")
        
        confirm = input("确认继续恢复? (yes/No): ")
        if confirm.lower() != 'yes':
            print("恢复操作已取消")
            return False
        
        backup_files = list(backup_path.glob("*.tar.gz"))
        success_count = 0
        
        for backup_file in backup_files:
            volume_name = backup_file.stem.replace('.tar', '')
            
            try:
                print(f"恢复 {volume_name}...")
                
                # 创建volume (如果不存在)
                subprocess.run(['docker', 'volume', 'create', volume_name],
                             capture_output=True, text=True)
                
                # 使用临时容器恢复volume
                cmd = [
                    'docker', 'run', '--rm',
                    '-v', f'{volume_name}:/data',
                    '-v', f'{backup_path.absolute()}:/backup',
                    'alpine:latest',
                    'sh', '-c', f'cd /data && tar xzf /backup/{backup_file.name}'
                ]
                
                result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                
                print(f"  ✓ 恢复完成: {volume_name}")
                success_count += 1
                
            except subprocess.CalledProcessError as e:
                print(f"  ❌ 恢复失败 {volume_name}: {e}")
        
        print(f"\n恢复完成: {success_count}/{len(backup_files)} 个卷成功恢复")
        return success_count == len(backup_files)
    
    def clean_volumes(self):
        """清理未使用的volumes"""
        print("=== 清理未使用的存储卷 ===")
        print("⚠️  警告: 这将删除所有未使用的Docker volumes!")
        
        confirm = input("确认继续清理? (yes/No): ")
        if confirm.lower() != 'yes':
            print("清理操作已取消")
            return False
        
        try:
            result = subprocess.run(['docker', 'volume', 'prune', '-f'],
                                  capture_output=True, text=True, check=True)
            
            print("✓ 清理完成")
            print(result.stdout)
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ 清理失败: {e}")
            return False
    
    def show_disk_usage(self):
        """显示Docker存储使用情况"""
        print("=== Docker存储使用情况 ===")
        
        try:
            result = subprocess.run(['docker', 'system', 'df', '-v'],
                                  capture_output=True, text=True, check=True)
            
            print(result.stdout)
            
        except subprocess.CalledProcessError as e:
            print(f"❌ 获取存储信息失败: {e}")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='Docker Volume数据持久化管理工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python manage_volumes.py list                    # 列出所有存储卷
  python manage_volumes.py backup ./backup         # 备份所有数据
  python manage_volumes.py restore ./backup        # 从备份恢复数据
  python manage_volumes.py usage                   # 显示存储使用情况
  python manage_volumes.py clean                   # 清理未使用的卷
        """
    )
    
    parser.add_argument('action', 
                       choices=['list', 'backup', 'restore', 'clean', 'usage'],
                       help='执行的操作')
    
    parser.add_argument('path', nargs='?', 
                       help='备份目录路径 (backup/restore时需要)')
    
    args = parser.parse_args()
    
    manager = VolumeManager()
    
    if args.action == 'list':
        manager.list_volumes()
        
    elif args.action == 'backup':
        if not args.path:
            print("❌ 请指定备份目录路径")
            sys.exit(1)
        manager.backup_volumes(args.path)
        
    elif args.action == 'restore':
        if not args.path:
            print("❌ 请指定备份目录路径")
            sys.exit(1)
        manager.restore_volumes(args.path)
        
    elif args.action == 'clean':
        manager.clean_volumes()
        
    elif args.action == 'usage':
        manager.show_disk_usage()

if __name__ == "__main__":
    main()