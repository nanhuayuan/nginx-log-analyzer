#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
nginx-analytics-warehouse 统一管理脚本
Unified management script for nginx-analytics-warehouse
"""

import argparse
import sys
import subprocess
import os
from pathlib import Path

class WarehouseManager:
    """数据仓库管理器"""
    
    def __init__(self):
        self.base_dir = Path(__file__).parent
        self.docker_dir = self.base_dir / "docker"
        self.processors_dir = self.base_dir / "processors"
        self.data_dir = self.base_dir / "data"
        self.logs_dir = self.base_dir / "logs"
        
    def init_structure(self):
        """初始化目录结构"""
        print("=== 初始化目录结构 ===")
        
        init_script = self.base_dir / "init_directories.py"
        if not init_script.exists():
            print("❌ 找不到初始化脚本")
            return False
        
        try:
            result = subprocess.run([sys.executable, str(init_script)], 
                                  cwd=self.base_dir, check=True)
            return True
        except subprocess.CalledProcessError as e:
            print(f"❌ 初始化失败: {e}")
            return False
    
    def start_services(self):
        """启动所有服务"""
        print("=== 启动服务 ===")
        
        docker_compose_file = self.docker_dir / "docker-compose.yml"
        if not docker_compose_file.exists():
            print(f"❌ 找不到docker-compose文件: {docker_compose_file}")
            print("请先运行: python manage.py init")
            return False
        
        # 检查环境变量文件
        env_file = self.docker_dir / ".env"
        if not env_file.exists():
            print("⚠️  未找到.env文件，复制默认配置...")
            env_example = self.docker_dir / ".env.example"
            if env_example.exists():
                import shutil
                shutil.copy(env_example, env_file)
                print("✓ 已创建.env文件，请检查配置")
        
        try:
            # 启动服务
            result = subprocess.run([
                'docker-compose', 'up', '-d'
            ], cwd=self.docker_dir, check=True, capture_output=True, text=True)
            
            print("✅ 服务启动完成")
            
            # 显示服务状态
            self.show_services_status()
            
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ 服务启动失败: {e}")
            if e.stderr:
                print(f"错误详情: {e.stderr}")
            return False
    
    def stop_services(self):
        """停止所有服务"""
        print("=== 停止服务 ===")
        
        docker_compose_file = self.docker_dir / "docker-compose.yml"
        if not docker_compose_file.exists():
            print(f"❌ 找不到docker-compose文件: {docker_compose_file}")
            return False
        
        try:
            subprocess.run([
                'docker-compose', 'down'
            ], cwd=self.docker_dir, check=True)
            
            print("✅ 服务已停止")
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ 停止服务失败: {e}")
            return False
    
    def restart_services(self):
        """重启所有服务"""
        print("=== 重启服务 ===")
        self.stop_services()
        return self.start_services()
    
    def show_services_status(self):
        """显示服务状态"""
        print("\n=== 服务状态 ===")
        
        try:
            # 显示容器状态
            result = subprocess.run([
                'docker', 'ps', '--filter', 'name=nginx-analytics'
            ], capture_output=True, text=True)
            
            if result.stdout:
                print(result.stdout)
            else:
                print("没有运行中的nginx-analytics容器")
            
            print("\n访问地址:")
            print("  ClickHouse: http://localhost:8123")
            print("  Grafana:    http://localhost:3000 (admin/admin123)")
            print("  Superset:   http://localhost:8088 (admin/admin123)")
            
        except Exception as e:
            print(f"❌ 获取状态失败: {e}")
    
    def init_database(self):
        """初始化数据库"""
        print("=== 初始化数据库 ===")
        
        init_db_script = self.processors_dir / "init_database.py"
        if not init_db_script.exists():
            print(f"❌ 找不到数据库初始化脚本: {init_db_script}")
            return False
        
        try:
            result = subprocess.run([
                sys.executable, str(init_db_script)
            ], cwd=self.processors_dir, check=True)
            
            print("✅ 数据库初始化完成")
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ 数据库初始化失败: {e}")
            return False
    
    def process_logs(self, date=None, force=False):
        """处理nginx日志"""
        print("=== 处理nginx日志 ===")
        
        main_script = self.processors_dir / "main_simple.py"
        if not main_script.exists():
            print(f"❌ 找不到主处理脚本: {main_script}")
            return False
        
        try:
            if date:
                cmd = [sys.executable, str(main_script), "process", "--date", date]
                if force:
                    cmd.append("--force")
            else:
                cmd = [sys.executable, str(main_script), "process-all"]
            
            result = subprocess.run(cmd, cwd=self.processors_dir, check=True)
            
            print("✅ 日志处理完成")
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ 日志处理失败: {e}")
            return False
    
    def show_status(self):
        """显示系统整体状态"""
        print("=== 系统状态 ===")
        
        # 显示服务状态
        self.show_services_status()
        
        # 显示数据目录大小
        print("\n=== 数据存储状态 ===")
        if self.data_dir.exists():
            try:
                # 计算各目录大小
                for subdir in self.data_dir.iterdir():
                    if subdir.is_dir():
                        size = self._get_directory_size(subdir)
                        print(f"  {subdir.name}: {self._format_size(size)}")
            except Exception as e:
                print(f"  无法获取存储信息: {e}")
        else:
            print("  数据目录不存在")
        
        # 显示日志处理状态
        print("\n=== 处理状态 ===")
        processed_logs_file = self.processors_dir / "processed_logs_complete.json"
        if processed_logs_file.exists():
            try:
                import json
                with open(processed_logs_file, 'r', encoding='utf-8') as f:
                    processed_data = json.load(f)
                
                print(f"  已处理日志文件: {len(processed_data)}个")
                
                total_ods = sum(data.get('ods_records', 0) for data in processed_data.values())
                total_dwd = sum(data.get('dwd_records', 0) for data in processed_data.values())
                
                print(f"  ODS记录总数: {total_ods:,}")
                print(f"  DWD记录总数: {total_dwd:,}")
                
            except Exception as e:
                print(f"  处理记录读取失败: {e}")
        else:
            print("  未找到处理记录")
    
    def backup_data(self, backup_dir):
        """备份数据"""
        print("=== 数据备份 ===")
        
        manage_volumes_script = self.processors_dir / "manage_volumes.py"
        if not manage_volumes_script.exists():
            print("❌ 找不到volume管理脚本")
            return False
        
        try:
            result = subprocess.run([
                sys.executable, str(manage_volumes_script), "backup", backup_dir
            ], cwd=self.processors_dir, check=True)
            
            print("✅ 备份完成")
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ 备份失败: {e}")
            return False
    
    def restore_data(self, backup_dir):
        """恢复数据"""
        print("=== 数据恢复 ===")
        
        manage_volumes_script = self.processors_dir / "manage_volumes.py"
        if not manage_volumes_script.exists():
            print("❌ 找不到volume管理脚本")
            return False
        
        try:
            result = subprocess.run([
                sys.executable, str(manage_volumes_script), "restore", backup_dir
            ], cwd=self.processors_dir, check=True)
            
            print("✅ 恢复完成")
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ 恢复失败: {e}")
            return False
    
    def clean_data(self):
        """清理数据"""
        print("=== 清理数据 ===")
        
        main_script = self.processors_dir / "main_simple.py"
        if not main_script.exists():
            print("❌ 找不到主处理脚本")
            return False
        
        try:
            result = subprocess.run([
                sys.executable, str(main_script), "clear-all"
            ], cwd=self.processors_dir, check=True)
            
            print("✅ 数据清理完成")
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ 数据清理失败: {e}")
            return False
    
    def _get_directory_size(self, directory):
        """计算目录大小"""
        total = 0
        try:
            for dirpath, dirnames, filenames in os.walk(directory):
                for filename in filenames:
                    filepath = os.path.join(dirpath, filename)
                    if os.path.exists(filepath):
                        total += os.path.getsize(filepath)
        except:
            pass
        return total
    
    def _format_size(self, size_bytes):
        """格式化文件大小"""
        if size_bytes < 1024:
            return f"{size_bytes}B"
        elif size_bytes < 1024**2:
            return f"{size_bytes/1024:.1f}KB"
        elif size_bytes < 1024**3:
            return f"{size_bytes/1024**2:.1f}MB"
        else:
            return f"{size_bytes/1024**3:.1f}GB"

def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='nginx-analytics-warehouse 统一管理工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python manage.py init                     # 初始化目录结构
  python manage.py start                    # 启动所有服务
  python manage.py stop                     # 停止所有服务
  python manage.py restart                  # 重启所有服务
  python manage.py status                   # 显示系统状态
  python manage.py init-db                  # 初始化数据库
  python manage.py process                  # 处理所有未处理日志
  python manage.py process --date 20250422  # 处理指定日期日志
  python manage.py backup ./backup          # 备份数据
  python manage.py restore ./backup         # 恢复数据
  python manage.py clean                    # 清理所有数据
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # 基础管理命令
    subparsers.add_parser('init', help='初始化目录结构')
    subparsers.add_parser('start', help='启动所有服务')
    subparsers.add_parser('stop', help='停止所有服务')
    subparsers.add_parser('restart', help='重启所有服务')
    subparsers.add_parser('status', help='显示系统状态')
    subparsers.add_parser('init-db', help='初始化数据库')
    
    # 日志处理命令
    process_parser = subparsers.add_parser('process', help='处理nginx日志')
    process_parser.add_argument('--date', help='指定日期 (YYYYMMDD)')
    process_parser.add_argument('--force', action='store_true', help='强制重新处理')
    
    # 数据管理命令
    backup_parser = subparsers.add_parser('backup', help='备份数据')
    backup_parser.add_argument('path', help='备份目录路径')
    
    restore_parser = subparsers.add_parser('restore', help='恢复数据')
    restore_parser.add_argument('path', help='备份目录路径')
    
    subparsers.add_parser('clean', help='清理所有数据')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    manager = WarehouseManager()
    
    # 执行对应命令
    if args.command == 'init':
        manager.init_structure()
        
    elif args.command == 'start':
        manager.start_services()
        
    elif args.command == 'stop':
        manager.stop_services()
        
    elif args.command == 'restart':
        manager.restart_services()
        
    elif args.command == 'status':
        manager.show_status()
        
    elif args.command == 'init-db':
        manager.init_database()
        
    elif args.command == 'process':
        manager.process_logs(args.date, args.force)
        
    elif args.command == 'backup':
        manager.backup_data(args.path)
        
    elif args.command == 'restore':
        manager.restore_data(args.path)
        
    elif args.command == 'clean':
        manager.clean_data()

if __name__ == "__main__":
    main()