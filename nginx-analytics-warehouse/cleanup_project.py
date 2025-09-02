#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
项目清理脚本 - 删除无用文件和目录
Project Cleanup Script - Remove unnecessary files and directories
"""

import os
import shutil
from pathlib import Path
import fnmatch

class ProjectCleaner:
    """项目清理器"""
    
    def __init__(self):
        self.base_dir = Path(__file__).parent
        
        # 定义需要保留的核心文件和目录
        self.keep_patterns = [
            # 核心目录
            'docker/',
            'processors/',
            'ddl/',
            'nginx_logs/',
            'docs/',
            'config/',
            'scripts/',
            
            # 核心文件
            'README.md',
            'DIRECTORY_STRUCTURE.md',
            'manage.py',
            'init_directories.py',
            '.gitignore',
            '.env.example',
            
            # 数据目录（但清理内容）
            'data/',
            'logs/',
            'backup/',
        ]
        
        # 定义需要删除的文件（精确匹配）
        self.delete_files = [
            # 旧的配置文件
            'docker-compose-core.yml',
            'docker-compose-full.yml', 
            'docker-compose-simple.yml',
            'docker-compose.yml',  # 根目录的，保留docker/docker-compose.yml
            'Dockerfile.processor',
            'docker-entrypoint.sh',
            'start.bat',
            'start.sh',
            
            # 测试和演示文件  
            'test_clickhouse.py',
            'test_complete_pipeline.py',
            'test_simple.py',
            'simple_test.py',
            'quick_setup_comparison.py',
            
            # 设置和创建脚本
            'setup_clickhouse_pipeline.py',
            'setup_grafana_datasource.py',
            'create_complete_schema.py',
            'init_all_tables.sh',
            
            # 文档碎片
            'ALTINITY_CLICKHOUSE_CONFIG.md',
            'DEPLOYMENT_STATUS.md',
            'DOCKER_COMPOSE_SETUP_COMPLETE.md',
            'FINAL_CONNECTION_GUIDE.md',
            'GRAFANA_CLICKHOUSE_SETUP.md',
            'GRAFANA_VS_SUPERSET_COMPARISON.md', 
            'SUPERSET_CLICKHOUSE_CONNECTION.md',
        ]
        
        # 需要完全删除的目录
        self.delete_directories = [
            'volumes/',        # 旧的数据目录
            'etl/',           # 重复的ETL目录
            'grafana/',       # 重复的grafana目录(不是config/grafana)
            '__pycache__/',
            '.pytest_cache/',
            'temp/',
            'tmp/',
        ]
    
    def should_keep_file(self, file_path):
        """判断是否应该保留文件"""
        relative_path = file_path.relative_to(self.base_dir)
        path_str = str(relative_path).replace('\\', '/')
        
        # 检查是否在删除列表中
        if file_path.name in self.delete_files:
            return False
        
        # 特殊处理：processors目录中的文件都保留
        if path_str.startswith('processors/'):
            return True
            
        # 特殊处理：ddl目录中的文件都保留
        if path_str.startswith('ddl/'):
            return True
            
        # 特殊处理：docker目录只保留特定文件
        if path_str.startswith('docker/'):
            return file_path.name in ['docker-compose.yml', '.env', '.env.example']
        
        # 特殊处理：config目录中的文件保留
        if path_str.startswith('config/'):
            return True
        
        # 特殊处理：docs目录中的文件保留
        if path_str.startswith('docs/'):
            return True
            
        # 特殊处理：scripts目录中的文件保留
        if path_str.startswith('scripts/'):
            return True
        
        # 核心文件保留
        core_files = [
            'README.md',
            'DIRECTORY_STRUCTURE.md', 
            'manage.py',
            'init_directories.py',
            '.gitignore',
            'requirements.txt',
            'cleanup_project.py'  # 保留清理脚本本身
        ]
        
        if file_path.name in core_files:
            return True
        
        return False
    
    def should_delete_directory(self, dir_path):
        """判断是否应该删除整个目录"""
        relative_path = dir_path.relative_to(self.base_dir)
        path_str = str(relative_path).replace('\\', '/')
        
        for delete_dir in self.delete_directories:
            if path_str == delete_dir.rstrip('/') or path_str.startswith(delete_dir):
                return True
        
        return False
    
    def clean_data_directories(self):
        """清理数据目录（保留结构，清理内容）"""
        print("=== 清理数据目录内容 ===")
        
        data_dirs = ['data', 'logs', 'backup']
        for data_dir in data_dirs:
            data_path = self.base_dir / data_dir
            if data_path.exists():
                print(f"清理 {data_dir}/ 目录内容...")
                
                # 保留.gitkeep文件
                for item in data_path.rglob('*'):
                    if item.is_file() and item.name != '.gitkeep':
                        try:
                            item.unlink()
                        except Exception as e:
                            print(f"  删除文件失败: {item} - {e}")
                
                # 删除空目录（保留顶级目录和包含.gitkeep的目录）
                for item in data_path.rglob('*'):
                    if item.is_dir() and not any(item.iterdir()):
                        try:
                            item.rmdir()
                        except Exception as e:
                            print(f"  删除目录失败: {item} - {e}")
                
                print(f"  [OK] {data_dir}/ 清理完成")
    
    def clean_project(self, dry_run=True):
        """清理项目"""
        print("=== 项目清理工具 ===")
        if dry_run:
            print("*** 预览模式 - 不会实际删除文件 ***")
        else:
            print("*** 实际删除模式 ***")
        
        deleted_files = []
        deleted_dirs = []
        kept_files = []
        
        # 遍历所有文件
        for item in self.base_dir.rglob('*'):
            if item == Path(__file__):  # 跳过自己
                continue
                
            try:
                relative_path = item.relative_to(self.base_dir)
                
                if item.is_dir():
                    if self.should_delete_directory(item):
                        deleted_dirs.append(relative_path)
                        if not dry_run:
                            shutil.rmtree(item)
                        print(f"[DELETE DIR] {relative_path}")
                        continue
                
                elif item.is_file():
                    if not self.should_keep_file(item):
                        deleted_files.append(relative_path)
                        if not dry_run:
                            item.unlink()
                        print(f"[DELETE FILE] {relative_path}")
                    else:
                        kept_files.append(relative_path)
                        if dry_run:
                            print(f"[KEEP] {relative_path}")
                            
            except Exception as e:
                print(f"[ERROR] 处理 {item} 时出错: {e}")
        
        # 清理数据目录
        if not dry_run:
            self.clean_data_directories()
        
        print("\n=== 清理统计 ===")
        print(f"删除文件数: {len(deleted_files)}")
        print(f"删除目录数: {len(deleted_dirs)}")
        print(f"保留文件数: {len(kept_files)}")
        
        if dry_run:
            print("\n运行 python cleanup_project.py --execute 执行实际删除")
        else:
            print("\n清理完成！")
        
        return len(deleted_files), len(deleted_dirs)

def main():
    """主函数"""
    import sys
    
    cleaner = ProjectCleaner()
    
    # 检查参数
    dry_run = True
    force = False
    
    for arg in sys.argv[1:]:
        if arg == '--execute':
            dry_run = False
        elif arg == '--force':
            force = True
    
    if not dry_run and not force:
        print("确认要执行实际删除吗？这个操作不可逆！")
        try:
            confirm = input("输入 'YES' 确认: ")
            if confirm != 'YES':
                print("操作已取消")
                return
        except EOFError:
            print("无法获取用户输入，请使用 --force 参数强制执行")
            return
    
    cleaner.clean_project(dry_run)

if __name__ == "__main__":
    main()