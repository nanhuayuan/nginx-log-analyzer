#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
项目根目录清理脚本 - 删除无用文件和目录
Root Project Cleanup Script - Remove unnecessary files and directories
"""

import os
import shutil
from pathlib import Path
import fnmatch

class RootProjectCleaner:
    """根目录项目清理器"""
    
    def __init__(self):
        self.base_dir = Path(__file__).parent
        
        # 定义需要保留的核心目录
        self.keep_directories = [
            'nginx-analytics-warehouse/',
            'light-data-platform/',
            'aliyun-log-analyzer/',
            'aliyun-log-download/',
            'n9e-daily/',
            'self-log-download/',
        ]
        
        # 定义需要保留的核心文件
        self.keep_files = [
            'CLAUDE.md',
            '__init__.py',
            '.gitignore',
            'README.md',
            'requirements.txt',
            'cleanup_root_project.py'  # 保留清理脚本本身
        ]
        
        # 定义需要删除的文件（精确匹配）
        self.delete_files = [
            'docker-compose-simple-fixed.yml',  # 应该在docker目录中
            'python_code_cleaner.py',  # 工具脚本，不需要
            'Dprojectnginx-log-analyzerlight-data-platformweb_appbusiness_routes.py',  # 错误路径文件
        ]
        
        # 需要完全删除的目录
        self.delete_directories = [
            'data/',           # 临时数据目录，包含大量分析结果
            'volumes/',        # Docker卷目录，应该使用本地目录
        ]
        
        # self目录中需要删除的文件类型
        self.self_delete_patterns = [
            # 备份文件
            '*_backup_*.py',
            # 测试文件
            'test_*.py',
            # 调试文件
            'debug_*.py',
            'demo_*.py',
            'diagnose_*.py',
            # 验证文件
            'validate_*.py',
            'verify_*.py',
            # 旧版本文件
            '*_v[0-9]*_*.py',
            '*_v[0-9]*.py',
            '*—*.py',  # 包含特殊字符的文件
            # 优化报告文档（保留核心代码）
            '*README.md',
            '*REPORT*.md',
            '*SUMMARY.md', 
            '*ANALYSIS.md',
            '*OPTIMIZATION*.md',
            '*FIX*.md',
            '*EXPLANATION.md',
            '*COMPARISON*.md',
            '*ENHANCEMENT*.md',
            '*.txt',
            '*.tsx',  # 前端文件不属于Python项目
        ]
    
    def should_keep_file(self, file_path):
        """判断是否应该保留文件"""
        relative_path = file_path.relative_to(self.base_dir)
        path_str = str(relative_path).replace('\\', '/')
        file_name = file_path.name
        
        # 检查是否在删除列表中
        if file_name in self.delete_files:
            return False
        
        # 检查是否在保留列表中
        if file_name in self.keep_files:
            return True
            
        # 特殊处理：self目录中的文件
        if path_str.startswith('self/'):
            # 保留核心分析器文件（不带版本号的主文件）
            core_analyzers = [
                'self_00_01_constants.py',
                'self_00_02_utils.py', 
                'self_00_03_log_parser.py',
                'self_00_04_excel_processor.py',
                'self_00_05_sampling_algorithms.py',
                'self_01_api_analyzer.py',
                'self_02_service_analyzer.py',
                'self_03_slow_requests_analyzer.py',
                'self_04_status_analyzer.py',
                'self_05_time_dimension_analyzer.py',
                'self_06_performance_stability_analyzer.py',
                'self_07_generate_summary_report_analyzer.py',
                'self_08_ip_analyzer.py',
                'self_09_main_nginx_log_analyzer.py',
                'self_10_request_header_analyzer.py',
                'self_11_header_performance_analyzer.py',
                'self_13_interface_error_analyzer.py',
                '__init__.py'
            ]
            
            if file_name in core_analyzers:
                return True
                
            # 检查是否匹配删除模式
            for pattern in self.self_delete_patterns:
                if fnmatch.fnmatch(file_name, pattern):
                    return False
                    
            # self目录中的其他文件默认删除
            return False
        
        # 其他目录的文件暂时保留（按用户要求）
        return True
    
    def should_delete_directory(self, dir_path):
        """判断是否应该删除整个目录"""
        relative_path = dir_path.relative_to(self.base_dir)
        path_str = str(relative_path).replace('\\', '/')
        
        for delete_dir in self.delete_directories:
            if path_str == delete_dir.rstrip('/') or path_str.startswith(delete_dir):
                return True
        
        return False
    
    def clean_data_directory(self):
        """清理data目录（保留结构，清理内容）"""
        print("=== 清理data目录内容 ===")
        
        data_dir = self.base_dir / 'data'
        if data_dir.exists():
            print(f"删除data目录: {data_dir}")
            try:
                shutil.rmtree(data_dir)
                print("[OK] data目录删除完成")
            except Exception as e:
                print(f"[ERROR] 删除data目录失败: {e}")
    
    def clean_project(self, dry_run=True):
        """清理项目"""
        print("=== 根目录项目清理工具 ===")
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
                            print(f"[KEEP] {str(relative_path).replace(chr(0xf03a), ':')}")
                            
            except Exception as e:
                print(f"[ERROR] 处理 {str(item).replace(chr(0xf03a), ':')} 时出错: {str(e).replace(chr(0xf03a), ':')}")
        
        print("\n=== 清理统计 ===")
        print(f"删除文件数: {len(deleted_files)}")
        print(f"删除目录数: {len(deleted_dirs)}")
        print(f"保留文件数: {len(kept_files)}")
        
        if dry_run:
            print("\n运行 python cleanup_root_project.py --execute 执行实际删除")
        else:
            print("\n清理完成！")
        
        return len(deleted_files), len(deleted_dirs)

def main():
    """主函数"""
    import sys
    
    cleaner = RootProjectCleaner()
    
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