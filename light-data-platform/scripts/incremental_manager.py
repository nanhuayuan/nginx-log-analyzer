# -*- coding: utf-8 -*-
"""
增量处理管理器 - 处理状态追踪和断点续传
"""

import os
import sys
import json
import hashlib
from datetime import datetime, date
from pathlib import Path
from typing import List, Dict, Optional, Tuple

# 添加项目路径
sys.path.append(str(Path(__file__).parent.parent.parent))
sys.path.append(str(Path(__file__).parent.parent))

from self.self_00_02_utils import log_info

class IncrementalManager:
    """增量处理管理器"""
    
    def __init__(self, status_file: str = "processing-status.json"):
        self.status_file = status_file
        self.status_data = {}
        self.load_status()
    
    def load_status(self):
        """加载处理状态"""
        try:
            if os.path.exists(self.status_file):
                with open(self.status_file, 'r', encoding='utf-8') as f:
                    self.status_data = json.load(f)
                log_info(f"加载状态文件成功，包含 {len(self.status_data)} 个日期记录")
            else:
                log_info("状态文件不存在，创建新的状态记录")
                self.status_data = {}
        except Exception as e:
            log_info(f"加载状态文件失败: {e}", level="ERROR")
            self.status_data = {}
    
    def save_status(self):
        """保存处理状态"""
        try:
            # 备份原文件
            if os.path.exists(self.status_file):
                backup_file = f"{self.status_file}.backup"
                os.rename(self.status_file, backup_file)
            
            with open(self.status_file, 'w', encoding='utf-8') as f:
                json.dump(self.status_data, f, ensure_ascii=False, indent=2, default=str)
            log_info("状态文件保存成功")
            
            # 删除备份文件
            backup_file = f"{self.status_file}.backup"
            if os.path.exists(backup_file):
                os.remove(backup_file)
                
        except Exception as e:
            log_info(f"保存状态文件失败: {e}", level="ERROR")
            
            # 恢复备份
            backup_file = f"{self.status_file}.backup"
            if os.path.exists(backup_file):
                os.rename(backup_file, self.status_file)
                log_info("已恢复状态文件备份")
    
    def get_file_info(self, file_path: str) -> Dict:
        """获取文件信息（大小、哈希等）"""
        try:
            stat = os.stat(file_path)
            file_hash = self._calculate_file_hash(file_path)
            
            return {
                'file_path': file_path,
                'file_name': os.path.basename(file_path),
                'file_size': stat.st_size,
                'file_hash': file_hash,
                'modified_time': datetime.fromtimestamp(stat.st_mtime),
                'created_time': datetime.fromtimestamp(stat.st_ctime)
            }
        except Exception as e:
            log_info(f"获取文件信息失败 {file_path}: {e}", level="ERROR")
            return None
    
    def _calculate_file_hash(self, file_path: str, chunk_size: int = 8192) -> str:
        """计算文件MD5哈希值"""
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(chunk_size), b""):
                    hash_md5.update(chunk)
        except Exception as e:
            log_info(f"计算文件哈希失败 {file_path}: {e}", level="ERROR")
            return None
        return hash_md5.hexdigest()
    
    def is_file_processed(self, file_path: str, log_date: date) -> bool:
        """检查文件是否已处理"""
        date_str = log_date.strftime('%Y-%m-%d')
        
        if date_str not in self.status_data:
            return False
        
        date_info = self.status_data[date_str]
        if 'files_processed' not in date_info:
            return False
        
        file_name = os.path.basename(file_path)
        current_hash = self._calculate_file_hash(file_path)
        
        if not current_hash:
            return False
        
        # 检查是否存在相同文件名和哈希的已完成记录
        for file_record in date_info['files_processed']:
            if (file_record.get('file') == file_name and 
                file_record.get('hash') == current_hash and
                file_record.get('status') == 'completed'):
                return True
        
        return False
    
    def mark_file_processing(self, file_path: str, log_date: date) -> str:
        """标记文件开始处理，返回进程ID"""
        date_str = log_date.strftime('%Y-%m-%d')
        
        if date_str not in self.status_data:
            self.status_data[date_str] = {
                'status': 'processing',
                'files_processed': []
            }
        
        file_info = self.get_file_info(file_path)
        if not file_info:
            return None
        
        process_id = f"{date_str}_{file_info['file_name']}_{datetime.now().strftime('%H%M%S')}"
        
        file_record = {
            'process_id': process_id,
            'file': file_info['file_name'],
            'full_path': file_info['file_path'],
            'hash': file_info['file_hash'],
            'file_size': file_info['file_size'],
            'status': 'processing',
            'start_time': datetime.now().isoformat(),
            'end_time': None,
            'records_count': 0,
            'error_message': None
        }
        
        # 查找现有记录或添加新记录
        existing_found = False
        for i, record in enumerate(self.status_data[date_str]['files_processed']):
            if record['file'] == file_info['file_name']:
                self.status_data[date_str]['files_processed'][i] = file_record
                existing_found = True
                break
        
        if not existing_found:
            self.status_data[date_str]['files_processed'].append(file_record)
        
        self.save_status()
        log_info(f"标记文件开始处理: {file_path}, 进程ID: {process_id}")
        return process_id
    
    def mark_file_completed(self, file_path: str, log_date: date, records_count: int, process_id: str = None):
        """标记文件处理完成"""
        date_str = log_date.strftime('%Y-%m-%d')
        file_name = os.path.basename(file_path)
        
        if date_str not in self.status_data:
            return
        
        # 找到对应的文件记录
        for record in self.status_data[date_str]['files_processed']:
            if (record['file'] == file_name and 
                (process_id is None or record.get('process_id') == process_id)):
                record['status'] = 'completed'
                record['end_time'] = datetime.now().isoformat()
                record['records_count'] = records_count
                break
        
        # 检查该日期下所有文件是否都已完成
        all_completed = all(f.get('status') == 'completed' 
                           for f in self.status_data[date_str]['files_processed'])
        
        if all_completed:
            self.status_data[date_str]['status'] = 'completed'
        
        self.save_status()
        log_info(f"标记文件处理完成: {file_path}, 记录数: {records_count}")
    
    def mark_file_failed(self, file_path: str, log_date: date, error_message: str, process_id: str = None):
        """标记文件处理失败"""
        date_str = log_date.strftime('%Y-%m-%d')
        file_name = os.path.basename(file_path)
        
        if date_str not in self.status_data:
            return
        
        # 找到对应的文件记录
        for record in self.status_data[date_str]['files_processed']:
            if (record['file'] == file_name and 
                (process_id is None or record.get('process_id') == process_id)):
                record['status'] = 'failed'
                record['end_time'] = datetime.now().isoformat()
                record['error_message'] = error_message
                break
        
        self.save_status()
        log_info(f"标记文件处理失败: {file_path}, 错误: {error_message}", level="ERROR")
    
    def get_unprocessed_files(self, log_files: List[Tuple[str, date]]) -> List[Tuple[str, date]]:
        """获取未处理的文件列表"""
        unprocessed = []
        
        for file_path, log_date in log_files:
            if not self.is_file_processed(file_path, log_date):
                unprocessed.append((file_path, log_date))
        
        log_info(f"发现 {len(unprocessed)} 个未处理文件，总文件数: {len(log_files)}")
        return unprocessed
    
    def get_date_status(self, log_date: date) -> Dict:
        """获取指定日期的处理状态"""
        date_str = log_date.strftime('%Y-%m-%d')
        
        if date_str not in self.status_data:
            return {
                'date': date_str,
                'status': 'not_started',
                'files_count': 0,
                'completed_count': 0,
                'failed_count': 0,
                'processing_count': 0,
                'total_records': 0
            }
        
        date_info = self.status_data[date_str]
        files = date_info.get('files_processed', [])
        
        completed_count = len([f for f in files if f.get('status') == 'completed'])
        failed_count = len([f for f in files if f.get('status') == 'failed'])
        processing_count = len([f for f in files if f.get('status') == 'processing'])
        total_records = sum(f.get('records_count', 0) for f in files if f.get('status') == 'completed')
        
        return {
            'date': date_str,
            'status': date_info.get('status', 'unknown'),
            'files_count': len(files),
            'completed_count': completed_count,
            'failed_count': failed_count,
            'processing_count': processing_count,
            'total_records': total_records,
            'files': files
        }
    
    def get_processing_summary(self) -> Dict:
        """获取处理汇总信息"""
        total_dates = len(self.status_data)
        completed_dates = len([d for d in self.status_data.values() if d.get('status') == 'completed'])
        
        all_files = []
        for date_info in self.status_data.values():
            all_files.extend(date_info.get('files_processed', []))
        
        total_files = len(all_files)
        completed_files = len([f for f in all_files if f.get('status') == 'completed'])
        failed_files = len([f for f in all_files if f.get('status') == 'failed'])
        total_records = sum(f.get('records_count', 0) for f in all_files if f.get('status') == 'completed')
        
        return {
            'total_dates': total_dates,
            'completed_dates': completed_dates,
            'completion_rate': (completed_dates / total_dates * 100) if total_dates > 0 else 0,
            'total_files': total_files,
            'completed_files': completed_files,
            'failed_files': failed_files,
            'file_success_rate': (completed_files / total_files * 100) if total_files > 0 else 0,
            'total_records': total_records,
            'last_updated': max([
                datetime.fromisoformat(f.get('end_time', '1900-01-01T00:00:00'))
                for date_info in self.status_data.values()
                for f in date_info.get('files_processed', [])
                if f.get('end_time')
            ] + [datetime(1900, 1, 1)]).isoformat()
        }
    
    def reset_failed_files(self, log_date: date = None):
        """重置失败文件状态，允许重新处理"""
        if log_date:
            date_str = log_date.strftime('%Y-%m-%d')
            if date_str in self.status_data:
                files = self.status_data[date_str].get('files_processed', [])
                reset_count = 0
                for file_record in files:
                    if file_record.get('status') == 'failed':
                        file_record['status'] = 'pending'
                        file_record['error_message'] = None
                        reset_count += 1
                
                if reset_count > 0:
                    self.status_data[date_str]['status'] = 'processing'
                    self.save_status()
                    log_info(f"重置 {date_str} 日期下 {reset_count} 个失败文件")
        else:
            # 重置所有失败文件
            total_reset = 0
            for date_str, date_info in self.status_data.items():
                files = date_info.get('files_processed', [])
                date_reset = 0
                for file_record in files:
                    if file_record.get('status') == 'failed':
                        file_record['status'] = 'pending'
                        file_record['error_message'] = None
                        date_reset += 1
                
                if date_reset > 0:
                    date_info['status'] = 'processing'
                    total_reset += date_reset
            
            if total_reset > 0:
                self.save_status()
                log_info(f"重置所有失败文件，总计 {total_reset} 个")


def main():
    """测试和命令行工具"""
    import argparse
    
    parser = argparse.ArgumentParser(description='增量处理管理器')
    parser.add_argument('--status', action='store_true', help='显示处理状态')
    parser.add_argument('--date', type=str, help='查看指定日期状态 (YYYY-MM-DD)')
    parser.add_argument('--reset-failed', action='store_true', help='重置失败文件')
    parser.add_argument('--status-file', type=str, default='processing-status.json', help='状态文件路径')
    
    args = parser.parse_args()
    
    manager = IncrementalManager(args.status_file)
    
    if args.status:
        if args.date:
            try:
                target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
                status = manager.get_date_status(target_date)
                print(json.dumps(status, ensure_ascii=False, indent=2, default=str))
            except ValueError:
                print(f"日期格式错误: {args.date}")
        else:
            summary = manager.get_processing_summary()
            print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    
    if args.reset_failed:
        target_date = None
        if args.date:
            try:
                target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
            except ValueError:
                print(f"日期格式错误: {args.date}")
                return
        
        manager.reset_failed_files(target_date)


if __name__ == "__main__":
    main()