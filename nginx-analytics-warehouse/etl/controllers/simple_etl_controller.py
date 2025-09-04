#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简化ETL控制器 - Phase 1端到端测试
Simple ETL Controller - Phase 1 End-to-End Testing

协调日志解析器、字段映射器、数据写入器进行完整的ETL流程测试
"""

import sys
import os
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

# 添加路径以导入其他模块
current_dir = Path(__file__).parent
etl_root = current_dir.parent
sys.path.append(str(etl_root))

from parsers.base_log_parser import BaseLogParser
from processors.field_mapper import FieldMapper
from writers.dwd_writer import DWDWriter

class SimpleETLController:
    """简化ETL控制器"""
    
    def __init__(self, batch_size: int = 100):
        """
        初始化ETL控制器
        
        Args:
            batch_size: 批处理大小
        """
        self.batch_size = batch_size
        self.logger = logging.getLogger(__name__)
        
        # 初始化组件
        self.parser = BaseLogParser()
        self.mapper = FieldMapper()
        self.writer = DWDWriter()
        
        # 统计信息
        self.stats = {
            'start_time': None,
            'end_time': None,
            'total_files': 0,
            'total_lines': 0,
            'parsed_lines': 0,
            'mapped_lines': 0,
            'written_lines': 0,
            'failed_lines': 0,
            'batches_processed': 0,
            'processing_errors': []
        }
        
        self.logger.info("简化ETL控制器初始化完成")
    
    def process_file(self, file_path: str, test_mode: bool = False, limit: int = None) -> Dict[str, Any]:
        """
        处理单个日志文件
        
        Args:
            file_path: 日志文件路径
            test_mode: 测试模式（不实际写入数据库）
            limit: 限制处理的行数
            
        Returns:
            处理结果字典
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            error_msg = f"文件不存在: {file_path}"
            self.logger.error(error_msg)
            return self._create_error_result(error_msg)
        
        self.stats['start_time'] = datetime.now()
        self.stats['total_files'] += 1
        
        self.logger.info(f"开始处理文件: {file_path.name}")
        if test_mode:
            self.logger.info("🧪 测试模式：将不会实际写入数据库")
        if limit:
            self.logger.info(f"🔢 限制处理行数: {limit}")
        
        try:
            # 连接数据库（非测试模式）
            if not test_mode:
                if not self.writer.connect():
                    return self._create_error_result("数据库连接失败")
            
            # 批量处理
            batch = []
            processed_count = 0
            
            for parsed_data in self.parser.parse_file(file_path):
                self.stats['total_lines'] += 1
                
                if parsed_data:
                    self.stats['parsed_lines'] += 1
                    
                    try:
                        # 字段映射
                        mapped_data = self.mapper.map_to_dwd(parsed_data, file_path.name)
                        self.stats['mapped_lines'] += 1
                        
                        batch.append(mapped_data)
                        
                        # 批量写入
                        if len(batch) >= self.batch_size:
                            write_result = self._write_batch(batch, test_mode)
                            if write_result['success']:
                                self.stats['written_lines'] += write_result['count']
                            else:
                                self.stats['failed_lines'] += len(batch)
                                self.stats['processing_errors'].append(write_result['error'])
                            
                            self.stats['batches_processed'] += 1
                            batch = []
                        
                        processed_count += 1
                        
                        # 检查限制
                        if limit and processed_count >= limit:
                            self.logger.info(f"达到处理限制 ({limit} 行)，停止处理")
                            break
                            
                    except Exception as e:
                        self.logger.error(f"字段映射失败: {e}")
                        self.stats['failed_lines'] += 1
                        self.stats['processing_errors'].append(str(e))
                else:
                    self.stats['failed_lines'] += 1
            
            # 处理最后一批
            if batch:
                write_result = self._write_batch(batch, test_mode)
                if write_result['success']:
                    self.stats['written_lines'] += write_result['count']
                else:
                    self.stats['failed_lines'] += len(batch)
                    self.stats['processing_errors'].append(write_result['error'])
                
                self.stats['batches_processed'] += 1
            
            self.stats['end_time'] = datetime.now()
            
            # 关闭连接
            if not test_mode:
                self.writer.close()
            
            # 生成结果报告
            return self._create_success_result()
            
        except Exception as e:
            self.logger.error(f"处理文件失败: {e}")
            self.stats['end_time'] = datetime.now()
            if not test_mode:
                self.writer.close()
            return self._create_error_result(str(e))
    
    def test_with_sample_data(self, file_path: str, sample_size: int = 10) -> Dict[str, Any]:
        """
        使用样本数据测试ETL流程
        
        Args:
            file_path: 日志文件路径
            sample_size: 样本大小
            
        Returns:
            测试结果
        """
        self.logger.info(f"🧪 开始样本数据测试 (样本大小: {sample_size})")
        
        result = self.process_file(file_path, test_mode=True, limit=sample_size)
        
        if result['success']:
            self.logger.info("✅ 样本数据测试成功")
            self._print_sample_output(result)
        else:
            self.logger.error("❌ 样本数据测试失败")
        
        return result
    
    def run_full_process(self, file_path: str) -> Dict[str, Any]:
        """
        运行完整ETL流程
        
        Args:
            file_path: 日志文件路径
            
        Returns:
            处理结果
        """
        self.logger.info("🚀 开始完整ETL流程")
        
        result = self.process_file(file_path, test_mode=False)
        
        if result['success']:
            self.logger.info("✅ 完整ETL流程执行成功")
            self._print_final_report(result)
        else:
            self.logger.error("❌ 完整ETL流程执行失败")
        
        return result
    
    def get_stats(self) -> Dict[str, Any]:
        """获取处理统计信息"""
        stats = self.stats.copy()
        
        if stats['start_time'] and stats['end_time']:
            duration = stats['end_time'] - stats['start_time']
            stats['duration_seconds'] = duration.total_seconds()
            
            if stats['duration_seconds'] > 0:
                stats['lines_per_second'] = stats['total_lines'] / stats['duration_seconds']
            else:
                stats['lines_per_second'] = 0
        
        if stats['total_lines'] > 0:
            stats['success_rate'] = (stats['written_lines'] / stats['total_lines']) * 100
        else:
            stats['success_rate'] = 0
        
        return stats
    
    # === 私有方法 ===
    
    def _write_batch(self, batch: List[Dict[str, Any]], test_mode: bool) -> Dict[str, Any]:
        """写入批量数据"""
        if test_mode:
            # 测试模式：只返回成功结果
            return {
                'success': True,
                'count': len(batch),
                'message': f'测试模式：模拟写入 {len(batch)} 条记录'
            }
        else:
            # 实际写入数据库
            return self.writer.write_batch(batch)
    
    def _create_success_result(self) -> Dict[str, Any]:
        """创建成功结果"""
        stats = self.get_stats()
        
        return {
            'success': True,
            'message': '文件处理成功',
            'stats': stats,
            'timestamp': datetime.now()
        }
    
    def _create_error_result(self, error: str) -> Dict[str, Any]:
        """创建错误结果"""
        stats = self.get_stats()
        
        return {
            'success': False,
            'error': error,
            'stats': stats,
            'timestamp': datetime.now()
        }
    
    def _print_sample_output(self, result: Dict[str, Any]):
        """打印样本输出"""
        stats = result['stats']
        
        print("\n" + "="*60)
        print("🧪 样本数据测试结果")
        print("="*60)
        print(f"📄 处理文件: {stats['total_files']} 个")
        print(f"📊 总行数: {stats['total_lines']}")
        print(f"✅ 解析成功: {stats['parsed_lines']}")
        print(f"🔄 映射成功: {stats['mapped_lines']}")
        print(f"💾 模拟写入: {stats['written_lines']}")
        print(f"❌ 失败行数: {stats['failed_lines']}")
        
        if stats.get('duration_seconds'):
            print(f"⏱️  处理时间: {stats['duration_seconds']:.2f} 秒")
            print(f"🚀 处理速度: {stats.get('lines_per_second', 0):.1f} 行/秒")
        
        print(f"📈 成功率: {stats.get('success_rate', 0):.1f}%")
        print("="*60)
    
    def _print_final_report(self, result: Dict[str, Any]):
        """打印最终报告"""
        stats = result['stats']
        
        print("\n" + "="*60)
        print("🚀 完整ETL流程执行报告")
        print("="*60)
        print(f"📄 处理文件: {stats['total_files']} 个")
        print(f"📊 总行数: {stats['total_lines']}")
        print(f"✅ 解析成功: {stats['parsed_lines']}")
        print(f"🔄 映射成功: {stats['mapped_lines']}")
        print(f"💾 写入成功: {stats['written_lines']}")
        print(f"❌ 失败行数: {stats['failed_lines']}")
        print(f"📦 批次数: {stats['batches_processed']}")
        
        if stats.get('duration_seconds'):
            print(f"⏱️  处理时间: {stats['duration_seconds']:.2f} 秒")
            print(f"🚀 处理速度: {stats.get('lines_per_second', 0):.1f} 行/秒")
        
        print(f"📈 成功率: {stats.get('success_rate', 0):.1f}%")
        
        if stats.get('processing_errors'):
            print(f"⚠️  错误信息 ({len(stats['processing_errors'])} 个):")
            for i, error in enumerate(stats['processing_errors'][:5], 1):
                print(f"   {i}. {error}")
            if len(stats['processing_errors']) > 5:
                print(f"   ... 还有 {len(stats['processing_errors']) - 5} 个错误")
        
        print("="*60)


def main():
    """主函数"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 测试文件路径
    #test_file = Path("D:/project/nginx-log-analyzer/nginx-analytics-warehouse/nginx_logs/20250422/access186.log")
    test_file = Path("D:/project/nginx-log-analyzer/nginx-analytics-warehouse/nginx_logs/20250829/nginx_part_4.log")

    if not test_file.exists():
        print(f"❌ 测试文件不存在: {test_file}")
        return
    
    # 创建ETL控制器
    controller = SimpleETLController(batch_size=50)
    
    print("🎯 Phase 1 ETL 端到端测试")
    print("="*60)
    
    # 第一步：样本数据测试
    print("\n📋 第一步: 样本数据测试...")
    sample_result = controller.test_with_sample_data(test_file, sample_size=5)
    
    if not sample_result['success']:
        print("❌ 样本测试失败，停止执行")
        return
    
    # 第二步：完整流程测试
    print("\n📋 第二步: 完整数据处理...")
    user_input = input("继续执行完整ETL流程吗？(y/N): ").strip().lower()
    
    if user_input == 'y':
        full_result = controller.run_full_process(test_file)
        
        if full_result['success']:
            print("\n🎉 Phase 1 ETL测试完全成功！")
        else:
            print("\n💥 Phase 1 ETL测试失败")
    else:
        print("\n⏹️  用户取消完整流程执行")


if __name__ == "__main__":
    main()