import os
import zipfile
import gzip
import shutil
import tempfile
import logging
from datetime import datetime, timedelta
from pathlib import Path
import time
import argparse

class NginxLogProcessor:
    def __init__(self):
        # 配置路径
        self.download_dir = Path(r"D:\下载\火狐下载")
        self.backup_dir = Path(r"W:\底座Ng日志") 
        self.analysis_dir = Path(r"D:\project\nginx-log-analyzer\nginx-analytics-warehouse\nginx_logs")
        
        # 创建目标目录
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        self.analysis_dir.mkdir(parents=True, exist_ok=True)
        
        # 配置日志
        self.setup_logging()
        
    def setup_logging(self):
        """设置日志记录"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('nginx_zip_log_processor.log', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def find_latest_zip(self):
        """查找最新的日志zip文件"""
        try:
            zip_files = list(self.download_dir.glob("*.zip"))
            if not zip_files:
                self.logger.info("未找到任何zip文件")
                return None
            
            # 按修改时间排序，取最新的
            latest_zip = max(zip_files, key=os.path.getmtime)
            
            # 验证文件名格式（应该是日期时间格式）
            name = latest_zip.stem
            if len(name) == 14 and name.isdigit():
                self.logger.info(f"找到最新zip文件: {latest_zip.name}")
                return latest_zip
            else:
                self.logger.warning(f"文件名格式不符合预期: {latest_zip.name}")
                return None
                
        except Exception as e:
            self.logger.error(f"查找zip文件失败: {e}")
            return None
    
    def find_all_zips(self):
        """查找所有符合格式的日志zip文件"""
        try:
            zip_files = list(self.download_dir.glob("*.zip"))
            if not zip_files:
                self.logger.info("未找到任何zip文件")
                return []
            
            valid_zips = []
            for zip_file in zip_files:
                # 验证文件名格式（应该是日期时间格式）
                name = zip_file.stem
                if len(name) == 14 and name.isdigit():
                    valid_zips.append(zip_file)
                    self.logger.info(f"找到有效zip文件: {zip_file.name}")
                else:
                    self.logger.warning(f"文件名格式不符合预期，跳过: {zip_file.name}")
            
            # 按文件名排序（即按日期时间排序）
            valid_zips.sort(key=lambda x: x.stem)
            return valid_zips
                    
        except Exception as e:
            self.logger.error(f"查找zip文件失败: {e}")
            return []
    
    def extract_date_from_zip(self, zip_path):
        """从zip文件名提取日志日期（前一天）"""
        try:
            filename = zip_path.stem
            # 提取日期部分 (前8位)
            date_str = filename[:8]
            
            # 转换为日期对象
            zip_date = datetime.strptime(date_str, "%Y%m%d")
            
            # 日志日期是zip下载日期的前一天
            log_date = zip_date - timedelta(days=1)
            
            return log_date.strftime("%Y%m%d")
        except Exception as e:
            self.logger.error(f"提取日期失败: {e}")
            return None
    
    def process_single_log_gz(self, gz_path, output_dir, log_date):
        """处理单个.log.gz文件"""
        try:
            gz_name = gz_path.stem  # 去掉.gz后缀
            
            # 解压.gz文件
            with gzip.open(gz_path, 'rb') as f_in:
                content = f_in.read()
            
            # 从原始文件名提取前缀 (去掉.log.gz)
            if gz_name.endswith('.log'):
                prefix = gz_name[:-4]  # 去掉.log后缀
            else:
                prefix = gz_name
            
            # 新的文件名格式
            new_filename = f"{prefix}.log"
            output_path = output_dir / new_filename
            
            # 写入解压内容
            with open(output_path, 'wb') as f_out:
                f_out.write(content)
            
            self.logger.info(f"处理完成: {gz_path.name} -> {new_filename}")
            return output_path
            
        except Exception as e:
            self.logger.error(f"处理{gz_path.name}失败: {e}")
            return None
    
    def process_zip(self, zip_path):
        """处理zip文件"""
        try:
            # 提取日志日期
            log_date = self.extract_date_from_zip(zip_path)
            if not log_date:
                return False
            
            self.logger.info(f"开始处理日志日期: {log_date}")
            
            # 创建临时目录
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                
                # 解压zip文件到临时目录
                with zipfile.ZipFile(zip_path, 'r') as zip_file:
                    zip_file.extractall(temp_path)
                    self.logger.info(f"zip文件解压到临时目录: {temp_path}")
                
                # 创建处理后的文件目录
                processed_dir = temp_path / "processed"
                processed_dir.mkdir()
                
                # 查找所有.log.gz文件
                gz_files = list(temp_path.glob("*.log.gz"))
                if not gz_files:
                    self.logger.warning("未找到.log.gz文件")
                    return False
                
                self.logger.info(f"找到 {len(gz_files)} 个.log.gz文件")
                
                processed_files = []
                
                # 逐个处理.log.gz文件
                for gz_file in gz_files:
                    processed_file = self.process_single_log_gz(gz_file, processed_dir, log_date)
                    if processed_file:
                        processed_files.append(processed_file)
                    
                    # 删除原始.gz文件
                    try:
                        gz_file.unlink()
                        self.logger.info(f"删除原始文件: {gz_file.name}")
                    except Exception as e:
                        self.logger.warning(f"删除文件失败: {e}")
                
                if not processed_files:
                    self.logger.error("没有成功处理任何文件")
                    return False
                
                # 4.1 创建备份zip
                backup_zip_path = self.backup_dir / f"{log_date}.zip"
                with zipfile.ZipFile(backup_zip_path, 'w', zipfile.ZIP_DEFLATED) as backup_zip:
                    for file_path in processed_files:
                        backup_zip.write(file_path, file_path.name)
                
                self.logger.info(f"创建备份zip: {backup_zip_path}")
                
                # 4.2 复制到分析目录
                analysis_date_dir = self.analysis_dir / log_date
                analysis_date_dir.mkdir(exist_ok=True)
                
                for file_path in processed_files:
                    dest_path = analysis_date_dir / file_path.name
                    shutil.copy2(file_path, dest_path)
                
                self.logger.info(f"文件复制到分析目录: {analysis_date_dir}")
                
            # 处理完成后，可选择删除原始zip文件
            zip_path.unlink()  # 取消注释以删除原始zip
            self.logger.info(f"删除原始zip文件: {zip_path.name}")
            
            self.logger.info(f"日志 {log_date} 处理完成")
            return True
            
        except Exception as e:
            self.logger.error(f"处理zip文件失败: {e}")
            return False
    
    def run_once_old(self):
        """执行一次处理"""
        self.logger.info("开始执行日志处理...")
        
        # 查找最新zip文件
        zip_file = self.find_latest_zip()
        if not zip_file:
            self.logger.info("没有找到需要处理的zip文件")
            return False
        
        # 处理zip文件
        success = self.process_zip(zip_file)
        
        if success:
            self.logger.info("日志处理完成！")
        else:
            self.logger.error("日志处理失败！")
        
        return success
    
    def run_once(self):
        """执行一次处理"""
        self.logger.info("开始执行日志处理...")
        
        # 查找所有zip文件
        zip_files = self.find_all_zips()
        if not zip_files:
            self.logger.info("没有找到需要处理的zip文件")
            return False
        
        self.logger.info(f"找到 {len(zip_files)} 个zip文件需要处理")
        
        success_count = 0
        
        # 逐个处理zip文件
        for zip_file in zip_files:
            self.logger.info(f"正在处理: {zip_file.name}")
            if self.process_zip(zip_file):
                success_count += 1
            else:
                self.logger.error(f"处理失败: {zip_file.name}")
        
        self.logger.info(f"处理完成！成功: {success_count}/{len(zip_files)}")
        return success_count > 0
    
    def watch_mode(self, check_interval=300):
        """监控模式，定期检查新文件"""
        self.logger.info(f"进入监控模式，检查间隔: {check_interval}秒")
        
        processed_files = set()
        
        while True:
            try:
                zip_files = list(self.download_dir.glob("*.zip"))
                
                # 检查是否有新文件
                for zip_file in zip_files:
                    if zip_file not in processed_files:
                        self.logger.info(f"发现新文件: {zip_file.name}")
                        
                        if self.process_zip(zip_file):
                            processed_files.add(zip_file)
                        
                time.sleep(check_interval)
                
            except KeyboardInterrupt:
                self.logger.info("监控模式已停止")
                break
            except Exception as e:
                self.logger.error(f"监控模式错误: {e}")
                time.sleep(check_interval)


def main():
    parser = argparse.ArgumentParser(description='Nginx日志自动处理工具')
    parser.add_argument('--watch', action='store_true', help='启动监控模式')
    parser.add_argument('--interval', type=int, default=300, help='监控间隔（秒），默认300')
    
    args = parser.parse_args()
    
    processor = NginxLogProcessor()
    
    if args.watch:
        processor.watch_mode(args.interval)
    else:
        processor.run_once()


if __name__ == "__main__":
    main()