#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Nginx日志批量下载工具
支持多实例、智能过滤、并发下载、断点续传等功能
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import requests
from bs4 import BeautifulSoup
import os
import json
import sqlite3
import threading
import time
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
from datetime import datetime
import re

class DownloadHistoryDB:
    """下载历史数据库管理"""
    
    def __init__(self, db_path="download_history.db"):
        self.db_path = db_path
        self.init_db()
    
    def init_db(self):
        """初始化数据库"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS download_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_url TEXT UNIQUE,
                file_name TEXT,
                file_size INTEGER,
                download_time TEXT,
                local_path TEXT,
                status TEXT
            )
        ''')
        conn.commit()
        conn.close()
    
    def is_downloaded(self, file_url):
        """检查文件是否已下载"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT id FROM download_history WHERE file_url = ? AND status = "completed"', (file_url,))
        result = cursor.fetchone()
        conn.close()
        return result is not None
    
    def add_download_record(self, file_url, file_name, file_size, local_path, status="completed"):
        """添加下载记录"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        download_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute('''
            INSERT OR REPLACE INTO download_history 
            (file_url, file_name, file_size, download_time, local_path, status)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (file_url, file_name, file_size, download_time, local_path, status))
        conn.commit()
        conn.close()

class FileFilter:
    """文件过滤器"""
    
    def __init__(self, include_keywords=None, exclude_keywords=None, include_priority=True):
        self.include_keywords = include_keywords or []
        self.exclude_keywords = exclude_keywords or []
        self.include_priority = include_priority  # True表示包含优先，False表示排除优先
    
    def should_download(self, filename):
        """判断文件是否应该下载"""
        has_include = any(keyword.lower() in filename.lower() for keyword in self.include_keywords)
        has_exclude = any(keyword.lower() in filename.lower() for keyword in self.exclude_keywords)
        
        # 如果没有设置过滤条件，默认下载
        if not self.include_keywords and not self.exclude_keywords:
            return True
        
        # 只有包含条件
        if self.include_keywords and not self.exclude_keywords:
            return has_include
        
        # 只有排除条件
        if not self.include_keywords and self.exclude_keywords:
            return not has_exclude
        
        # 同时有包含和排除条件，根据优先级判断
        if has_include and has_exclude:
            return self.include_priority
        elif has_include:
            return True
        elif has_exclude:
            return False
        else:
            return not self.include_keywords  # 如果有包含条件但都不匹配，则不下载

class NginxLogDownloader:
    """Nginx日志下载器"""
    
    def __init__(self, base_url="http://10.14.34.175:19000", nginx_instances=8):
        self.base_url = base_url
        self.nginx_instances = nginx_instances
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.db = DownloadHistoryDB()
        self.download_threads = {}
        self.pause_flags = {}
        
    def get_file_list(self, nginx_id):
        """获取指定nginx实例的文件列表"""
        url = f"{self.base_url}/logs/nginx{nginx_id}/"
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            files = []
            
            for link in soup.find_all('a'):
                href = link.get('href')
                if href and (href.endswith('.log') or href.endswith('.tar.gz')):
                    # 获取文件信息
                    file_info = link.parent.text.strip()
                    parts = file_info.split()
                    
                    file_name = href
                    file_url = urljoin(url, href)
                    file_size = 0
                    
                    # 尝试解析文件大小
                    for part in parts:
                        if part.isdigit():
                            file_size = int(part)
                            break
                        elif 'K' in part or 'M' in part or 'G' in part:
                            try:
                                size_str = part.replace('K', '').replace('M', '').replace('G', '')
                                if 'K' in part:
                                    file_size = int(float(size_str) * 1024)
                                elif 'M' in part:
                                    file_size = int(float(size_str) * 1024 * 1024)
                                elif 'G' in part:
                                    file_size = int(float(size_str) * 1024 * 1024 * 1024)
                            except:
                                pass
                    
                    files.append({
                        'name': file_name,
                        'url': file_url,
                        'size': file_size,
                        'nginx_id': nginx_id
                    })
            
            return files
        except Exception as e:
            print(f"获取nginx{nginx_id}文件列表失败: {e}")
            return []
    
    def download_file(self, file_info, download_dir, progress_callback=None):
        """下载单个文件，支持断点续传"""
        file_url = file_info['url']
        original_name = file_info['name']
        nginx_id = file_info['nginx_id']
        
        # 生成本地文件名
        # 生成本地文件名
        local_filename = f"nginx{nginx_id}_{original_name}"
        
        local_path = os.path.join(download_dir, local_filename)
        
        # 检查是否已下载
        if self.db.is_downloaded(file_url) and os.path.exists(local_path):
            if progress_callback:
                progress_callback(file_info['size'], file_info['size'], f"已存在: {local_filename}")
            return True
        
        try:
            # 获取已下载的文件大小
            resume_pos = 0
            if os.path.exists(local_path):
                resume_pos = os.path.getsize(local_path)
            
            # 设置断点续传头
            headers = {}
            if resume_pos > 0:
                headers['Range'] = f'bytes={resume_pos}-'
            
            response = self.session.get(file_url, headers=headers, stream=True, timeout=30)
            
            # 检查是否支持断点续传
            if resume_pos > 0 and response.status_code not in [206, 200]:
                resume_pos = 0  # 不支持断点续传，重新下载
            
            # 获取文件总大小
            total_size = resume_pos
            if 'content-length' in response.headers:
                total_size += int(response.headers['content-length'])
            elif 'content-range' in response.headers:
                total_size = int(response.headers['content-range'].split('/')[-1])
            
            # 开始下载
            mode = 'ab' if resume_pos > 0 else 'wb'
            downloaded = resume_pos
            
            with open(local_path, mode) as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        # 检查暂停标志
                        thread_id = threading.current_thread().ident
                        if thread_id in self.pause_flags and self.pause_flags[thread_id]:
                            break
                        
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        if progress_callback:
                            progress_callback(downloaded, total_size, f"下载中: {local_filename}")
            
            # 检查下载是否完成
            if downloaded >= total_size:
                self.db.add_download_record(file_url, original_name, total_size, local_path)
                if progress_callback:
                    progress_callback(downloaded, total_size, f"完成: {local_filename}")
                return True
            else:
                if progress_callback:
                    progress_callback(downloaded, total_size, f"暂停: {local_filename}")
                return False
                
        except Exception as e:
            if progress_callback:
                progress_callback(0, file_info['size'], f"错误: {local_filename} - {str(e)}")
            return False

class DownloaderGUI:
    """图形界面"""
    
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Nginx日志批量下载工具")
        self.root.geometry("900x700")
        
        self.downloader = NginxLogDownloader()
        self.file_filter = FileFilter()
        self.download_dir = ""
        self.file_list = []
        self.filtered_files = []
        self.is_downloading = False
        self.executor = None
        
        self.setup_ui()
        self.load_config()
    
    def setup_ui(self):
        """设置界面"""
        # 主框架
        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # 配置区域
        config_frame = ttk.LabelFrame(main_frame, text="配置设置", padding=10)
        config_frame.pack(fill=tk.X, pady=(0, 10))
        
        # 服务器配置
        server_frame = ttk.Frame(config_frame)
        server_frame.pack(fill=tk.X, pady=(0, 5))
        ttk.Label(server_frame, text="服务器地址:").pack(side=tk.LEFT)
        self.server_var = tk.StringVar(value="http://10.14.34.175:19000")
        ttk.Entry(server_frame, textvariable=self.server_var, width=40).pack(side=tk.LEFT, padx=(5, 0))
        
        # 下载目录
        dir_frame = ttk.Frame(config_frame)
        dir_frame.pack(fill=tk.X, pady=(5, 5))
        ttk.Label(dir_frame, text="下载目录:").pack(side=tk.LEFT)
        self.dir_var = tk.StringVar()
        ttk.Entry(dir_frame, textvariable=self.dir_var, width=50).pack(side=tk.LEFT, padx=(5, 5))
        ttk.Button(dir_frame, text="选择", command=self.select_directory).pack(side=tk.LEFT)
        
        # 过滤规则
        filter_frame = ttk.LabelFrame(config_frame, text="过滤规则", padding=5)
        filter_frame.pack(fill=tk.X, pady=(5, 0))
        
        # 包含关键词
        include_frame = ttk.Frame(filter_frame)
        include_frame.pack(fill=tk.X, pady=(0, 5))
        ttk.Label(include_frame, text="包含关键词:").pack(side=tk.LEFT)
        self.include_var = tk.StringVar()
        ttk.Entry(include_frame, textvariable=self.include_var, width=40).pack(side=tk.LEFT, padx=(5, 0))
        ttk.Label(include_frame, text="(用逗号分隔)").pack(side=tk.LEFT, padx=(5, 0))
        
        # 排除关键词
        exclude_frame = ttk.Frame(filter_frame)
        exclude_frame.pack(fill=tk.X, pady=(0, 5))
        ttk.Label(exclude_frame, text="排除关键词:").pack(side=tk.LEFT)
        self.exclude_var = tk.StringVar()
        ttk.Entry(exclude_frame, textvariable=self.exclude_var, width=40).pack(side=tk.LEFT, padx=(5, 0))
        ttk.Label(exclude_frame, text="(用逗号分隔)").pack(side=tk.LEFT, padx=(5, 0))
        
        # 优先级
        priority_frame = ttk.Frame(filter_frame)
        priority_frame.pack(fill=tk.X)
        ttk.Label(priority_frame, text="优先级:").pack(side=tk.LEFT)
        self.priority_var = tk.StringVar(value="包含优先")
        priority_combo = ttk.Combobox(priority_frame, textvariable=self.priority_var, 
                                    values=["包含优先", "排除优先"], state="readonly", width=15)
        priority_combo.pack(side=tk.LEFT, padx=(5, 0))
        
        # 并发设置
        concurrent_frame = ttk.Frame(config_frame)
        concurrent_frame.pack(fill=tk.X, pady=(5, 0))
        ttk.Label(concurrent_frame, text="并发下载数:").pack(side=tk.LEFT)
        self.concurrent_var = tk.StringVar(value="3")
        ttk.Spinbox(concurrent_frame, from_=1, to=10, textvariable=self.concurrent_var, width=10).pack(side=tk.LEFT, padx=(5, 0))
        
        # 操作按钮
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Button(button_frame, text="扫描文件", command=self.scan_files).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(button_frame, text="开始下载", command=self.start_download).pack(side=tk.LEFT, padx=(0, 5))
        self.pause_btn = ttk.Button(button_frame, text="暂停", command=self.pause_download, state=tk.DISABLED)
        self.pause_btn.pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(button_frame, text="清空历史", command=self.clear_history).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(button_frame, text="保存配置", command=self.save_config).pack(side=tk.RIGHT)
        
        # 文件列表
        list_frame = ttk.LabelFrame(main_frame, text="文件列表", padding=5)
        list_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
        
        # 创建Treeview
        columns = ("文件名", "大小", "实例", "状态")
        self.file_tree = ttk.Treeview(list_frame, columns=columns, show="headings", height=15)
        
        for col in columns:
            self.file_tree.heading(col, text=col)
            self.file_tree.column(col, width=150)
        
        # 滚动条
        scrollbar = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=self.file_tree.yview)
        self.file_tree.configure(yscrollcommand=scrollbar.set)
        
        self.file_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # 状态栏
        status_frame = ttk.Frame(main_frame)
        status_frame.pack(fill=tk.X)
        
        self.status_var = tk.StringVar(value="就绪")
        ttk.Label(status_frame, textvariable=self.status_var).pack(side=tk.LEFT)
        
        # 进度条
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(status_frame, variable=self.progress_var, length=200)
        self.progress_bar.pack(side=tk.RIGHT, padx=(10, 0))
    
    def select_directory(self):
        """选择下载目录"""
        directory = filedialog.askdirectory()
        if directory:
            self.dir_var.set(directory)
            self.download_dir = directory
    
    def scan_files(self):
        """扫描所有nginx实例的文件"""
        if not self.download_dir:
            messagebox.showerror("错误", "请先选择下载目录")
            return
        
        self.status_var.set("扫描文件中...")
        self.file_tree.delete(*self.file_tree.get_children())
        
        # 更新下载器配置
        self.downloader.base_url = self.server_var.get()
        
        # 创建过滤器
        include_keywords = [k.strip() for k in self.include_var.get().split(',') if k.strip()]
        exclude_keywords = [k.strip() for k in self.exclude_var.get().split(',') if k.strip()]
        include_priority = self.priority_var.get() == "包含优先"
        
        self.file_filter = FileFilter(include_keywords, exclude_keywords, include_priority)
        
        def scan_worker():
            all_files = []
            for i in range(1, self.downloader.nginx_instances + 1):
                files = self.downloader.get_file_list(i)
                all_files.extend(files)
            
            # 应用过滤器
            filtered_files = [f for f in all_files if self.file_filter.should_download(f['name'])]
            
            self.root.after(0, self.update_file_list, filtered_files)
        
        threading.Thread(target=scan_worker, daemon=True).start()
    
    def update_file_list(self, files):
        """更新文件列表显示"""
        self.filtered_files = files
        
        for file_info in files:
            size_str = self.format_size(file_info['size'])
            status = "已下载" if self.downloader.db.is_downloaded(file_info['url']) else "待下载"
            
            self.file_tree.insert("", tk.END, values=(
                file_info['name'],
                size_str,
                f"nginx{file_info['nginx_id']}",
                status
            ))
        
        self.status_var.set(f"扫描完成，找到 {len(files)} 个文件")
    
    def start_download(self):
        """开始下载"""
        if not self.filtered_files:
            messagebox.showerror("错误", "请先扫描文件")
            return
        
        if not self.download_dir:
            messagebox.showerror("错误", "请选择下载目录")
            return
        
        self.is_downloading = True
        self.pause_btn.config(state=tk.NORMAL)
        
        # 重置暂停标志
        self.downloader.pause_flags.clear()
        
        max_workers = int(self.concurrent_var.get())
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        
        def download_worker():
            completed = 0
            total = len(self.filtered_files)
            
            def progress_callback(downloaded, total_size, message):
                self.root.after(0, self.update_progress, downloaded, total_size, message)
            
            futures = []
            for file_info in self.filtered_files:
                if not self.is_downloading:
                    break
                future = self.executor.submit(
                    self.downloader.download_file, 
                    file_info, 
                    self.download_dir, 
                    progress_callback
                )
                futures.append(future)
            
            for future in as_completed(futures):
                if not self.is_downloading:
                    break
                try:
                    result = future.result()
                    completed += 1
                    progress = (completed / total) * 100
                    self.root.after(0, lambda p=progress: self.progress_var.set(p))
                except Exception as e:
                    print(f"下载错误: {e}")
            
            self.root.after(0, self.download_finished)
        
        threading.Thread(target=download_worker, daemon=True).start()
        self.status_var.set("正在下载...")
    
    def pause_download(self):
        """暂停/恢复下载"""
        if self.is_downloading:
            self.is_downloading = False
            # 设置所有线程的暂停标志
            for thread_id in self.downloader.download_threads:
                self.downloader.pause_flags[thread_id] = True
            
            if self.executor:
                self.executor.shutdown(wait=False)
            
            self.pause_btn.config(text="恢复")
            self.status_var.set("已暂停")
        else:
            self.start_download()
            self.pause_btn.config(text="暂停")
    
    def download_finished(self):
        """下载完成"""
        self.is_downloading = False
        self.pause_btn.config(state=tk.DISABLED, text="暂停")
        self.progress_var.set(100)
        self.status_var.set("下载完成")
        
        # 刷新文件列表状态
        self.scan_files()
    
    def update_progress(self, downloaded, total_size, message):
        """更新进度"""
        self.status_var.set(message)
    
    def clear_history(self):
        """清空下载历史"""
        if messagebox.askyesno("确认", "确定要清空下载历史吗？"):
            os.remove("download_history.db")
            self.downloader.db = DownloadHistoryDB()
            messagebox.showinfo("提示", "下载历史已清空")
            if self.filtered_files:
                self.update_file_list(self.filtered_files)
    
    def format_size(self, size):
        """格式化文件大小"""
        if size < 1024:
            return f"{size} B"
        elif size < 1024 * 1024:
            return f"{size / 1024:.1f} KB"
        elif size < 1024 * 1024 * 1024:
            return f"{size / (1024 * 1024):.1f} MB"
        else:
            return f"{size / (1024 * 1024 * 1024):.1f} GB"
    
    def save_config(self):
        """保存配置"""
        config = {
            'server_url': self.server_var.get(),
            'download_dir': self.dir_var.get(),
            'include_keywords': self.include_var.get(),
            'exclude_keywords': self.exclude_var.get(),
            'priority': self.priority_var.get(),
            'concurrent': self.concurrent_var.get()
        }
        
        with open('config.json', 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
        
        messagebox.showinfo("提示", "配置已保存")
    
    def load_config(self):
        """加载配置"""
        try:
            with open('config.json', 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            self.server_var.set(config.get('server_url', 'http://10.14.34.175:19000'))
            self.dir_var.set(config.get('download_dir', ''))
            self.download_dir = config.get('download_dir', '')
            self.include_var.set(config.get('include_keywords', ''))
            self.exclude_var.set(config.get('exclude_keywords', ''))
            self.priority_var.set(config.get('priority', '包含优先'))
            self.concurrent_var.set(config.get('concurrent', '3'))
        except:
            pass  # 配置文件不存在或格式错误时忽略

    
    def run(self):
        """运行应用"""
        self.root.mainloop()

if __name__ == "__main__":
    app = DownloaderGUI()
    app.run()
