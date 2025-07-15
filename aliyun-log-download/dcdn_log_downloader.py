#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
阿里云DCDN日志批量下载工具
功能：批量下载指定时间范围内的DCDN日志，按日期分类存储
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import os
import json
import threading
import time
import requests
from datetime import datetime, timedelta
from urllib.parse import urlparse
import gzip
import hashlib
import queue
import re
import sqlite3

try:
    from aliyunsdkcore.client import AcsClient
    from aliyunsdkdcdn.request.v20180115 import DescribeDcdnDomainLogRequest

    SDK_AVAILABLE = True
except ImportError:
    SDK_AVAILABLE = False


class LogContinuityChecker:
    """日志连续性检查器 - 参考cdn_log_analyzer.py"""
    
    def __init__(self):
        # DCDN日志文件名格式: domain_YYYY_MM_DD_HHMMSS_HHMMSS.gz
        self.time_pattern = re.compile(r'(.+)_(\d{4})_(\d{2})_(\d{2})_(\d{6})_(\d{6})\.gz')
    
    def extract_time_from_filename(self, filename):
        """从DCDN日志文件名提取时间范围"""
        match = self.time_pattern.search(filename)
        if not match:
            return None
            
        try:
            domain, year, month, day, start_time, end_time = match.groups()
            
            # 解析开始时间
            start_hour = int(start_time[:2])
            start_minute = int(start_time[2:4])
            start_second = int(start_time[4:6])
            
            start_dt = datetime(
                int(year), int(month), int(day),
                start_hour, start_minute, start_second
            )
            
            # 解析结束时间
            end_hour = int(end_time[:2])
            end_minute = int(end_time[2:4])
            end_second = int(end_time[4:6])
            
            end_dt = datetime(
                int(year), int(month), int(day),
                end_hour, end_minute, end_second
            )
            
            return start_dt, end_dt
            
        except ValueError:
            return None
    
    def check_continuity(self, log_infos):
        """检查日志连续性"""
        time_ranges = []
        invalid_files = []
        
        for log_info in log_infos:
            filename = log_info['LogName']
            time_range = self.extract_time_from_filename(filename)
            
            if time_range:
                time_ranges.append((log_info, time_range[0], time_range[1]))
            else:
                invalid_files.append(log_info)
        
        # 按开始时间排序
        time_ranges.sort(key=lambda x: x[1])
        
        # 检查连续性
        gaps = []
        if len(time_ranges) > 1:
            for i in range(len(time_ranges) - 1):
                current_end = time_ranges[i][2]
                next_start = time_ranges[i + 1][1]
                
                if current_end < next_start:
                    gap_duration = next_start - current_end
                    gaps.append((current_end, next_start, gap_duration))
        
        return {
            'total_files': len(log_infos),
            'valid_files': len(time_ranges),
            'invalid_files': invalid_files,
            'time_ranges': time_ranges,
            'gaps': gaps,
            'continuous': len(gaps) == 0
        }


class DownloadHistoryDB:
    """下载历史数据库管理"""
    
    def __init__(self, db_path="dcdn_download_history.db"):
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


class DCDNLogDownloader:
    def __init__(self, root):
        self.root = root
        self.root.title("阿里云DCDN日志批量下载工具 - 增强版")
        self.root.geometry("1000x900")

        # 下载状态
        self.is_downloading = False
        self.download_queue = queue.Queue()
        self.download_history = []
        self.current_logs = []  # 存储当前获取的日志列表

        # 统计信息
        self.total_files = 0
        self.completed_files = 0
        self.failed_files = 0

        # 新增功能组件
        self.continuity_checker = LogContinuityChecker()
        self.history_db = DownloadHistoryDB()
        
        # 快速时间选择变量
        self.quick_select_var = tk.StringVar(value="昨天")

        self.setup_ui()
        
        # 初始化时间信息显示
        self.update_time_info()

        # 检查SDK
        if not SDK_AVAILABLE:
            messagebox.showwarning("警告",
                                   "阿里云SDK未安装，部分功能可能受限。\n请安装：pip install aliyun-log-analyzer-python-sdk-dcdn")
        else:
            self.log_message("🚀 DCDN日志下载工具已启动（增强版）")
            self.log_message("🆕 新增功能: 快速时间选择、连续性检查、下载历史")

    def setup_ui(self):
        # 主框架
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # 配置输入区域
        config_frame = ttk.LabelFrame(main_frame, text="配置信息", padding="10")
        config_frame.grid(row=0, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))

        # AccessKey
        ttk.Label(config_frame, text="AccessKey ID:").grid(row=0, column=0, sticky=tk.W, pady=2)
        self.access_key_var = tk.StringVar()
        ttk.Entry(config_frame, textvariable=self.access_key_var, width=50, show="*").grid(row=0, column=1,
                                                                                           sticky=(tk.W, tk.E), pady=2)

        # SecretKey
        ttk.Label(config_frame, text="AccessKey Secret:").grid(row=1, column=0, sticky=tk.W, pady=2)
        self.secret_key_var = tk.StringVar()
        ttk.Entry(config_frame, textvariable=self.secret_key_var, width=50, show="*").grid(row=1, column=1,
                                                                                           sticky=(tk.W, tk.E), pady=2)

        # 区域
        ttk.Label(config_frame, text="区域:").grid(row=2, column=0, sticky=tk.W, pady=2)
        self.region_var = tk.StringVar(value="cn-hangzhou")
        region_combo = ttk.Combobox(config_frame, textvariable=self.region_var, width=47,
                                    values=["cn-hangzhou", "cn-beijing", "cn-shanghai", "cn-shenzhen"])
        region_combo.grid(row=2, column=1, sticky=(tk.W, tk.E), pady=2)

        # 快速时间选择区域
        quick_time_frame = ttk.LabelFrame(main_frame, text="快速时间选择", padding="10")
        quick_time_frame.grid(row=1, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        
        ttk.Label(quick_time_frame, text="快捷选择:").grid(row=0, column=0, sticky=tk.W, pady=2)
        quick_combo = ttk.Combobox(quick_time_frame, textvariable=self.quick_select_var, width=15,
                                   values=["昨天", "最近7天", "最近30天", "本月", "上月", "自定义"],
                                   state="readonly")
        quick_combo.grid(row=0, column=1, sticky=tk.W, pady=2, padx=(10, 0))
        
        ttk.Button(quick_time_frame, text="应用", command=self.apply_quick_select).grid(row=0, column=2, padx=(10, 0))
        
        # 时间范围信息显示
        self.time_info_var = tk.StringVar(value="")
        ttk.Label(quick_time_frame, textvariable=self.time_info_var, foreground="blue").grid(row=0, column=3, padx=(20, 0))

        # 下载参数区域
        param_frame = ttk.LabelFrame(main_frame, text="下载参数", padding="10")
        param_frame.grid(row=2, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))

        # 域名
        ttk.Label(param_frame, text="域名:").grid(row=0, column=0, sticky=tk.W, pady=2)
        self.domain_var = tk.StringVar(value="example.com")
        ttk.Entry(param_frame, textvariable=self.domain_var, width=50).grid(row=0, column=1, sticky=(tk.W, tk.E),
                                                                            pady=2)

        # 开始日期
        ttk.Label(param_frame, text="开始日期:").grid(row=1, column=0, sticky=tk.W, pady=2)
        self.start_time_var = tk.StringVar(value=(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"))
        ttk.Entry(param_frame, textvariable=self.start_time_var, width=50).grid(row=1, column=1, sticky=(tk.W, tk.E),
                                                                                pady=2)

        # 结束日期
        ttk.Label(param_frame, text="结束日期:").grid(row=2, column=0, sticky=tk.W, pady=2)
        self.end_time_var = tk.StringVar(value=datetime.now().strftime("%Y-%m-%d"))
        ttk.Entry(param_frame, textvariable=self.end_time_var, width=50).grid(row=2, column=1, sticky=(tk.W, tk.E),
                                                                              pady=2)

        # 下载路径
        ttk.Label(param_frame, text="下载路径:").grid(row=3, column=0, sticky=tk.W, pady=2)
        path_frame = ttk.Frame(param_frame)
        path_frame.grid(row=3, column=1, sticky=(tk.W, tk.E), pady=2)

        self.download_path_var = tk.StringVar(value="../data/dcdn/logs")
        ttk.Entry(path_frame, textvariable=self.download_path_var, width=40).grid(row=0, column=0, sticky=(tk.W, tk.E))
        ttk.Button(path_frame, text="浏览", command=self.browse_path).grid(row=0, column=1, padx=(5, 0))

        # 高级选项
        advanced_frame = ttk.LabelFrame(main_frame, text="高级选项", padding="10")
        advanced_frame.grid(row=3, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))

        # 并发数
        ttk.Label(advanced_frame, text="并发下载数:").grid(row=0, column=0, sticky=tk.W, pady=2)
        self.concurrent_var = tk.IntVar(value=3)
        ttk.Spinbox(advanced_frame, from_=1, to=10, textvariable=self.concurrent_var, width=10).grid(row=0, column=1,
                                                                                                     sticky=tk.W,
                                                                                                     pady=2)

        # 重试次数
        ttk.Label(advanced_frame, text="重试次数:").grid(row=0, column=2, sticky=tk.W, pady=2, padx=(20, 0))
        self.retry_var = tk.IntVar(value=3)
        ttk.Spinbox(advanced_frame, from_=1, to=10, textvariable=self.retry_var, width=10).grid(row=0, column=3,
                                                                                                sticky=tk.W, pady=2)

        # 校验文件完整性
        self.verify_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(advanced_frame, text="校验文件完整性", variable=self.verify_var).grid(row=1, column=0,
                                                                                              sticky=tk.W, pady=2)

        # 跳过已存在文件
        self.skip_existing_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(advanced_frame, text="跳过已存在文件", variable=self.skip_existing_var).grid(row=1, column=1,
                                                                                                     sticky=tk.W,
                                                                                                     pady=2)
        
        # 启用下载历史记录
        self.use_history_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(advanced_frame, text="启用下载历史", variable=self.use_history_var).grid(row=1, column=2,
                                                                                                    sticky=tk.W,
                                                                                                    pady=2, padx=(20, 0))
        
        # 时区设置
        ttk.Label(advanced_frame, text="本地时区:").grid(row=2, column=0, sticky=tk.W, pady=2)
        self.timezone_var = tk.IntVar(value=8)  # 默认东八区
        timezone_combo = ttk.Combobox(advanced_frame, textvariable=self.timezone_var, width=10,
                                     values=list(range(-12, 13)), state="readonly")
        timezone_combo.grid(row=2, column=1, sticky=tk.W, pady=2)
        ttk.Label(advanced_frame, text="(UTC+8=东八区)", foreground="gray").grid(row=2, column=2, sticky=tk.W, pady=2)
        
        # 时区测试按钮
        ttk.Button(advanced_frame, text="测试时区", command=self.test_timezone).grid(row=2, column=3, padx=(10, 0))
        
        # 显示连续性检查
        self.show_continuity_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(advanced_frame, text="显示连续性检查", variable=self.show_continuity_var).grid(row=1, column=3,
                                                                                                      sticky=tk.W,
                                                                                                      pady=2, padx=(20, 0))

        # 操作按钮
        button_frame = ttk.Frame(main_frame)
        button_frame.grid(row=4, column=0, columnspan=2, pady=(0, 10))

        self.get_list_button = ttk.Button(button_frame, text="获取日志列表", command=self.get_log_list)
        self.get_list_button.grid(row=0, column=0, padx=(0, 5))

        self.start_button = ttk.Button(button_frame, text="开始下载", command=self.start_download, state=tk.DISABLED)
        self.start_button.grid(row=0, column=1, padx=(5, 5))

        self.stop_button = ttk.Button(button_frame, text="停止下载", command=self.stop_download, state=tk.DISABLED)
        self.stop_button.grid(row=0, column=2, padx=(5, 5))

        self.clear_button = ttk.Button(button_frame, text="清空日志", command=self.clear_log)
        self.clear_button.grid(row=0, column=3, padx=(5, 5))

        self.save_button = ttk.Button(button_frame, text="保存日志", command=self.save_log)
        self.save_button.grid(row=0, column=4, padx=(5, 0))

        # 进度条
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(main_frame, variable=self.progress_var, maximum=100)
        self.progress_bar.grid(row=5, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 5))

        # 状态标签
        self.status_var = tk.StringVar(value="就绪")
        ttk.Label(main_frame, textvariable=self.status_var).grid(row=6, column=0, columnspan=2, sticky=tk.W)

        # 日志文件列表显示区域
        list_frame = ttk.LabelFrame(main_frame, text="日志文件列表", padding="10")
        list_frame.grid(row=7, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(10, 5))

        # 创建Treeview显示日志列表
        columns = ('日期', '文件名', '大小', '时间范围')
        self.log_tree = ttk.Treeview(list_frame, columns=columns, show='tree headings', height=8)

        # 设置列宽
        self.log_tree.column('#0', width=30, minwidth=30)
        self.log_tree.column('日期', width=100, minwidth=80)
        self.log_tree.column('文件名', width=350, minwidth=200)
        self.log_tree.column('大小', width=80, minwidth=60)
        self.log_tree.column('时间范围', width=200, minwidth=150)

        # 设置列标题
        self.log_tree.heading('#0', text='选择')
        self.log_tree.heading('日期', text='日期')
        self.log_tree.heading('文件名', text='文件名')
        self.log_tree.heading('大小', text='大小')
        self.log_tree.heading('时间范围', text='时间范围')

        # 添加滚动条
        tree_scroll = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=self.log_tree.yview)
        self.log_tree.configure(yscrollcommand=tree_scroll.set)

        self.log_tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        tree_scroll.grid(row=0, column=1, sticky=(tk.N, tk.S))

        # 列表操作按钮
        list_btn_frame = ttk.Frame(list_frame)
        list_btn_frame.grid(row=1, column=0, columnspan=2, pady=(5, 0))

        ttk.Button(list_btn_frame, text="✅ 全选", command=self.select_all_logs).grid(row=0, column=0, padx=(0, 5))
        ttk.Button(list_btn_frame, text="❌ 取消全选", command=self.deselect_all_logs).grid(row=0, column=1, padx=(5, 5))
        ttk.Button(list_btn_frame, text="🔄 刷新列表", command=self.get_log_list).grid(row=0, column=2, padx=(5, 5))
        ttk.Button(list_btn_frame, text="🔍 连续性检查", command=self.manual_continuity_check).grid(row=0, column=3, padx=(5, 0))

        # 日志显示区域
        log_frame = ttk.LabelFrame(main_frame, text="下载日志", padding="10")
        log_frame.grid(row=8, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(5, 0))

        self.log_text = scrolledtext.ScrolledText(log_frame, height=10, width=80)
        self.log_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # 配置网格权重
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        main_frame.rowconfigure(7, weight=1)
        main_frame.rowconfigure(8, weight=1)
        quick_time_frame.columnconfigure(3, weight=1)
        list_frame.columnconfigure(0, weight=1)
        list_frame.rowconfigure(0, weight=1)
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)
        config_frame.columnconfigure(1, weight=1)
        param_frame.columnconfigure(1, weight=1)
        path_frame.columnconfigure(0, weight=1)

    def browse_path(self):
        """浏览并选择下载路径"""
        path = filedialog.askdirectory()
        if path:
            self.download_path_var.set(path)
    
    def apply_quick_select(self):
        """应用快捷时间选择 - 参考nginx_monitoring_optimized.py"""
        selection = self.quick_select_var.get()
        now = datetime.now()

        if selection == "昨天":
            yesterday = now - timedelta(days=1)
            start_time = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
            end_time = yesterday.replace(hour=23, minute=59, second=59, microsecond=0)
        elif selection == "最近7天":
            start_time = (now - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
            end_time = (now - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=0)
        elif selection == "最近30天":
            start_time = (now - timedelta(days=30)).replace(hour=0, minute=0, second=0, microsecond=0)
            end_time = (now - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=0)
        elif selection == "本月":
            start_time = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            end_time = now.replace(hour=23, minute=59, second=59, microsecond=0)
        elif selection == "上月":
            # 计算上个月的第一天
            if now.month == 1:
                last_month = now.replace(year=now.year-1, month=12, day=1)
            else:
                last_month = now.replace(month=now.month-1, day=1)
            start_time = last_month.replace(hour=0, minute=0, second=0, microsecond=0)
            
            # 计算上个月的最后一天
            if now.month == 1:
                end_time = now.replace(year=now.year-1, month=12, day=31, hour=23, minute=59, second=59)
            else:
                # 获取上个月的最后一天
                next_month = now.replace(day=1) 
                end_time = (next_month - timedelta(days=1)).replace(hour=23, minute=59, second=59)
        else:  # 自定义
            return

        self.start_time_var.set(start_time.strftime("%Y-%m-%d"))
        self.end_time_var.set(end_time.strftime("%Y-%m-%d"))
        self.update_time_info()
    
    def update_time_info(self):
        """更新时间范围信息显示"""
        try:
            start_str = self.start_time_var.get()
            end_str = self.end_time_var.get()

            start_time = datetime.strptime(start_str, "%Y-%m-%d")
            end_time = datetime.strptime(end_str, "%Y-%m-%d")

            duration = end_time - start_time
            days = duration.days + 1  # 包含结束日期

            if days <= 0:
                self.time_info_var.set("⚠️ 结束日期必须大于等于开始日期")
            else:
                self.time_info_var.set(f"📅 时间跨度: {days}天")

        except ValueError:
            self.time_info_var.set("⚠️ 日期格式错误")

    def log_message(self, message):
        """在日志区域显示消息"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"
        self.log_text.insert(tk.END, log_entry)
        self.log_text.see(tk.END)
        self.root.update()

    def clear_log(self):
        """清空日志"""
        self.log_text.delete(1.0, tk.END)

    def save_log(self):
        """保存日志到文件"""
        if not self.log_text.get(1.0, tk.END).strip():
            messagebox.showwarning("警告", "没有日志内容可保存")
            return

        file_path = filedialog.asksaveasfilename(
            defaultextension=".txt",
            filetypes=[("文本文件", "*.txt"), ("所有文件", "*.*")]
        )

        if file_path:
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(self.log_text.get(1.0, tk.END))
                messagebox.showinfo("成功", f"日志已保存到：{file_path}")
            except Exception as e:
                messagebox.showerror("错误", f"保存日志失败：{str(e)}")

    def validate_inputs(self):
        """验证输入参数"""
        if not self.access_key_var.get().strip():
            messagebox.showerror("错误", "请输入AccessKey ID")
            return False

        if not self.secret_key_var.get().strip():
            messagebox.showerror("错误", "请输入AccessKey Secret")
            return False

        if not self.domain_var.get().strip():
            messagebox.showerror("错误", "请输入域名")
            return False

        try:
            start_time = datetime.strptime(self.start_time_var.get(), "%Y-%m-%d")
            end_time = datetime.strptime(self.end_time_var.get(), "%Y-%m-%d")

            if start_time > end_time:
                messagebox.showerror("错误", "开始日期必须早于或等于结束日期")
                return False

        except ValueError:
            messagebox.showerror("错误", "日期格式错误，请使用：YYYY-MM-DD")
            return False

        download_path = self.download_path_var.get().strip()
        if not download_path:
            messagebox.showerror("错误", "请选择下载路径")
            return False

        return True

    def fix_log_url(self, url):
        """修复日志下载URL，添加协议头"""
        if not url:
            return url
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        return url

    def format_file_size(self, size_bytes):
        """格式化文件大小显示"""
        if size_bytes == 0:
            return "0 B"
        size_names = ["B", "KB", "MB", "GB"]
        i = 0
        while size_bytes >= 1024 and i < len(size_names) - 1:
            size_bytes /= 1024
            i += 1
        return f"{size_bytes:.1f} {size_names[i]}"

    def select_all_logs(self):
        """全选日志文件"""
        for item in self.log_tree.get_children():
            for child in self.log_tree.get_children(item):
                self.log_tree.item(child, text='☑')

    def deselect_all_logs(self):
        """取消全选日志文件"""
        for item in self.log_tree.get_children():
            for child in self.log_tree.get_children(item):
                self.log_tree.item(child, text='☐')

    def get_domain_logs(self):
        """获取域名日志列表"""
        if not SDK_AVAILABLE:
            # 模拟数据用于测试
            self.log_message("SDK未安装，使用模拟数据")
            return self.generate_mock_logs()

        try:
            client = AcsClient(
                self.access_key_var.get(),
                self.secret_key_var.get(),
                self.region_var.get()
            )

            request = DescribeDcdnDomainLogRequest.DescribeDcdnDomainLogRequest()
            request.set_DomainName(self.domain_var.get())

            # 转换日期格式为API需要的格式，正确处理时区
            # 本地时间转换为UTC时间（阿里云API使用UTC）
            from datetime import timezone
            
            # 解析输入的日期（本地时间）
            start_local = datetime.strptime(self.start_time_var.get(), "%Y-%m-%d")
            end_local = datetime.strptime(self.end_time_var.get(), "%Y-%m-%d")
            
            # 设置为当天的开始和结束时间（本地时间）
            start_local = start_local.replace(hour=0, minute=0, second=0, microsecond=0)
            end_local = end_local.replace(hour=23, minute=59, second=59, microsecond=999999)
            
            # 转换为UTC时间（使用用户设置的时区）
            timezone_offset = self.timezone_var.get()
            start_utc = start_local - timedelta(hours=timezone_offset)
            end_utc = end_local - timedelta(hours=timezone_offset)
            
            self.log_message(f"时间范围转换：本地时区UTC+{timezone_offset} {start_local} ~ {end_local}")
            self.log_message(f"API查询UTC时间：{start_utc} ~ {end_utc}")
            
            # 验证时区设置的合理性
            if timezone_offset < -12 or timezone_offset > 14:
                self.log_message(f"⚠️ 时区设置异常：UTC+{timezone_offset}，请检查设置")

            request.set_StartTime(start_utc.strftime('%Y-%m-%dT%H:%M:%SZ'))
            request.set_EndTime(end_utc.strftime('%Y-%m-%dT%H:%M:%SZ'))

            response = client.do_action_with_exception(request)
            result = json.loads(response.decode('utf-8'))

            logs = []
            if 'DomainLogDetails' in result:
                for domain_detail in result['DomainLogDetails']['DomainLogDetail']:
                    if 'LogInfos' in domain_detail:
                        for log_info in domain_detail['LogInfos']['LogInfoDetail']:
                            # 修复URL
                            log_info['LogPath'] = self.fix_log_url(log_info['LogPath'])
                            logs.append(log_info)

            return logs

        except Exception as e:
            self.log_message(f"获取日志列表失败：{str(e)}")
            return []

    def generate_mock_logs(self):
        """生成模拟日志数据用于测试（时区已修正）"""
        logs = []
        start_time = datetime.strptime(self.start_time_var.get(), "%Y-%m-%d")
        end_time = datetime.strptime(self.end_time_var.get(), "%Y-%m-%d")

        current_time = start_time
        while current_time <= end_time:
            # 每天生成24个小时的日志文件（本地时间）
            for hour in range(24):
                file_time = current_time.replace(hour=hour)
                next_time = file_time + timedelta(hours=1)
                
                # 转换为UTC时间用于API格式（使用用户设置的时区）
                timezone_offset = self.timezone_var.get()
                file_time_utc = file_time - timedelta(hours=timezone_offset)
                next_time_utc = next_time - timedelta(hours=timezone_offset)

                logs.append({
                    'LogName': f"{self.domain_var.get()}_{file_time_utc.strftime('%Y_%m_%d_%H%M%S')}_{next_time_utc.strftime('%H%M%S')}.gz",
                    'LogPath': f"https://cdnlog2.oss-cn-hangzhou.aliyuncs.com/v1.l1cache/test/{self.domain_var.get()}/{file_time_utc.strftime('%Y_%m_%d')}/{self.domain_var.get()}_{file_time_utc.strftime('%Y_%m_%d_%H%M%S')}_{next_time_utc.strftime('%H%M%S')}.gz",
                    'LogSize': 1024000 + hour * 50000,  # 模拟不同大小
                    'StartTime': file_time_utc.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'EndTime': next_time_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
                })
            current_time += timedelta(days=1)

        return logs

    def get_log_list(self):
        """获取并显示日志列表"""
        if not self.validate_inputs():
            return

        self.log_message("正在获取日志列表...")
        self.get_list_button.config(state=tk.DISABLED)
        self.status_var.set("获取日志列表中...")

        def fetch_logs():
            logs = self.get_domain_logs()

            # 在主线程中更新UI
            self.root.after(0, self.update_log_list, logs)

        threading.Thread(target=fetch_logs, daemon=True).start()

    def update_log_list(self, logs):
        """更新日志列表显示"""
        # 清空现有列表
        for item in self.log_tree.get_children():
            self.log_tree.delete(item)

        self.current_logs = logs

        if not logs:
            self.log_message("未找到符合条件的日志文件")
            self.get_list_button.config(state=tk.NORMAL)
            self.start_button.config(state=tk.DISABLED)
            self.status_var.set("未找到日志文件")
            return

        # 按日期组织日志
        organized_logs = self.organize_logs_by_date(logs)

        # 插入日志信息到树形控件
        for date_str, date_logs in organized_logs.items():
            # 添加日期节点
            date_node = self.log_tree.insert('', 'end', text=f"📅 {date_str} ({len(date_logs)}个文件)",
                                             values=(date_str, f"{len(date_logs)}个文件", "", ""))

            # 添加该日期下的文件
            for log_info in date_logs:
                filename = log_info['LogName']
                file_size = self.format_file_size(log_info.get('LogSize', 0))
                # 显示本地时间范围
                timezone_offset = self.timezone_var.get()
                
                # 转换开始时间到本地时间
                start_utc_str = log_info['StartTime'].replace('Z', '')
                start_utc = datetime.strptime(start_utc_str, '%Y-%m-%dT%H:%M:%S')
                start_local = start_utc + timedelta(hours=timezone_offset)
                
                # 转换结束时间到本地时间
                end_utc_str = log_info['EndTime'].replace('Z', '')
                end_utc = datetime.strptime(end_utc_str, '%Y-%m-%dT%H:%M:%S')
                end_local = end_utc + timedelta(hours=timezone_offset)
                
                time_range = f"{start_local.strftime('%H:%M:%S')} ~ {end_local.strftime('%H:%M:%S')}"

                # 添加文件节点，默认选中
                self.log_tree.insert(date_node, 'end', text='☑',
                                     values=(date_str, filename, file_size, time_range))

        # 展开所有节点
        for item in self.log_tree.get_children():
            self.log_tree.item(item, open=True)

        self.log_message(f"找到 {len(logs)} 个日志文件，分布在 {len(organized_logs)} 天")
        
        # 更新时间信息显示
        self.update_time_info()
        self.get_list_button.config(state=tk.NORMAL)
        self.start_button.config(state=tk.NORMAL)
        self.status_var.set(f"共找到 {len(logs)} 个日志文件")

        # 绑定点击事件切换选择状态
        self.log_tree.bind('<Button-1>', self.on_tree_click)
        
        # 显示连续性检查结果
        if self.show_continuity_var.get():
            self.show_continuity_check(logs)

    def on_tree_click(self, event):
        """处理树形控件点击事件"""
        item = self.log_tree.identify('item', event.x, event.y)
        column = self.log_tree.identify('column', event.x, event.y)

        # 只处理点击第一列（选择框）的情况
        if column == '#0' and item:
            # 如果点击的是文件项（不是日期节点）
            if self.log_tree.parent(item):  # 有父节点说明是文件项
                current_value = self.log_tree.item(item, 'text')
                new_value = '☐' if current_value == '☑' else '☑'
                self.log_tree.item(item, text=new_value)

    def show_continuity_check(self, logs):
        """显示连续性检查结果"""
        if not logs:
            return
            
        continuity_result = self.continuity_checker.check_continuity(logs)
        
        self.log_message(f"🔍 连续性检查结果:")
        self.log_message(f"  • 总文件数: {continuity_result['total_files']}")
        self.log_message(f"  • 有效文件数: {continuity_result['valid_files']}")
        
        if continuity_result['invalid_files']:
            self.log_message(f"  • 无效文件数: {len(continuity_result['invalid_files'])}")
            for invalid_file in continuity_result['invalid_files'][:3]:  # 只显示前3个
                self.log_message(f"    - {invalid_file['LogName']}")
        
        if continuity_result['continuous']:
            self.log_message(f"  ✅ 日志文件时间连续")
        else:
            self.log_message(f"  ⚠️ 发现 {len(continuity_result['gaps'])} 个时间间隙:")
            for i, (start_time, end_time, duration) in enumerate(continuity_result['gaps'][:5]):  # 只显示前5个
                self.log_message(f"    {i+1}. {start_time.strftime('%Y-%m-%d %H:%M:%S')} ~ {end_time.strftime('%Y-%m-%d %H:%M:%S')} (间隙: {duration})")
    
    def organize_logs_by_date(self, logs):
        """按日期组织日志（按本地时区分类）"""
        organized = {}
        timezone_offset = self.timezone_var.get()
        
        # 记录时区转换的示例
        conversion_examples = set()
        
        for log in logs:
            # 解析UTC时间
            utc_time_str = log['StartTime'].replace('Z', '')
            utc_time = datetime.strptime(utc_time_str, '%Y-%m-%dT%H:%M:%S')
            
            # 转换为本地时间
            local_time = utc_time + timedelta(hours=timezone_offset)
            date_str = local_time.strftime('%Y-%m-%d')
            
            # 收集转换示例用于调试
            utc_date = utc_time.strftime('%Y-%m-%d')
            local_date = local_time.strftime('%Y-%m-%d')
            if utc_date != local_date and len(conversion_examples) < 3:
                conversion_examples.add(f"UTC {utc_date} {utc_time.strftime('%H:%M')} → 本地 {local_date} {local_time.strftime('%H:%M')}")
            
            if date_str not in organized:
                organized[date_str] = []
            organized[date_str].append(log)
        
        # 显示时区转换示例
        if conversion_examples:
            self.log_message("📅 文件夹分类按本地时区处理，示例转换：")
            for example in list(conversion_examples)[:2]:
                self.log_message(f"   {example}")
            
        return organized

    def download_file(self, log_info, date_folder):
        """下载单个文件 - 增强版"""
        try:
            url = self.fix_log_url(log_info['LogPath'])  # 修复URL
            filename = log_info['LogName']
            file_path = os.path.join(date_folder, filename)
            file_size = log_info.get('LogSize', 0)

            # 检查下载历史
            if self.use_history_var.get() and self.history_db.is_downloaded(url):
                self.log_message(f"💾 历史记录显示已下载：{filename}")
                return True

            # 检查文件是否已存在
            if self.skip_existing_var.get() and os.path.exists(file_path):
                self.log_message(f"📁 跳过已存在文件：{filename}")
                # 记录到历史数据库
                if self.use_history_var.get():
                    self.history_db.add_download_record(url, filename, file_size, file_path, "skipped")
                return True

            # 创建目录
            os.makedirs(date_folder, exist_ok=True)

            # 下载文件
            self.log_message(f"📥 下载中：{filename} ({self.format_file_size(file_size)})")
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()

            with open(file_path, 'wb') as f:
                downloaded = 0
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        # 显示进度（每1MB显示一次）
                        if file_size > 0 and downloaded % (1024*1024) == 0:
                            progress = downloaded / file_size * 100
                            self.log_message(f"  进度: {progress:.1f}% ({self.format_file_size(downloaded)}/{self.format_file_size(file_size)})")

            # 验证文件完整性
            if self.verify_var.get():
                downloaded_size = os.path.getsize(file_path)
                if file_size > 0 and abs(downloaded_size - file_size) > 1024:
                    self.log_message(f"⚠️ 文件大小不匹配：{filename} (预期:{self.format_file_size(file_size)}, 实际:{self.format_file_size(downloaded_size)})")
                    return False

            # 记录成功下载
            if self.use_history_var.get():
                self.history_db.add_download_record(url, filename, file_size, file_path, "completed")
            
            self.log_message(f"✅ 下载成功：{filename}")
            return True

        except Exception as e:
            self.log_message(f"❌ 下载失败：{filename} - {str(e)}")
            return False

    def download_worker(self, organized_logs):
        """下载工作线程"""
        base_path = self.download_path_var.get()
        retry_count = self.retry_var.get()

        for date_str, logs in organized_logs.items():
            if not self.is_downloading:
                break

            date_folder = os.path.join(base_path, date_str)
            self.log_message(f"开始下载 {date_str} 的日志 ({len(logs)} 个文件)")

            for log_info in logs:
                if not self.is_downloading:
                    break

                success = False
                for attempt in range(retry_count):
                    if self.download_file(log_info, date_folder):
                        success = True
                        self.completed_files += 1
                        break
                    else:
                        if attempt < retry_count - 1:
                            self.log_message(f"重试下载：{log_info['LogName']} (第{attempt + 1}次)")
                            time.sleep(1)

                if not success:
                    self.failed_files += 1

                # 更新进度
                progress = (self.completed_files + self.failed_files) / self.total_files * 100
                self.progress_var.set(progress)
                self.status_var.set(f"进度: {self.completed_files}/{self.total_files} (失败: {self.failed_files})")

                time.sleep(0.1)  # 避免过于频繁的请求

        # 下载完成
        self.is_downloading = False
        self.start_button.config(state=tk.NORMAL)
        self.stop_button.config(state=tk.DISABLED)
        self.get_list_button.config(state=tk.NORMAL)

        # 统计信息
        total_size = sum(log.get('LogSize', 0) for logs in organized_logs.values() for log in logs)
        success_rate = (self.completed_files / self.total_files * 100) if self.total_files > 0 else 0
        
        self.log_message(f"")
        self.log_message(f"🎉 下载任务完成！")
        self.log_message(f"📈 统计信息:")
        self.log_message(f"  • 成功下载: {self.completed_files} 个文件")
        self.log_message(f"  • 失败文件: {self.failed_files} 个文件")
        self.log_message(f"  • 成功率: {success_rate:.1f}%")
        self.log_message(f"  • 总数据量: {self.format_file_size(total_size)}")
        
        if self.failed_files > 0:
            result_msg = f"📁 下载完成！\n\n📈 统计信息:\n• 成功: {self.completed_files} 个文件\n• 失败: {self.failed_files} 个文件\n• 成功率: {success_rate:.1f}%\n• 数据量: {self.format_file_size(total_size)}"
            messagebox.showwarning("下载完成", result_msg)
        else:
            result_msg = f"🎉 下载完成！\n\n全部 {self.completed_files} 个文件下载成功！\n总数据量: {self.format_file_size(total_size)}"
            messagebox.showinfo("下载成功", result_msg)

    def start_download(self):
        """开始下载选中的日志文件"""
        if not self.current_logs:
            messagebox.showwarning("警告", "请先获取日志列表")
            return

        # 获取选中的日志
        selected_logs = []
        for item in self.log_tree.get_children():
            for child in self.log_tree.get_children(item):
                if self.log_tree.item(child, 'text') == '☑':
                    # 通过值匹配找到对应的日志信息
                    filename = self.log_tree.set(child, '文件名')
                    for log in self.current_logs:
                        if log['LogName'] == filename:
                            selected_logs.append(log)
                            break

        if not selected_logs:
            messagebox.showwarning("警告", "请选择要下载的日志文件")
            return

        if self.is_downloading:
            messagebox.showwarning("警告", "正在下载中，请等待完成或停止当前下载")
            return

        self.is_downloading = True
        self.start_button.config(state=tk.DISABLED)
        self.stop_button.config(state=tk.NORMAL)
        self.get_list_button.config(state=tk.DISABLED)
        self.completed_files = 0
        self.failed_files = 0
        self.total_files = len(selected_logs)
        self.progress_var.set(0)

        self.log_message(f"开始下载 {self.total_files} 个选中的日志文件...")

        # 在后台线程中执行下载
        def download_thread():
            organized_logs = self.organize_logs_by_date(selected_logs)
            self.download_worker(organized_logs)

        threading.Thread(target=download_thread, daemon=True).start()

    def stop_download(self):
        """停止下载"""
        if self.is_downloading:
            self.is_downloading = False
            self.log_message("正在停止下载...")
            self.status_var.set("停止中...")

            # 重新启用按钮
            self.start_button.config(state=tk.NORMAL)
            self.stop_button.config(state=tk.DISABLED)
            self.get_list_button.config(state=tk.NORMAL)
    
    def manual_continuity_check(self):
        """手动执行连续性检查"""
        if not self.current_logs:
            messagebox.showwarning("警告", "请先获取日志列表")
            return
        
        self.log_message("🔍 执行手动连续性检查...")
        self.show_continuity_check(self.current_logs)

    def test_timezone(self):
        """测试时区设置效果"""
        timezone_offset = self.timezone_var.get()
        now_utc = datetime.utcnow()
        now_local = now_utc + timedelta(hours=timezone_offset)
        
        self.log_message("🕐 时区设置测试：")
        self.log_message(f"   当前UTC时间：{now_utc.strftime('%Y-%m-%d %H:%M:%S')}")
        self.log_message(f"   本地时间(UTC+{timezone_offset})：{now_local.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 测试文件夹分类示例
        test_utc_times = [
            "2025-06-21T16:00:00Z",  # UTC下午4点
            "2025-06-22T15:59:59Z",  # UTC下午3点59分
            "2025-06-22T16:00:00Z",  # UTC下午4点
        ]
        
        self.log_message(f"   文件夹分类示例（UTC+{timezone_offset}）：")
        for utc_time_str in test_utc_times:
            utc_time = datetime.strptime(utc_time_str.replace('Z', ''), '%Y-%m-%dT%H:%M:%S')
            local_time = utc_time + timedelta(hours=timezone_offset)
            folder_name = local_time.strftime('%Y-%m-%d')
            self.log_message(f"     UTC {utc_time.strftime('%m-%d %H:%M')} → 文件夹 {folder_name}")


def main():
    root = tk.Tk()
    app = DCDNLogDownloader(root)
    root.mainloop()


if __name__ == "__main__":
    main()