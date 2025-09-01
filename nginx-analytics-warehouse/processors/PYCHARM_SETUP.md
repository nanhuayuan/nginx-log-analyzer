# PyCharm配置指南

## 1. 项目导入

### 步骤1: 打开项目
1. 启动PyCharm
2. 选择 "File" -> "Open" 
3. 选择目录：`D:\project\nginx-log-analyzer\nginx-analytics-warehouse\processors`
4. 点击"OK"确认

### 步骤2: Python解释器配置
1. 打开设置：`File` -> `Settings` (Ctrl+Alt+S)
2. 导航到：`Project` -> `Python Interpreter`
3. 点击齿轮图标 -> `Add...`
4. 选择 `Existing environment`
5. 解释器路径：`D:\soft\Anaconda3\python.exe`
6. 点击"OK"确认

## 2. 运行配置

### 主处理器配置
创建运行配置：
1. 点击右上角的配置下拉框 -> `Edit Configurations...`
2. 点击"+"号 -> `Python`
3. 配置如下：
   - **Name**: `Process All Logs`
   - **Script path**: `D:\project\nginx-log-analyzer\nginx-analytics-warehouse\processors\main_simple.py`
   - **Parameters**: `process-all`
   - **Working directory**: `D:\project\nginx-log-analyzer\nginx-analytics-warehouse\processors`
   - **Python interpreter**: 选择Anaconda解释器

### 其他有用的配置

#### 处理指定日期
- **Name**: `Process Date`
- **Parameters**: `process --date 20250422`
- 其他配置同上

#### 系统状态检查
- **Name**: `Show Status`
- **Parameters**: `status`
- 其他配置同上

#### 启动服务
- **Name**: `Start Services`
- **Parameters**: `start-services`
- 其他配置同上

## 3. 调试配置

### 断点调试
推荐在以下关键位置设置断点：
1. `nginx_processor_complete.py:line_80` - `process_all_unprocessed_logs()`方法开始
2. `nginx_processor_complete.py:line_200` - `parse_base_log_line()`日志解析
3. `nginx_processor_complete.py:line_300` - 数据库插入操作

### 调试步骤
1. 在代码行号左侧点击设置红色断点
2. 右键运行配置 -> `Debug 'Process All Logs'`
3. 使用调试工具栏：
   - `F8`: Step Over (单步跳过)
   - `F7`: Step Into (单步进入)
   - `F9`: Resume Program (继续运行)

## 4. 代码导航

### 项目结构
```
processors/
├── main_simple.py              # 主入口文件
├── nginx_processor_complete.py # 核心处理器
├── show_data_flow.py          # 状态检查
├── validate_processing.py     # 数据验证  
├── docker-compose-simple-fixed.yml # Docker配置
├── processed_logs_complete.json    # 处理记录
├── DEPLOYMENT_GUIDE.md        # 部署指南
└── PYCHARM_SETUP.md          # 本文件
```

### 关键文件说明
- **主入口**: `main_simple.py` - 提供命令行界面
- **核心逻辑**: `nginx_processor_complete.py` - 包含所有数据处理逻辑
- **验证工具**: `validate_processing.py` - 检查数据质量

## 5. 代码编辑配置

### 代码格式设置
1. `Settings` -> `Editor` -> `Code Style` -> `Python`
2. 建议配置：
   - **Tab size**: 4
   - **Indent**: 4
   - **Use tab character**: 取消勾选
   - **Line separator**: LF (Unix)

### 自动导入优化
1. `Settings` -> `Editor` -> `General` -> `Auto Import`
2. 勾选：
   - `Add unambiguous imports on the fly`
   - `Optimize imports on the fly`

## 6. 版本控制

### Git集成
1. `VCS` -> `Enable Version Control Integration`
2. 选择 `Git`
3. 设置Git根目录：`D:\project\nginx-log-analyzer`

### 推荐.gitignore
```gitignore
# Python
__pycache__/
*.py[cod]
*$py.class
*.egg-info/
.venv/

# 处理记录和日志
processed_logs_complete.json
nginx_logs/

# IDE
.idea/
*.swp
*.swo

# 临时文件
*.tmp
*.temp
```

## 7. 插件推荐

### 有用的PyCharm插件
1. **Docker** - Docker文件支持和容器管理
2. **Database Tools and SQL** - 数据库连接和SQL编辑
3. **Markdown** - 文档编辑支持
4. **Python Security** - 代码安全检查

### 安装方式
1. `File` -> `Settings` -> `Plugins`
2. 搜索插件名称并安装
3. 重启PyCharm

## 8. 数据库连接配置

### ClickHouse连接
1. 打开Database工具窗口：`View` -> `Tool Windows` -> `Database`
2. 点击"+"号 -> `Data Source` -> `ClickHouse`
3. 配置连接：
   - **Host**: localhost
   - **Port**: 8123
   - **Database**: nginx_analytics
   - **User**: analytics_user
   - **Password**: analytics_password
4. 测试连接并保存

### SQL Console使用
连接成功后可以直接在PyCharm中执行SQL查询：
```sql
-- 检查数据量
SELECT 'ods_nginx_raw' as table_name, count() as records FROM ods_nginx_raw
UNION ALL
SELECT 'dwd_nginx_enriched' as table_name, count() as records FROM dwd_nginx_enriched
UNION ALL  
SELECT 'ads_top_hot_apis' as table_name, count() as records FROM ads_top_hot_apis;

-- 查看最新处理的数据
SELECT * FROM dwd_nginx_enriched ORDER BY log_time DESC LIMIT 10;
```

## 9. 快速开发工作流

### 日常开发流程
1. **启动服务**: 运行`Start Services`配置
2. **处理日志**: 运行`Process All Logs`配置
3. **验证结果**: 在Database工具中查询数据
4. **调试问题**: 设置断点重新运行
5. **提交代码**: 使用Git工具提交变更

### 测试流程
1. 准备测试日志文件到nginx_logs目录
2. 清空现有数据：运行`Clear All Data`
3. 处理测试数据：运行`Process All Logs`
4. 验证结果：运行`Show Status`

## 10. 故障排查

### 常见问题
1. **模块导入错误**: 检查Python解释器配置
2. **Docker连接失败**: 确保Docker Desktop运行
3. **数据库连接超时**: 检查ClickHouse容器状态
4. **编码错误**: 确保文件编码设置为UTF-8

### 调试技巧
1. 使用PyCharm内置的Python Console测试代码片段
2. 利用Variables窗口查看运行时变量值
3. 使用Evaluate Expression功能测试表达式
4. 查看Run窗口的完整输出日志

---

配置完成后，建议先运行`Process All Logs`配置验证系统正常工作。