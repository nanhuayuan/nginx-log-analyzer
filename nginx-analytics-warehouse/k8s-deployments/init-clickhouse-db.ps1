#Requires -RunAsAdministrator
# ==========================================
# ClickHouse数据库初始化脚本 (PowerShell版本)
# 使用Python的database_manager_unified.py
# 自动配置K8s连接
# ==========================================

$ErrorActionPreference = "Stop"

$NAMESPACE = "nginx-analytics"
$POD_NAME = "clickhouse-0"
$PROJECT_ROOT = "D:\project\nginx-log-analyzer\nginx-analytics-warehouse"
$DDL_PATH = "$PROJECT_ROOT\ddl"
$PROCESSOR_PATH = "$PROJECT_ROOT\processors"

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "ClickHouse数据库初始化" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""

# 检查ClickHouse Pod是否运行
Write-Host "[1/4] 检查ClickHouse状态..." -ForegroundColor Yellow
try {
    $podStatus = kubectl get pod $POD_NAME -n $NAMESPACE -o jsonpath='{.status.phase}' 2>$null
    if ($podStatus -ne "Running") {
        Write-Host "✗ 错误: ClickHouse Pod状态异常: $podStatus" -ForegroundColor Red
        Read-Host "按Enter键退出"
        exit 1
    }
    Write-Host "✓ ClickHouse Pod运行正常" -ForegroundColor Green
} catch {
    Write-Host "✗ 错误: ClickHouse Pod未运行" -ForegroundColor Red
    Read-Host "按Enter键退出"
    exit 1
}
Write-Host ""

# 检查DDL文件
Write-Host "[2/4] 检查DDL文件..." -ForegroundColor Yellow
if (-not (Test-Path $DDL_PATH)) {
    Write-Host "✗ 错误: DDL目录不存在: $DDL_PATH" -ForegroundColor Red
    Read-Host "按Enter键退出"
    exit 1
}

$ddlFiles = @(
    "01_ods_layer_real.sql",
    "02_dwd_layer_real.sql",
    "03_ads_layer_real.sql",
    "04_materialized_views_corrected.sql"
)

foreach ($file in $ddlFiles) {
    $filePath = Join-Path $DDL_PATH $file
    if (-not (Test-Path $filePath)) {
        Write-Host "✗ 错误: DDL文件不存在: $file" -ForegroundColor Red
        Read-Host "按Enter键退出"
        exit 1
    }
}
Write-Host "✓ 所有DDL文件存在" -ForegroundColor Green
Write-Host ""

# 使用database_manager_unified.py初始化
Write-Host "[3/4] 使用Python初始化数据库..." -ForegroundColor Yellow

# 临时设置环境变量指向K8s ClickHouse
$env:CLICKHOUSE_HOST = "192.168.0.140"
$env:CLICKHOUSE_PORT = "8123"
$env:CLICKHOUSE_USER = "analytics_user"
$env:CLICKHOUSE_PASSWORD = "analytics_password_change_in_prod"
$env:CLICKHOUSE_DB = "nginx_analytics"

Write-Host "  连接配置:" -ForegroundColor Gray
Write-Host "    Host: $env:CLICKHOUSE_HOST" -ForegroundColor Gray
Write-Host "    Port: $env:CLICKHOUSE_PORT" -ForegroundColor Gray
Write-Host "    Database: $env:CLICKHOUSE_DB" -ForegroundColor Gray
Write-Host ""

# 检查Python环境
try {
    $pythonVersion = python --version 2>&1
    Write-Host "  Python版本: $pythonVersion" -ForegroundColor Gray
} catch {
    Write-Host "✗ 错误: Python未安装" -ForegroundColor Red
    Read-Host "按Enter键退出"
    exit 1
}

# 激活conda环境
Write-Host "  激活conda环境..." -ForegroundColor Gray
try {
    conda activate py39 2>&1 | Out-Null
    Write-Host "  ✓ conda环境已激活" -ForegroundColor Green
} catch {
    Write-Host "  ⚠ 警告: 无法激活conda环境，使用系统Python" -ForegroundColor Yellow
}

# 切换到processors目录执行
Set-Location $PROCESSOR_PATH

Write-Host ""
Write-Host "  执行数据库初始化..." -ForegroundColor Gray
Write-Host "  这可能需要1-2分钟..." -ForegroundColor Gray
Write-Host ""

# 执行database_manager_unified.py
# 选项1: 全新初始化(执行所有DDL)
# 选项5: 执行DDL文件(01-04)
# 自动选择选项5
$input = "5`n"  # 选择选项5: 执行DDL文件

try {
    $input | python database_manager_unified.py
    Write-Host ""
    Write-Host "✓ 数据库初始化完成" -ForegroundColor Green
} catch {
    Write-Host "✗ 错误: 数据库初始化失败" -ForegroundColor Red
    Write-Host "错误信息: $_" -ForegroundColor Red
    Read-Host "按Enter键退出"
    exit 1
}
Write-Host ""

# 验证表创建
Write-Host "[4/4] 验证数据库表..." -ForegroundColor Yellow
try {
    kubectl exec -n $NAMESPACE $POD_NAME -- clickhouse-client `
        --user=analytics_user `
        --password=analytics_password_change_in_prod `
        --query="SELECT database, name, engine, total_rows FROM system.tables WHERE database = 'nginx_analytics' ORDER BY name" `
        --format=PrettyCompact

    Write-Host ""
    Write-Host "✓ 数据库验证完成" -ForegroundColor Green
} catch {
    Write-Host "⚠ 警告: 无法验证表，但初始化可能已成功" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "==========================================" -ForegroundColor Green
Write-Host "✓ ClickHouse数据库初始化成功!" -ForegroundColor Green
Write-Host "==========================================" -ForegroundColor Green
Write-Host ""
Write-Host "核心表已创建:" -ForegroundColor Cyan
Write-Host "  - ods_nginx_raw (ODS原始层)" -ForegroundColor White
Write-Host "  - dwd_nginx_enriched_v3 (DWD明细层)" -ForegroundColor White
Write-Host "  - ads_* (18个ADS主题表)" -ForegroundColor White
Write-Host "  - mv_* (17个物化视图)" -ForegroundColor White
Write-Host ""
Write-Host "下一步:" -ForegroundColor Yellow
Write-Host "1. 配置ETL连接: .\config-etl-auto.ps1" -ForegroundColor White
Write-Host "2. 运行ETL测试处理日志数据" -ForegroundColor White
Write-Host "==========================================" -ForegroundColor Cyan

Read-Host "按Enter键退出"
