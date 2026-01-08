#Requires -RunAsAdministrator
# ==========================================
# ETL自动配置脚本
# 自动修改ETL连接到K8s ClickHouse
# 无需手动修改任何文件
# ==========================================

$ErrorActionPreference = "Stop"

$PROJECT_ROOT = "D:\project\nginx-log-analyzer\nginx-analytics-warehouse"
$ETL_PATH = "$PROJECT_ROOT\etl"
$WRITER_FILE = "$ETL_PATH\writers\dwd_writer.py"

# K8s ClickHouse连接配置
$K8S_HOST = "192.168.0.140"
$K8S_PORT = "8123"
$K8S_USER = "analytics_user"
$K8S_PASSWORD = "analytics_password_change_in_prod"
$K8S_DATABASE = "nginx_analytics"

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "ETL自动配置 - 连接到K8s ClickHouse" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""

# 检查文件存在
Write-Host "[1/4] 检查ETL文件..." -ForegroundColor Yellow
if (-not (Test-Path $WRITER_FILE)) {
    Write-Host "✗ 错误: Writer文件不存在: $WRITER_FILE" -ForegroundColor Red
    Read-Host "按Enter键退出"
    exit 1
}
Write-Host "✓ ETL文件存在" -ForegroundColor Green
Write-Host ""

# 备份原文件
Write-Host "[2/4] 备份原配置..." -ForegroundColor Yellow
$backupFile = "$WRITER_FILE.backup.$(Get-Date -Format 'yyyyMMddHHmmss')"
try {
    Copy-Item $WRITER_FILE $backupFile
    Write-Host "✓ 备份已创建: $(Split-Path $backupFile -Leaf)" -ForegroundColor Green
} catch {
    Write-Host "⚠ 警告: 无法创建备份" -ForegroundColor Yellow
}
Write-Host ""

# 读取并修改配置
Write-Host "[3/4] 修改ClickHouse连接配置..." -ForegroundColor Yellow
Write-Host "  目标配置:" -ForegroundColor Gray
Write-Host "    Host: $K8S_HOST" -ForegroundColor Gray
Write-Host "    Port: $K8S_PORT" -ForegroundColor Gray
Write-Host "    User: $K8S_USER" -ForegroundColor Gray
Write-Host "    Database: $K8S_DATABASE" -ForegroundColor Gray
Write-Host ""

try {
    $content = Get-Content $WRITER_FILE -Raw

    # 替换host配置
    $content = $content -replace "self\.host\s*=\s*['\"].*?['\"]", "self.host = '$K8S_HOST'"

    # 替换port配置
    $content = $content -replace "self\.port\s*=\s*\d+", "self.port = $K8S_PORT"

    # 替换user配置（如果存在）
    $content = $content -replace "self\.user\s*=\s*['\"].*?['\"]", "self.user = '$K8S_USER'"

    # 替换password配置（如果存在）
    $content = $content -replace "self\.password\s*=\s*['\"].*?['\"]", "self.password = '$K8S_PASSWORD'"

    # 替换database配置（如果存在）
    $content = $content -replace "self\.database\s*=\s*['\"].*?['\"]", "self.database = '$K8S_DATABASE'"

    # 写回文件
    $content | Set-Content $WRITER_FILE -Encoding UTF8

    Write-Host "✓ 配置文件已更新" -ForegroundColor Green
} catch {
    Write-Host "✗ 错误: 配置修改失败" -ForegroundColor Red
    Write-Host "错误信息: $_" -ForegroundColor Red

    # 恢复备份
    if (Test-Path $backupFile) {
        Write-Host "  正在恢复备份..." -ForegroundColor Yellow
        Copy-Item $backupFile $WRITER_FILE -Force
        Write-Host "  ✓ 已恢复原配置" -ForegroundColor Green
    }

    Read-Host "按Enter键退出"
    exit 1
}
Write-Host ""

# 测试连接
Write-Host "[4/4] 测试ClickHouse连接..." -ForegroundColor Yellow
try {
    $response = Invoke-WebRequest -Uri "http://${K8S_HOST}:${K8S_PORT}/ping" -UseBasicParsing -TimeoutSec 5
    if ($response.StatusCode -eq 200 -and $response.Content -eq "Ok.`n") {
        Write-Host "✓ ClickHouse连接测试成功" -ForegroundColor Green
    } else {
        Write-Host "⚠ 警告: ClickHouse响应异常" -ForegroundColor Yellow
    }
} catch {
    Write-Host "⚠ 警告: 无法连接到ClickHouse" -ForegroundColor Yellow
    Write-Host "  请确认K8s服务已启动" -ForegroundColor Gray
}
Write-Host ""

# 显示验证信息
Write-Host "==========================================" -ForegroundColor Green
Write-Host "✓ ETL配置完成!" -ForegroundColor Green
Write-Host "==========================================" -ForegroundColor Green
Write-Host ""
Write-Host "配置已修改:" -ForegroundColor Cyan
Write-Host "  文件: dwd_writer.py" -ForegroundColor White
Write-Host "  ClickHouse连接: $K8S_HOST:$K8S_PORT" -ForegroundColor White
Write-Host ""
Write-Host "下一步 - 运行ETL测试:" -ForegroundColor Yellow
Write-Host ""
Write-Host "  # 1. 切换到ETL目录" -ForegroundColor Gray
Write-Host "  cd D:\project\nginx-log-analyzer\nginx-analytics-warehouse\etl" -ForegroundColor White
Write-Host ""
Write-Host "  # 2. 激活conda环境" -ForegroundColor Gray
Write-Host "  conda activate py39" -ForegroundColor White
Write-Host ""
Write-Host "  # 3. 测试处理100条记录" -ForegroundColor Gray
Write-Host "  python controllers\integrated_ultra_etl_controller.py --date 20250106 --test --limit 100" -ForegroundColor White
Write-Host ""
Write-Host "  # 4. 验证数据写入" -ForegroundColor Gray
Write-Host "  kubectl exec -it clickhouse-0 -n nginx-analytics -- clickhouse-client --user=analytics_user --password=analytics_password_change_in_prod -q `"SELECT count(*) FROM nginx_analytics.dwd_nginx_enriched_v3`"" -ForegroundColor White
Write-Host ""
Write-Host "==========================================" -ForegroundColor Cyan

Read-Host "按Enter键退出"
