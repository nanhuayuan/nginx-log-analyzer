# Nginx日志ETL自动处理系统 (PowerShell版本)
# 解决clickhouse_connect依赖问题，实现日志双重输出

param(
    [int]$Duration = 7200,      # 运行时长(秒)，默认2小时
    [int]$BatchSize = 3000,     # 批处理大小
    [int]$Workers = 6,          # 工作线程数
    [int]$RefreshMinutes = 2    # 刷新间隔(分钟)
)

# 设置控制台编码
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

Write-Host "=============================================" -ForegroundColor Green
Write-Host "Nginx日志ETL自动处理系统 (PowerShell版)" -ForegroundColor Green
Write-Host "启动时间: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Green
Write-Host "=============================================" -ForegroundColor Green
Write-Host

# 切换到脚本所在目录
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
Set-Location $ScriptDir
Write-Host "当前工作目录: $PWD" -ForegroundColor Yellow
Write-Host

# 激活conda环境
Write-Host "🔄 激活conda环境..." -ForegroundColor Blue
try {
    & conda activate py39
    if ($LASTEXITCODE -ne 0) {
        throw "conda activate失败"
    }
    Write-Host "✅ conda环境激活成功" -ForegroundColor Green
} catch {
    Write-Host "❌ 错误: 无法激活conda环境py39" -ForegroundColor Red
    Write-Host "请检查conda是否已安装以及py39环境是否存在" -ForegroundColor Red
    Write-Host
    Write-Host "常见解决方案:" -ForegroundColor Yellow
    Write-Host "1. 检查conda是否在PATH中: conda --version" -ForegroundColor Yellow
    Write-Host "2. 检查环境是否存在: conda env list" -ForegroundColor Yellow
    Write-Host "3. 创建环境: conda create -n py39 python=3.9" -ForegroundColor Yellow
    Read-Host "按任意键退出"
    exit 1
}
Write-Host

# 检查Python版本
Write-Host "🐍 检查Python版本..." -ForegroundColor Blue
try {
    $pythonVersion = & python --version 2>&1
    Write-Host "Python版本: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "❌ 错误: Python不可用" -ForegroundColor Red
    Read-Host "按任意键退出"
    exit 1
}
Write-Host

# 检查ETL控制器文件
Write-Host "📂 检查ETL控制器文件..." -ForegroundColor Blue
$ControllerFile = "controllers\integrated_ultra_etl_controller-v1.py"
if (-not (Test-Path $ControllerFile)) {
    Write-Host "❌ 错误: 未找到ETL控制器文件" -ForegroundColor Red
    Write-Host "期望文件: $ControllerFile" -ForegroundColor Red
    Write-Host "当前目录: $PWD" -ForegroundColor Red
    Write-Host
    Write-Host "请确保脚本在正确的ETL目录下运行" -ForegroundColor Yellow
    Read-Host "按任意键退出"
    exit 1
}
Write-Host "✅ ETL控制器文件存在" -ForegroundColor Green
Write-Host

# 检查和安装依赖
Write-Host "🔍 检查关键依赖..." -ForegroundColor Blue

# 检查clickhouse_connect
try {
    & python -c "import clickhouse_connect; print('✅ clickhouse_connect 可用')" 2>$null
    if ($LASTEXITCODE -ne 0) { throw "clickhouse_connect不可用" }
    Write-Host "✅ clickhouse_connect 可用" -ForegroundColor Green
} catch {
    Write-Host "❌ 缺少关键依赖: clickhouse_connect" -ForegroundColor Red
    Write-Host
    Write-Host "🔧 正在自动安装依赖..." -ForegroundColor Yellow
    & pip install clickhouse_connect
    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ 自动安装失败" -ForegroundColor Red
        Write-Host
        Write-Host "手动安装方案:" -ForegroundColor Yellow
        Write-Host "1. pip install clickhouse_connect" -ForegroundColor Yellow
        Write-Host "2. 或者运行: check_and_install_dependencies.bat" -ForegroundColor Yellow
        Read-Host "按任意键退出"
        exit 1
    }
    Write-Host "✅ clickhouse_connect 安装成功" -ForegroundColor Green
}

# 检查其他依赖
$dependencies = @("pandas", "pathlib")
foreach ($dep in $dependencies) {
    try {
        & python -c "import $dep; print('✅ $dep 可用')" 2>$null
        if ($LASTEXITCODE -ne 0) { throw "$dep不可用" }
        Write-Host "✅ $dep 可用" -ForegroundColor Green
    } catch {
        Write-Host "📦 安装$dep..." -ForegroundColor Yellow
        & pip install $dep
    }
}

Write-Host "✅ 所有依赖检查完成" -ForegroundColor Green
Write-Host

# 设置日志文件
$LogDir = Join-Path $ScriptDir "logs"
if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir | Out-Null
    Write-Host "✅ 创建日志目录: $LogDir" -ForegroundColor Green
}

$DateStr = Get-Date -Format "yyyyMMdd_HHmm"
$LogFile = Join-Path $LogDir "etl_auto_$DateStr.log"

Write-Host "📝 日志文件: $LogFile" -ForegroundColor Yellow
Write-Host

# 构建ETL命令
$ETLCmd = "python $ControllerFile --auto-monitor --monitor-duration $Duration --batch-size $BatchSize --workers $Workers --refresh-minutes $RefreshMinutes"

Write-Host "🚀 ETL处理配置:" -ForegroundColor Blue
Write-Host "  控制器: integrated_ultra_etl_controller-v1.py" -ForegroundColor White
Write-Host "  模式: 自动监控 ($([math]::Round($Duration/3600, 1))小时)" -ForegroundColor White
Write-Host "  批大小: $BatchSize" -ForegroundColor White
Write-Host "  工作线程: $Workers" -ForegroundColor White
Write-Host "  刷新间隔: $RefreshMinutes分钟" -ForegroundColor White
Write-Host
Write-Host "执行命令: $ETLCmd" -ForegroundColor Cyan
Write-Host "=============================================" -ForegroundColor Green
Write-Host

# 记录开始时间到日志文件
$StartTime = Get-Date
@"
=============================================
Nginx日志ETL自动处理系统 (PowerShell版)
开始时间: $($StartTime.ToString('yyyy-MM-dd HH:mm:ss'))
工作目录: $PWD
执行命令: $ETLCmd
参数: Duration=$Duration, BatchSize=$BatchSize, Workers=$Workers, RefreshMinutes=$RefreshMinutes
=============================================

"@ | Out-File -FilePath $LogFile -Encoding UTF8

Write-Host "🚀 开始ETL处理..." -ForegroundColor Green
Write-Host "📺 实时输出到控制台" -ForegroundColor Blue
Write-Host "📝 同时保存日志到: $LogFile" -ForegroundColor Blue
Write-Host
Write-Host "=============================================" -ForegroundColor Green
Write-Host

# 执行ETL命令并实现双重输出
try {
    # 使用Start-Process捕获输出并同时显示
    $ProcessInfo = New-Object System.Diagnostics.ProcessStartInfo
    $ProcessInfo.FileName = "python"
    $ProcessInfo.Arguments = "$ControllerFile --auto-monitor --monitor-duration $Duration --batch-size $BatchSize --workers $Workers --refresh-minutes $RefreshMinutes"
    $ProcessInfo.RedirectStandardOutput = $true
    $ProcessInfo.RedirectStandardError = $true
    $ProcessInfo.UseShellExecute = $false
    $ProcessInfo.CreateNoWindow = $false
    $ProcessInfo.WorkingDirectory = $PWD

    $Process = New-Object System.Diagnostics.Process
    $Process.StartInfo = $ProcessInfo

    # 事件处理器用于实时显示输出
    $OutputDataReceived = {
        param($sender, $e)
        if ($e.Data -ne $null) {
            Write-Host $e.Data
            Add-Content -Path $LogFile -Value $e.Data -Encoding UTF8
        }
    }

    $ErrorDataReceived = {
        param($sender, $e)
        if ($e.Data -ne $null) {
            Write-Host $e.Data -ForegroundColor Red
            Add-Content -Path $LogFile -Value "ERROR: $($e.Data)" -Encoding UTF8
        }
    }

    Register-ObjectEvent -InputObject $Process -EventName OutputDataReceived -Action $OutputDataReceived | Out-Null
    Register-ObjectEvent -InputObject $Process -EventName ErrorDataReceived -Action $ErrorDataReceived | Out-Null

    $Process.Start() | Out-Null
    $Process.BeginOutputReadLine()
    $Process.BeginErrorReadLine()
    $Process.WaitForExit()

    $ExitCode = $Process.ExitCode

    # 清理事件
    Get-EventSubscriber | Unregister-Event

} catch {
    Write-Host "❌ 执行过程中出现异常: $($_.Exception.Message)" -ForegroundColor Red
    $ExitCode = -1
}

$EndTime = Get-Date
$Duration = $EndTime - $StartTime

Write-Host
Write-Host "=============================================" -ForegroundColor Green
Write-Host "ETL处理完成时间: $($EndTime.ToString('yyyy-MM-dd HH:mm:ss'))" -ForegroundColor Green
Write-Host "总耗时: $([math]::Round($Duration.TotalMinutes, 1))分钟" -ForegroundColor Green
Write-Host "退出代码: $ExitCode" -ForegroundColor $(if ($ExitCode -eq 0) { "Green" } else { "Red" })

# 记录结束时间到日志文件
@"

=============================================
ETL处理完成时间: $($EndTime.ToString('yyyy-MM-dd HH:mm:ss'))
总耗时: $([math]::Round($Duration.TotalMinutes, 1))分钟
退出代码: $ExitCode
=============================================
"@ | Add-Content -Path $LogFile -Encoding UTF8

# 检查执行结果
if ($ExitCode -eq 0) {
    Write-Host "✅ ETL处理成功完成！" -ForegroundColor Green
    "✅ ETL处理成功完成！" | Add-Content -Path $LogFile -Encoding UTF8
} else {
    Write-Host "❌ ETL处理出现错误，退出代码: $ExitCode" -ForegroundColor Red
    Write-Host "请检查上方的错误信息和日志文件" -ForegroundColor Yellow
    "❌ ETL处理出现错误，退出代码: $ExitCode" | Add-Content -Path $LogFile -Encoding UTF8
}

Write-Host
Write-Host "📝 完整日志已保存到: $LogFile" -ForegroundColor Yellow
Write-Host "=============================================" -ForegroundColor Green
Write-Host

# 显示日志文件最后几行
Write-Host "📋 日志文件最后内容预览:" -ForegroundColor Blue
Get-Content $LogFile | Select-Object -Last 10 | ForEach-Object { Write-Host $_ -ForegroundColor Gray }

Write-Host
Write-Host "✅ 脚本执行完成" -ForegroundColor Green
Read-Host "按任意键退出"