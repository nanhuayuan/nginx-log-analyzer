# ETL自动调度任务设置脚本
# 每天凌晨1:30启动nginx日志ETL处理

# 检查管理员权限
if (-NOT ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator")) {
    Write-Host "需要管理员权限来创建定时任务" -ForegroundColor Red
    Write-Host "请右键点击PowerShell并选择'以管理员身份运行'" -ForegroundColor Yellow
    Read-Host "按Enter键退出"
    exit 1
}

# 获取脚本所在目录
$scriptPath = Split-Path -Parent $MyInvocation.MyCommand.Path
$batchScript = Join-Path $scriptPath "run_auto_etl_portable.bat"  # 使用便携版脚本

# 检查文件是否存在
# 检查两种脚本文件
$portableBatch = Join-Path $scriptPath "run_auto_etl_portable.bat"
$originalBatch = Join-Path $scriptPath "run_auto_etl.bat"

if (Test-Path $portableBatch) {
    $batchScript = $portableBatch
    Write-Host "使用便携版脚本: run_auto_etl_portable.bat" -ForegroundColor Green
} elseif (Test-Path $originalBatch) {
    $batchScript = $originalBatch
    Write-Host "使用原版脚本: run_auto_etl.bat" -ForegroundColor Yellow
} else {
    Write-Host "未找到批处理脚本文件" -ForegroundColor Red
    Write-Host "请确保以下文件之一存在:" -ForegroundColor Yellow
    Write-Host "  - run_auto_etl_portable.bat (推荐)" -ForegroundColor White
    Write-Host "  - run_auto_etl.bat" -ForegroundColor White
    Read-Host "按Enter键退出"
    exit 1
}

Write-Host "ETL调度任务配置" -ForegroundColor Green
Write-Host "脚本路径: $scriptPath" -ForegroundColor Cyan
Write-Host "批处理文件: $batchScript" -ForegroundColor Cyan

# 创建定时任务
$taskName = "NginxETLAutoProcessor"
$description = "自动处理nginx日志ETL - 每天凌晨1:30执行"

# 删除已存在的任务（如果有）
try {
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue
    Write-Host "已删除现有任务" -ForegroundColor Yellow
} catch {}

# 创建触发器 - 每天凌晨1:30执行
$trigger = New-ScheduledTaskTrigger -Daily -At "01:30AM"

# 创建操作 - 运行批处理文件
$action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$batchScript`"" -WorkingDirectory $scriptPath

# 创建主体 - 使用当前用户账户运行
$currentUser = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$principal = New-ScheduledTaskPrincipal -UserId $currentUser -RunLevel Highest

# 创建设置 - 任务最长运行3小时
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Hours 3) -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 5)

# 注册任务
try {
    Register-ScheduledTask -TaskName $taskName -Description $description -Trigger $trigger -Action $action -Principal $principal -Settings $settings
    Write-Host "定时任务创建成功!" -ForegroundColor Green
} catch {
    Write-Host "创建定时任务失败: $_" -ForegroundColor Red
    Read-Host "按Enter键退出"
    exit 1
}

Write-Host "===========================================" -ForegroundColor Green
Write-Host "ETL定时任务配置完成" -ForegroundColor Green
Write-Host "===========================================" -ForegroundColor Green
Write-Host "任务名称: $taskName" -ForegroundColor Cyan
Write-Host "执行时间: 每天凌晨1:30" -ForegroundColor Cyan
Write-Host "运行用户: $currentUser" -ForegroundColor Cyan
Write-Host "执行时长: 2小时自动监控 + 最多3小时超时保护" -ForegroundColor Cyan
Write-Host "日志位置: $scriptPath\logs\" -ForegroundColor Cyan
Write-Host "===========================================" -ForegroundColor Green

# 显示管理说明
Write-Host ""
Write-Host "任务管理说明:" -ForegroundColor Yellow
Write-Host "1. 查看任务: 按Win+R，输入 taskschd.msc 打开任务计划程序" -ForegroundColor White
Write-Host "2. 在任务计划程序库中找到 '$taskName'" -ForegroundColor White
Write-Host "3. 右键点击任务可以:" -ForegroundColor White
Write-Host "   - 立即运行（测试用）" -ForegroundColor White
Write-Host "   - 查看运行历史" -ForegroundColor White
Write-Host "   - 修改设置" -ForegroundColor White
Write-Host "   - 启用/禁用任务" -ForegroundColor White
Write-Host ""
Write-Host "运行逻辑说明:" -ForegroundColor Yellow
Write-Host "- 每天凌晨1:30自动启动" -ForegroundColor White
Write-Host "- 首先处理所有未处理的日志文件" -ForegroundColor White
Write-Host "- 然后进入2小时自动监控模式" -ForegroundColor White
Write-Host "- 每3分钟检查一次新文件并自动处理" -ForegroundColor White
Write-Host "- 日志文件保存在 logs 目录下" -ForegroundColor White

# 测试运行选项
Write-Host ""
$testRun = Read-Host "是否立即测试运行ETL任务? (y/N)"
if ($testRun -eq 'y' -or $testRun -eq 'Y') {
    Write-Host "开始测试运行ETL任务..." -ForegroundColor Yellow
    try {
        Start-ScheduledTask -TaskName $taskName
        Write-Host "ETL任务已启动!" -ForegroundColor Green
        Write-Host "请查看任务计划程序中的执行历史和日志文件" -ForegroundColor Cyan

        # 等待几秒钟显示状态
        Start-Sleep -Seconds 3
        $task = Get-ScheduledTask -TaskName $taskName
        Write-Host "当前任务状态: $($task.State)" -ForegroundColor Cyan

    } catch {
        Write-Host "启动任务失败: $_" -ForegroundColor Red
    }
}

Write-Host ""
Write-Host "配置完成! 系统将在每天凌晨1:30自动处理nginx日志" -ForegroundColor Green
Read-Host "按Enter键退出"