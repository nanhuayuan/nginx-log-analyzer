# 检查管理员权限
if (-NOT ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator")) {
    Write-Host "需要管理员权限来创建定时任务" -ForegroundColor Red
    Read-Host "按Enter键退出"
    exit 1
}

# 获取脚本所在目录
$scriptPath = Split-Path -Parent $MyInvocation.MyCommand.Path
$batchScript = Join-Path $scriptPath "run_nginx_zip_log_processor.bat"

# 检查文件是否存在
if (-not (Test-Path $batchScript)) {
    Write-Host "未找到批处理脚本: $batchScript" -ForegroundColor Red
    Read-Host "按Enter键退出"
    exit 1
}

Write-Host "脚本路径: $scriptPath" -ForegroundColor Cyan
Write-Host "批处理文件: $batchScript" -ForegroundColor Cyan

# 创建定时任务
$taskName = "NginxZipLogProcessor"
$description = "自动处理nginx日志zip文件"

# 删除已存在的任务（如果有）
try {
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue
    Write-Host "已删除现有任务" -ForegroundColor Yellow
} catch {}

# 创建触发器 - 每天凌晨2点执行
$trigger = New-ScheduledTaskTrigger -Daily -At "01:00AM"

# 创建操作 - 运行批处理文件
$action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$batchScript`"" -WorkingDirectory $scriptPath

# 创建主体 - 使用当前用户账户运行（确保conda环境可用）
$currentUser = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$principal = New-ScheduledTaskPrincipal -UserId $currentUser -RunLevel Highest

# 创建设置
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Hours 2)

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
Write-Host "任务名称: $taskName" -ForegroundColor Cyan
Write-Host "执行时间: 每天凌晨2:00" -ForegroundColor Cyan
Write-Host "运行用户: $currentUser" -ForegroundColor Cyan
Write-Host "可通过'任务计划程序'查看和管理此任务" -ForegroundColor Cyan
Write-Host "===========================================" -ForegroundColor Green

# 显示如何查看任务状态
Write-Host ""
Write-Host "查看任务状态的方法:" -ForegroundColor Yellow
Write-Host "1. 按Win+R，输入 taskschd.msc 打开任务计划程序" -ForegroundColor White
Write-Host "2. 在任务计划程序库中找到 '$taskName'" -ForegroundColor White
Write-Host "3. 右键点击任务，选择'运行'可立即测试" -ForegroundColor White
Write-Host "4. 查看'历史记录'标签页了解执行结果" -ForegroundColor White

# 测试运行
Write-Host ""
$testRun = Read-Host "是否立即测试运行? (y/N)"
if ($testRun -eq 'y' -or $testRun -eq 'Y') {
    Write-Host "开始测试运行..." -ForegroundColor Yellow
    try {
        Start-ScheduledTask -TaskName $taskName
        Write-Host "任务已启动，请检查日志输出和任务计划程序中的执行历史" -ForegroundColor Green
    } catch {
        Write-Host "启动任务失败: $_" -ForegroundColor Red
    }
}

Read-Host "按Enter键退出"