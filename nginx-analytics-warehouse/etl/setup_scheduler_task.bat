@echo off
chcp 65001
echo =============================================
echo ETL定时任务设置工具
echo =============================================
echo.

REM 检查管理员权限
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ 错误: 需要管理员权限来设置计划任务
    echo.
    echo 解决方案:
    echo 1. 右键点击此脚本
    echo 2. 选择"以管理员身份运行"
    echo.
    pause
    exit /b 1
)

echo ✅ 管理员权限确认
echo.

REM 获取当前脚本目录
set SCRIPT_DIR=%~dp0
set ETL_SCRIPT=%SCRIPT_DIR%run_etl_scheduler.bat

echo 📂 脚本位置: %ETL_SCRIPT%

REM 检查ETL脚本是否存在
if not exist "%ETL_SCRIPT%" (
    echo ❌ 错误: 找不到ETL执行脚本
    echo 期望文件: %ETL_SCRIPT%
    pause
    exit /b 1
)

echo ✅ ETL脚本文件存在
echo.

REM 任务配置
set TASK_NAME=NginxETLProcessor
set TASK_TIME=01:30
set TASK_DESCRIPTION=Nginx日志ETL自动处理任务，每天凌晨1:30执行

echo 📋 任务配置:
echo   任务名称: %TASK_NAME%
echo   执行时间: 每天 %TASK_TIME%
echo   执行脚本: %ETL_SCRIPT%
echo   任务描述: %TASK_DESCRIPTION%
echo.

REM 询问用户确认
set /p CONFIRM="确认创建定时任务? (Y/n): "
if /i "%CONFIRM%" neq "Y" if /i "%CONFIRM%" neq "" (
    echo 任务创建已取消
    pause
    exit /b 0
)

echo.
echo 🔧 创建计划任务...

REM 删除已存在的同名任务
schtasks /query /tn "%TASK_NAME%" >nul 2>&1
if %errorlevel% equ 0 (
    echo ⚠️  发现已存在的同名任务，正在删除...
    schtasks /delete /tn "%TASK_NAME%" /f >nul 2>&1
    if %errorlevel% equ 0 (
        echo ✅ 旧任务删除成功
    ) else (
        echo ❌ 旧任务删除失败
    )
)

REM 创建新的计划任务
schtasks /create /tn "%TASK_NAME%" /tr "\"%ETL_SCRIPT%\"" /sc daily /st %TASK_TIME% /rl highest /f /ru "SYSTEM" /rp
if %errorlevel% equ 0 (
    echo ✅ 计划任务创建成功！
    echo.

    REM 设置任务描述
    schtasks /change /tn "%TASK_NAME%" /tr "\"%ETL_SCRIPT%\"" /ru "SYSTEM" >nul 2>&1

    echo 📋 任务详情:
    schtasks /query /tn "%TASK_NAME%" /fo LIST /v | findstr /C:"任务名" /C:"状态" /C:"上次运行时间" /C:"下次运行时间" /C:"要运行的任务"

    echo.
    echo 🎯 任务管理命令:
    echo   查看任务: schtasks /query /tn "%TASK_NAME%"
    echo   手动运行: schtasks /run /tn "%TASK_NAME%"
    echo   禁用任务: schtasks /change /tn "%TASK_NAME%" /disable
    echo   启用任务: schtasks /change /tn "%TASK_NAME%" /enable
    echo   删除任务: schtasks /delete /tn "%TASK_NAME%" /f
    echo.

    echo ✅ 定时任务设置完成！
    echo 📅 下次执行时间: 明天 %TASK_TIME%
    echo 📝 执行日志将保存到: %SCRIPT_DIR%logs\

) else (
    echo ❌ 计划任务创建失败
    echo.
    echo 可能的原因:
    echo 1. 权限不足（请确保以管理员身份运行）
    echo 2. 脚本路径包含特殊字符
    echo 3. 系统服务异常
    echo.
    echo 手动创建方案:
    echo 1. 打开"任务计划程序"
    echo 2. 创建基本任务
    echo 3. 设置每天%TASK_TIME%执行
    echo 4. 操作设置为启动程序: "%ETL_SCRIPT%"
)

echo.
echo =============================================
pause