@echo off
chcp 65001
echo ========================================
echo Nginx日志ETL自动处理系统 (便携版)
echo 启动时间: %date% %time%
echo ========================================

REM 切换到脚本所在目录
cd /d "%~dp0"
echo 当前工作目录: %CD%

REM 检查Python环境
call conda activate py39
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ 错误: 未找到Python环境
    echo 请确保Python已安装并添加到PATH环境变量
    pause
    exit /b 1
)

REM 检查ETL控制器文件
if not exist "controllers\integrated_ultra_etl_controller.py" (
    echo ❌ 错误: 未找到ETL控制器文件
    echo 请确保脚本在正确的ETL目录下运行
    echo 当前目录: %CD%
    pause
    exit /b 1
)

REM 自动检测nginx_logs目录
set "NGINX_LOGS_DIR="
if exist "..\nginx_logs" (
    set "NGINX_LOGS_DIR=..\nginx_logs"
    echo ✅ 找到nginx_logs目录: %CD%\..\nginx_logs
) else (
    echo ⚠️  未找到nginx_logs目录，将使用默认配置
)

REM 设置日志文件目录
set LOG_DIR=%~dp0logs
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
set LOG_FILE=%LOG_DIR%\etl_auto_%date:~0,4%%date:~5,2%%date:~8,2%_%time:~0,2%%time:~3,2%.log

echo 📝 日志文件: %LOG_FILE%
echo ========================================

REM 构建命令参数
set ETL_CMD=python controllers\integrated_ultra_etl_controller.py --auto-monitor --monitor-duration 7200 --batch-size 3000 --workers 6 --refresh-minutes 2

REM 如果指定了nginx_logs目录，添加到命令中
if defined NGINX_LOGS_DIR (
    REM 这里可以通过环境变量传递，或者让程序自动检测
    echo 📁 使用nginx_logs目录: %NGINX_LOGS_DIR%
)

echo 🚀 启动ETL处理...
echo 执行命令: %ETL_CMD%
echo ========================================

echo.
echo 🚀 开始ETL处理，实时输出进度...
echo 📝 同时将日志保存到: %LOG_FILE%
echo ========================================
echo.

REM 创建一个临时脚本来实现双重输出（控制台+文件）
set TEMP_SCRIPT=%TEMP%\etl_dual_output.bat
echo @echo off > "%TEMP_SCRIPT%"
echo %ETL_CMD% 2^>^&1 ^| tee "%LOG_FILE%" >> "%TEMP_SCRIPT%"

REM 检查是否有tee命令，如果没有则使用PowerShell实现
where tee >nul 2>&1
if errorlevel 1 (
    echo 使用PowerShell实现双重输出...
    REM 使用PowerShell实现tee功能
    powershell -Command "& {%ETL_CMD% 2>&1 | Tee-Object -FilePath '%LOG_FILE%'}"
    set ETL_EXIT_CODE=%errorlevel%
) else (
    echo 使用tee命令实现双重输出...
    REM 如果系统有tee命令，直接使用
    call "%TEMP_SCRIPT%"
    set ETL_EXIT_CODE=%errorlevel%
)

REM 清理临时文件
if exist "%TEMP_SCRIPT%" del "%TEMP_SCRIPT%"

echo.
echo ========================================
echo ETL处理完成时间: %date% %time%
echo 退出代码: %ETL_EXIT_CODE%

REM 检查执行结果
if %ETL_EXIT_CODE% equ 0 (
    echo ✅ ETL处理成功完成！
) else (
    echo ❌ ETL处理出现错误，退出代码: %ETL_EXIT_CODE%
    echo 请检查上方的错误信息和日志文件
)

echo 📝 详细日志已保存到: %LOG_FILE%
echo ========================================

echo.
echo 按任意键退出...
pause >nul