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

REM 执行ETL处理并记录日志
%ETL_CMD% > "%LOG_FILE%" 2>&1

REM 检查执行结果
if errorlevel 1 (
    echo ❌ ETL处理过程中出现错误
    echo 请查看日志文件: %LOG_FILE%
) else (
    echo ✅ ETL处理完成
)

echo ========================================
echo ETL处理完成时间: %date% %time%
echo 详细日志位置: %LOG_FILE%
echo ========================================

REM 显示最后几行日志（如果存在）
if exist "%LOG_FILE%" (
    echo.
    echo 📋 最后几行日志:
    echo ----------------------------------------
    more +0 "%LOG_FILE%" | findstr /E /C:"✅" /C:"❌" /C:"处理完成" /C:"ERROR" /C:"成功"
    echo ----------------------------------------
)

REM 保持窗口打开以查看结果
timeout /t 10