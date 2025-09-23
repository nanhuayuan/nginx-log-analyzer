@echo off
chcp 65001
echo ========================================
echo Nginx日志ETL自动处理系统
echo 启动时间: %date% %time%
echo ========================================

REM 切换到脚本所在目录
cd /d "%~dp0"

REM 设置日志文件
set LOG_DIR=%~dp0logs
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
set LOG_FILE=%LOG_DIR%\etl_auto_%date:~0,4%%date:~5,2%%date:~8,2%_%time:~0,2%%time:~3,2%.log

echo 日志文件: %LOG_FILE%
echo ========================================

REM 启动自动监控模式，运行2小时（7200秒）
call conda activate py39
python controllers\integrated_ultra_etl_controller.py --auto-monitor --monitor-duration 7200 --batch-size 3000 --workers 6 --refresh-minutes 2

echo ========================================
echo ETL处理完成时间: %date% %time%
echo 详细日志请查看: %LOG_FILE%
echo ========================================

echo.
echo Press any key to exit...
pause >nul