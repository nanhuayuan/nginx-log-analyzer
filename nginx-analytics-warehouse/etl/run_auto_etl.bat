@echo off
chcp 65001
echo ========================================
echo Nginx日志ETL自动处理系统
echo 启动时间: %date% %time%
echo ========================================

REM 切换到脚本所在目录
cd /d "%~dp0"

REM 激活conda环境
echo 激活conda环境...
call conda activate py39
if errorlevel 1 (
    echo ERROR: Failed to activate conda environment py39
    echo Please check if conda is installed and py39 environment exists
    pause
    exit /b 1
)

echo Conda environment activated successfully
echo.

REM 检查Python脚本是否存在
if not exist "controllers\integrated_ultra_etl_controller.py" (
    echo ERROR: integrated_ultra_etl_controller.py not found
    echo Current directory: %cd%
    pause
    exit /b 1
)

echo Starting ETL processing...
echo Script path: %cd%\controllers\integrated_ultra_etl_controller.py
echo.

REM 设置日志文件
set LOG_DIR=%~dp0logs
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
set LOG_FILE=%LOG_DIR%\etl_auto_%date:~0,4%%date:~5,2%%date:~8,2%_%time:~0,2%%time:~3,2%.log

echo 日志文件: %LOG_FILE%
echo ========================================

REM 记录开始信息到日志文件
echo ======================================== > "%LOG_FILE%"
echo Nginx日志ETL自动处理系统 >> "%LOG_FILE%"
echo 启动时间: %date% %time% >> "%LOG_FILE%"
echo 工作目录: %cd% >> "%LOG_FILE%"
echo ======================================== >> "%LOG_FILE%"
echo. >> "%LOG_FILE%"

REM 启动自动监控模式，运行2小时（7200秒），同时输出到控制台和文件
python controllers\integrated_ultra_etl_controller.py --auto-monitor --monitor-duration 7200 --batch-size 3000 --workers 6 --refresh-minutes 2 2>&1 | powershell -Command "$input | ForEach-Object { Write-Host $_; Add-Content -Path '%LOG_FILE%' -Value $_ -Encoding UTF8 }"
set ETL_EXIT_CODE=%errorlevel%

echo.
echo ========================================
echo ETL处理完成时间: %date% %time%
echo 退出代码: %ETL_EXIT_CODE%

REM 记录结束信息到日志文件
echo. >> "%LOG_FILE%"
echo ======================================== >> "%LOG_FILE%"
echo ETL处理完成时间: %date% %time% >> "%LOG_FILE%"
echo 退出代码: %ETL_EXIT_CODE% >> "%LOG_FILE%"

REM 检查执行结果
if %ETL_EXIT_CODE% equ 0 (
    echo ✅ ETL处理成功完成！
    echo ✅ ETL处理成功完成！ >> "%LOG_FILE%"
) else (
    echo ❌ ETL处理出现错误，退出代码: %ETL_EXIT_CODE%
    echo ❌ ETL处理出现错误，退出代码: %ETL_EXIT_CODE% >> "%LOG_FILE%"
)

echo ======================================== >> "%LOG_FILE%"

echo.
echo 📝 详细日志已保存到: %LOG_FILE%
echo ========================================

echo.
echo Press any key to exit...
pause >nul