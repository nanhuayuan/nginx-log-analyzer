@echo off
setlocal EnableDelayedExpansion
chcp 65001
echo ========================================
echo Nginx日志ETL自动处理系统
echo 启动时间: %date% %time%
echo ========================================

REM 切换到脚本所在目录
cd /d "%~dp0"

REM 激活conda环境并检查依赖
call conda activate py39
if errorlevel 1 (
    echo ❌ 错误: 无法激活conda环境py39
    pause
    exit /b 1
)

REM 快速检查关键依赖
echo ⚙️ 检查关键依赖...
python -c "import clickhouse_connect" 2>nul
if errorlevel 1 (
    echo ❌ 错误: 缺少关键依赖 clickhouse_connect
    echo 正在自动安装...
    pip install clickhouse_connect
    if errorlevel 1 (
        echo ❌ 安装失败，请手动运行: pip install clickhouse_connect
        pause
        exit /b 1
    )
    echo ✅ 依赖安装成功
)

REM 设置日志文件
set LOG_DIR=%~dp0logs
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
set LOG_FILE=%LOG_DIR%\etl_auto_%date:~0,4%%date:~5,2%%date:~8,2%_%time:~0,2%%time:~3,2%.log

echo 日志文件: %LOG_FILE%
echo ========================================

echo.
echo 🚀 开始ETL处理，实时输出进度...
echo 📝 同时将日志保存到: %LOG_FILE%
echo ========================================
echo.

REM 启动自动监控模式，运行2小时（7200秒）
REM conda环境已在前面激活

REM 使用PowerShell实现双重输出（控制台+文件）
powershell -Command "python controllers\integrated_ultra_etl_controller.py --auto-monitor --monitor-duration 7200 --batch-size 3000 --workers 6 --refresh-minutes 2 2>&1 | Tee-Object -FilePath '%LOG_FILE%'"
set ETL_EXIT_CODE=%errorlevel%

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