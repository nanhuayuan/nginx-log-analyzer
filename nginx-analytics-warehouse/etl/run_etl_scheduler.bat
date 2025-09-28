@echo off
chcp 65001
setlocal EnableDelayedExpansion

echo =============================================
echo Nginx日志ETL自动处理系统
echo 启动时间: %date% %time%
echo =============================================
echo.

REM 切换到脚本所在目录
cd /d "%~dp0"
echo 当前工作目录: %CD%
echo.

REM 激活conda环境
echo 🔄 激活conda环境...
call conda activate py39
if errorlevel 1 (
    echo ❌ 错误: 无法激活conda环境py39
    echo 请检查conda是否已安装以及py39环境是否存在
    echo.
    echo 常见解决方案:
    echo 1. 检查conda是否在PATH中: conda --version
    echo 2. 检查环境是否存在: conda env list
    echo 3. 创建环境: conda create -n py39 python=3.9
    pause
    exit /b 1
)
echo ✅ conda环境激活成功
echo.

REM 检查Python版本
echo 🐍 检查Python版本...
python --version
if errorlevel 1 (
    echo ❌ 错误: Python不可用
    pause
    exit /b 1
)
echo.

REM 检查ETL控制器文件
echo 📂 检查ETL控制器文件...
if not exist "controllers\integrated_ultra_etl_controller-v1.py" (
    echo ❌ 错误: 未找到ETL控制器文件
    echo 期望文件: controllers\integrated_ultra_etl_controller-v1.py
    echo 当前目录: %CD%
    echo.
    echo 请确保脚本在正确的ETL目录下运行
    pause
    exit /b 1
)
echo ✅ ETL控制器文件存在
echo.

REM 检查关键依赖
echo 🔍 检查关键依赖...
python -c "import sys; print('Python路径:', sys.executable)" 2>nul
python -c "import clickhouse_connect; print('✅ clickhouse_connect 可用')" 2>nul
if errorlevel 1 (
    echo ❌ 缺少关键依赖: clickhouse_connect
    echo.
    echo 🔧 正在自动安装依赖...
    pip install clickhouse_connect
    if errorlevel 1 (
        echo ❌ 自动安装失败
        echo.
        echo 手动安装方案:
        echo 1. pip install clickhouse_connect
        echo 2. 或者运行: check_and_install_dependencies.bat
        pause
        exit /b 1
    )
    echo ✅ clickhouse_connect 安装成功
    echo.
)

REM 检查其他依赖
python -c "import pandas; print('✅ pandas 可用')" 2>nul
if errorlevel 1 (
    echo 📦 安装pandas...
    pip install pandas
)

python -c "import pathlib; print('✅ pathlib 可用')" 2>nul

echo ✅ 所有依赖检查完成
echo.

REM 设置日志文件目录
set LOG_DIR=%~dp0logs
if not exist "%LOG_DIR%" (
    mkdir "%LOG_DIR%"
    echo ✅ 创建日志目录: %LOG_DIR%
)

REM 生成日志文件名（使用更简洁的格式）
for /f "tokens=1-3 delims=/" %%a in ("%date%") do set date_str=%%c%%a%%b
for /f "tokens=1-2 delims=:" %%a in ("%time%") do set time_str=%%a%%b
set LOG_FILE=%LOG_DIR%\etl_auto_%date_str%_%time_str%.log

echo 📝 日志文件: %LOG_FILE%
echo.

REM 构建ETL命令
set ETL_CMD=python controllers\integrated_ultra_etl_controller-v1.py --auto-monitor --monitor-duration 7200 --batch-size 3000 --workers 6 --refresh-minutes 2

echo 🚀 ETL处理配置:
echo   控制器: integrated_ultra_etl_controller-v1.py
echo   模式: 自动监控 (2小时)
echo   批大小: 3000
echo   工作线程: 6
echo   刷新间隔: 2分钟
echo.
echo 执行命令: %ETL_CMD%
echo =============================================
echo.

REM 记录开始时间到日志文件
echo =============================================>> "%LOG_FILE%" 2>&1
echo Nginx日志ETL自动处理系统>> "%LOG_FILE%" 2>&1
echo 开始时间: %date% %time%>> "%LOG_FILE%" 2>&1
echo 工作目录: %CD%>> "%LOG_FILE%" 2>&1
echo 执行命令: %ETL_CMD%>> "%LOG_FILE%" 2>&1
echo =============================================>> "%LOG_FILE%" 2>&1
echo.>> "%LOG_FILE%" 2>&1

echo 🚀 开始ETL处理...
echo 📺 实时输出到控制台
echo 📝 同时保存日志到: %LOG_FILE%
echo.
echo =============================================
echo.

REM 使用PowerShell实现双重输出（控制台+文件）
powershell -Command "& {$ErrorActionPreference='Continue'; try { Invoke-Expression '%ETL_CMD%' 2>&1 | Tee-Object -FilePath '%LOG_FILE%' -Append } catch { Write-Error $_.Exception.Message }}"
set ETL_EXIT_CODE=%errorlevel%

echo.
echo =============================================
echo ETL处理完成时间: %date% %time%
echo 退出代码: %ETL_EXIT_CODE%

REM 记录结束时间到日志文件
echo.>> "%LOG_FILE%" 2>&1
echo =============================================>> "%LOG_FILE%" 2>&1
echo ETL处理完成时间: %date% %time%>> "%LOG_FILE%" 2>&1
echo 退出代码: %ETL_EXIT_CODE%>> "%LOG_FILE%" 2>&1
echo =============================================>> "%LOG_FILE%" 2>&1

REM 检查执行结果
if %ETL_EXIT_CODE% equ 0 (
    echo ✅ ETL处理成功完成！
    echo ✅ ETL处理成功完成！>> "%LOG_FILE%" 2>&1
) else (
    echo ❌ ETL处理出现错误，退出代码: %ETL_EXIT_CODE%
    echo ❌ ETL处理出现错误，退出代码: %ETL_EXIT_CODE%>> "%LOG_FILE%" 2>&1
    echo 请检查上方的错误信息和日志文件
)

echo.
echo 📝 完整日志已保存到: %LOG_FILE%
echo =============================================
echo.

REM 显示最后几行日志内容
echo 📋 日志文件最后内容预览:
powershell -Command "Get-Content '%LOG_FILE%' | Select-Object -Last 10"

echo.
echo ✅ 脚本执行完成
echo Press any key to exit...
pause >nul