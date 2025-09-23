@echo off
chcp 65001
echo ========================================
echo 测试ETL自动处理功能
echo ========================================

REM 切换到脚本所在目录
cd /d "%~dp0"
echo 当前工作目录: %CD%

REM 激活conda环境
echo.
echo 激活conda环境...
call conda activate py39
if errorlevel 1 (
    echo ❌ 错误: 无法激活conda环境py39
    echo 请检查conda是否安装以及py39环境是否存在
    pause
    exit /b 1
)
echo ✅ Conda环境激活成功

echo.
echo 🧪 开始ETL功能测试...
echo ========================================

echo.
echo 📋 测试1: 基本处理功能（处理所有未处理的日志，限制100条）
echo ----------------------------------------
python controllers\integrated_ultra_etl_controller.py --all --test --limit 100
set TEST1_EXIT=%errorlevel%

echo.
echo ========================================
echo 📋 测试2: 自动监控功能（运行60秒）
echo ----------------------------------------
python controllers\integrated_ultra_etl_controller.py --auto-monitor --monitor-duration 60 --test
set TEST2_EXIT=%errorlevel%

echo.
echo ========================================
echo 🎯 测试结果汇总
echo ========================================
echo 测试1 (基本处理):
if %TEST1_EXIT% equ 0 (
    echo ✅ 成功
) else (
    echo ❌ 失败 ^(退出代码: %TEST1_EXIT%^)
)

echo 测试2 (自动监控):
if %TEST2_EXIT% equ 0 (
    echo ✅ 成功
) else (
    echo ❌ 失败 ^(退出代码: %TEST2_EXIT%^)
)

echo.
echo 📋 整体测试结果:
if %TEST1_EXIT% equ 0 if %TEST2_EXIT% equ 0 (
    echo ✅ 所有测试通过！系统可以正常使用
) else (
    echo ❌ 部分测试失败，请检查上方的错误信息
)

echo ========================================
echo.
echo 按任意键退出...
pause >nul