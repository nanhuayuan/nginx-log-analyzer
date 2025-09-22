@echo off
chcp 65001
echo ========================================
echo 测试ETL自动处理功能
echo ========================================

REM 切换到脚本所在目录
cd /d "%~dp0"

echo 1. 测试基本处理功能（处理所有未处理的日志）
python controllers\integrated_ultra_etl_controller.py --all --test --limit 100

echo.
echo ========================================
echo 2. 测试自动监控功能（运行60秒）
echo ========================================
python controllers\integrated_ultra_etl_controller.py --auto-monitor --monitor-duration 60 --test

echo.
echo ========================================
echo 测试完成
echo ========================================
pause