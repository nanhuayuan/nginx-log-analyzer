@echo off
chcp 65001 > nul
echo 🚀 启动Nginx日志分析平台...

REM 检查Docker是否安装
docker --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Docker 未安装，请先安装Docker Desktop
    pause
    exit /b 1
)

REM 创建必要目录
echo 📁 创建数据目录...
if not exist volumes mkdir volumes
if not exist volumes\clickhouse mkdir volumes\clickhouse
if not exist volumes\clickhouse\data mkdir volumes\clickhouse\data
if not exist volumes\clickhouse\logs mkdir volumes\clickhouse\logs
if not exist volumes\grafana mkdir volumes\grafana
if not exist volumes\postgres mkdir volumes\postgres
if not exist volumes\redis mkdir volumes\redis
if not exist volumes\superset mkdir volumes\superset
if not exist data mkdir data
if not exist logs mkdir logs

echo 🐳 启动服务...

REM 启动核心服务
docker-compose up -d clickhouse grafana superset-redis superset-postgres superset

echo ⏳ 等待服务启动...
timeout /t 30 /nobreak >nul

echo 🔍 检查服务状态:

REM 检查ClickHouse
curl -s http://localhost:8123/ping >nul 2>&1
if %errorlevel% equ 0 (
    echo   ✅ ClickHouse: http://localhost:8123
) else (
    echo   ❌ ClickHouse: 启动失败
)

REM 检查Grafana
curl -s http://localhost:3000 >nul 2>&1
if %errorlevel% equ 0 (
    echo   ✅ Grafana: http://localhost:3000 ^(admin/admin123^)
) else (
    echo   ⏳ Grafana: 仍在启动中...
)

REM 检查Superset
curl -s http://localhost:8088 >nul 2>&1
if %errorlevel% equ 0 (
    echo   ✅ Superset: http://localhost:8088 ^(admin/admin123^)
) else (
    echo   ⏳ Superset: 仍在启动中...
)

echo.
echo 🎉 平台启动完成!
echo.
echo 📊 访问地址:
echo   • ClickHouse: http://localhost:8123
echo   • Grafana: http://localhost:3000 ^(admin/admin123^)
echo   • Superset: http://localhost:8088 ^(admin/admin123^)
echo.
echo 📝 下一步:
echo   1. 等待所有服务完全启动 ^(约2-3分钟^)
echo   2. 访问Grafana和Superset配置ClickHouse数据源
echo   3. 运行数据处理脚本导入nginx日志
echo.
echo 🔧 管理命令:
echo   • 查看日志: docker-compose logs -f [service_name]
echo   • 停止服务: docker-compose down
echo   • 重启服务: docker-compose restart [service_name]
echo.
pause