@echo off
setlocal enabledelayedexpansion

:: ClickHouse Docker Compose 管理脚本 (Windows版本)
:: 使用方法: clickhouse-manager.bat [command]

set SCRIPT_DIR=%~dp0
set COMPOSE_FILE=%SCRIPT_DIR%docker-compose.yml

:: 检查Docker和Docker Compose
call :check_dependencies
if errorlevel 1 exit /b 1

:: 解析命令参数
set COMMAND=%1
if "%COMMAND%"=="" set COMMAND=start

goto :handle_command

:check_dependencies
echo [INFO] 检查Docker依赖...
docker --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker未安装，请先安装Docker Desktop
    exit /b 1
)

docker compose version >nul 2>&1
if errorlevel 1 (
    docker-compose --version >nul 2>&1
    if errorlevel 1 (
        echo [ERROR] Docker Compose未安装，请升级Docker Desktop
        exit /b 1
    )
)
echo [INFO] Docker环境检查通过
exit /b 0

:start_clickhouse
echo [INFO] 启动ClickHouse服务...
cd /d "%SCRIPT_DIR%"

:: 尝试使用新版docker compose命令
docker compose up -d clickhouse 2>nul
if errorlevel 1 (
    echo [INFO] 使用docker-compose命令...
    docker-compose up -d clickhouse
    if errorlevel 1 (
        echo [ERROR] 启动失败
        exit /b 1
    )
)

echo [INFO] ClickHouse服务启动中，等待就绪...

:: 等待健康检查通过
set /a retry_count=0
set /a max_retries=30

:wait_loop
if %retry_count% gtr %max_retries% (
    echo [ERROR] ClickHouse启动超时，请检查日志
    exit /b 1
)

docker exec nginx-analytics-clickhouse wget --quiet --tries=1 --spider http://localhost:8123/ping >nul 2>&1
if not errorlevel 1 (
    echo [SUCCESS] ClickHouse服务已就绪!
    echo [INFO] HTTP接口: http://localhost:8123
    echo [INFO] Native接口: localhost:9000
    echo [INFO] 默认用户: analytics_user / analytics_password
    echo [INFO] Web界面: http://localhost:8123/play
    exit /b 0
)

set /a retry_count+=1
echo|set /p=.
timeout /t 2 /nobreak >nul
goto :wait_loop

:start_full
echo [INFO] 启动完整环境 (ClickHouse + Grafana)...
cd /d "%SCRIPT_DIR%"

docker compose --profile monitoring up -d 2>nul
if errorlevel 1 (
    docker-compose --profile monitoring up -d
    if errorlevel 1 (
        echo [ERROR] 启动失败
        exit /b 1
    )
)

echo [SUCCESS] 完整环境启动中...
echo [INFO] ClickHouse: http://localhost:8123
echo [INFO] Grafana: http://localhost:3000 (admin/admin)
exit /b 0

:stop_services
echo [INFO] 停止所有服务...
cd /d "%SCRIPT_DIR%"

docker compose down 2>nul
if errorlevel 1 (
    docker-compose down
)

echo [SUCCESS] 所有服务已停止
exit /b 0

:restart_services
echo [INFO] 重启服务...
call :stop_services
timeout /t 3 /nobreak >nul
call :start_clickhouse
exit /b 0

:status_services
echo [INFO] 服务状态:
cd /d "%SCRIPT_DIR%"

docker compose ps 2>nul
if errorlevel 1 (
    docker-compose ps
)
exit /b 0

:show_logs
set SERVICE=%2
if "%SERVICE%"=="" set SERVICE=clickhouse

echo [INFO] 显示 %SERVICE% 服务日志...
cd /d "%SCRIPT_DIR%"

docker compose logs -f --tail=100 %SERVICE% 2>nul
if errorlevel 1 (
    docker-compose logs -f --tail=100 %SERVICE%
)
exit /b 0

:connect_client
echo [INFO] 连接到ClickHouse客户端...

docker ps | findstr nginx-analytics-clickhouse >nul
if errorlevel 1 (
    echo [ERROR] ClickHouse服务未运行，请先启动服务
    exit /b 1
)

docker exec -it nginx-analytics-clickhouse clickhouse-client --user analytics_user --password analytics_password --database nginx_analytics
exit /b 0

:execute_sql
set SQL_FILE=%2
if "%SQL_FILE%"=="" (
    echo [ERROR] 请指定SQL文件路径
    exit /b 1
)

if not exist "%SQL_FILE%" (
    echo [ERROR] SQL文件不存在: %SQL_FILE%
    exit /b 1
)

echo [INFO] 执行SQL文件: %SQL_FILE%

docker exec -i nginx-analytics-clickhouse clickhouse-client --user analytics_user --password analytics_password --database nginx_analytics --multiquery < "%SQL_FILE%"
if not errorlevel 1 (
    echo [SUCCESS] SQL执行完成
) else (
    echo [ERROR] SQL执行失败
    exit /b 1
)
exit /b 0

:backup_data
set BACKUP_DIR=%2
if "%BACKUP_DIR%"=="" set BACKUP_DIR=.\backups

for /f "tokens=1-6 delims=/ :. " %%a in ("%date% %time%") do (
    set TIMESTAMP=%%c%%a%%b_%%d%%e%%f
)
set TIMESTAMP=%TIMESTAMP: =0%

set BACKUP_FILE=%BACKUP_DIR%\clickhouse_backup_%TIMESTAMP%.sql

if not exist "%BACKUP_DIR%" mkdir "%BACKUP_DIR%"

echo [INFO] 备份数据到: %BACKUP_FILE%

:: 简化版备份 - 导出表结构和数据
docker exec nginx-analytics-clickhouse clickhouse-client --user analytics_user --password analytics_password --database nginx_analytics --query "SHOW TABLES" --format TabSeparated > temp_tables.txt

echo -- ClickHouse Backup %DATE% %TIME% > "%BACKUP_FILE%"
echo. >> "%BACKUP_FILE%"

for /f %%i in (temp_tables.txt) do (
    echo -- Table: %%i >> "%BACKUP_FILE%"
    docker exec nginx-analytics-clickhouse clickhouse-client --user analytics_user --password analytics_password --database nginx_analytics --query "SHOW CREATE TABLE %%i" >> "%BACKUP_FILE%"
    echo. >> "%BACKUP_FILE%"
)

del temp_tables.txt
echo [SUCCESS] 备份完成: %BACKUP_FILE%
exit /b 0

:cleanup_data
echo [WARNING] 此操作将删除所有数据和容器
set /p confirm="确认要清理所有数据吗? [y/N]: "
if /i not "%confirm%"=="y" (
    echo [INFO] 取消清理操作
    exit /b 0
)

echo [INFO] 清理所有数据...
cd /d "%SCRIPT_DIR%"

docker compose down -v 2>nul
if errorlevel 1 (
    docker-compose down -v
)

echo [SUCCESS] 数据已清理
exit /b 0

:show_help
echo ClickHouse Docker Compose 管理工具 (Windows版本)
echo.
echo 使用方法: %~nx0 [command]
echo.
echo 命令:
echo   start          启动ClickHouse服务
echo   start-full     启动完整环境 (ClickHouse + Grafana)
echo   stop           停止所有服务
echo   restart        重启服务
echo   status         查看服务状态
echo   logs [service] 查看服务日志 (默认: clickhouse)
echo   client         连接ClickHouse客户端
echo   sql ^<file^>     执行SQL文件
echo   backup [dir]   备份数据 (默认: .\backups)
echo   cleanup        清理所有数据和容器
echo   help           显示此帮助信息
echo.
echo 示例:
echo   %~nx0 start                    启动ClickHouse
echo   %~nx0 logs clickhouse          查看ClickHouse日志  
echo   %~nx0 sql .\init.sql          执行SQL文件
echo   %~nx0 backup .\my_backups     备份到指定目录
exit /b 0

:handle_command
if "%COMMAND%"=="start" call :start_clickhouse
if "%COMMAND%"=="start-full" call :start_full
if "%COMMAND%"=="stop" call :stop_services
if "%COMMAND%"=="restart" call :restart_services
if "%COMMAND%"=="status" call :status_services
if "%COMMAND%"=="logs" call :show_logs
if "%COMMAND%"=="client" call :connect_client
if "%COMMAND%"=="sql" call :execute_sql
if "%COMMAND%"=="backup" call :backup_data
if "%COMMAND%"=="cleanup" call :cleanup_data
if "%COMMAND%"=="help" call :show_help
if "%COMMAND%"=="-h" call :show_help
if "%COMMAND%"=="--help" call :show_help

:: 检查是否为未知命令
if not "%COMMAND%"=="start" if not "%COMMAND%"=="start-full" if not "%COMMAND%"=="stop" if not "%COMMAND%"=="restart" if not "%COMMAND%"=="status" if not "%COMMAND%"=="logs" if not "%COMMAND%"=="client" if not "%COMMAND%"=="sql" if not "%COMMAND%"=="backup" if not "%COMMAND%"=="cleanup" if not "%COMMAND%"=="help" if not "%COMMAND%"=="-h" if not "%COMMAND%"=="--help" (
    echo [ERROR] 未知命令: %COMMAND%
    echo.
    call :show_help
    exit /b 1
)

endlocal