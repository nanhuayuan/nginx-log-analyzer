@echo off
:: N9E数据库修复工具 - Windows批处理版本
:: 解决Windows环境下N9E数据库初始化问题

setlocal enabledelayedexpansion

echo.
echo ========================================
echo    N9E数据库修复工具 - Windows版本
echo ========================================
echo.

:: 检查Docker是否运行
echo [检查] 检查Docker环境...
docker info >nul 2>&1
if errorlevel 1 (
    echo [错误] Docker未运行，请启动Docker Desktop
    pause
    exit /b 1
)
echo [成功] Docker环境正常

:: 检查docker-compose
docker-compose --version >nul 2>&1
if errorlevel 1 (
    docker compose --version >nul 2>&1
    if errorlevel 1 (
        echo [错误] 未找到docker-compose命令
        pause
        exit /b 1
    )
    set DOCKER_COMPOSE=docker compose
) else (
    set DOCKER_COMPOSE=docker-compose
)
echo [成功] Docker Compose可用

:: 检查N9E MySQL容器状态
echo [检查] 检查N9E MySQL容器...
docker ps --filter "name=n9e-mysql" --format "table {{.Names}}" | findstr n9e-mysql >nul
if errorlevel 1 (
    echo [信息] N9E MySQL容器未运行，正在启动...
    %DOCKER_COMPOSE% up -d n9e-mysql
    if errorlevel 1 (
        echo [错误] 启动N9E MySQL失败
        pause
        exit /b 1
    )

    echo [等待] 等待MySQL就绪...
    timeout /t 15 /nobreak >nul

    :: 等待MySQL就绪
    set /a count=0
    :wait_mysql
    docker exec n9e-mysql mysqladmin ping -h localhost -uroot -p1234 --silent >nul 2>&1
    if errorlevel 1 (
        set /a count+=1
        if !count! geq 30 (
            echo [错误] MySQL启动超时
            pause
            exit /b 1
        )
        echo 等待中...
        timeout /t 2 /nobreak >nul
        goto wait_mysql
    )
)
echo [成功] N9E MySQL容器运行正常

:: 检查数据库状态
echo [检查] 检查数据库状态...
docker exec n9e-mysql mysql -uroot -p1234 -e "USE n9e_v6; SHOW TABLES;" 2>nul | find /c "Tables_in_n9e_v6" >nul
if errorlevel 1 (
    echo [警告] n9e_v6数据库不存在或有问题
    goto fix_database
)

:: 统计表数量
for /f %%i in ('docker exec n9e-mysql mysql -uroot -p1234 -e "USE n9e_v6; SHOW TABLES;" 2^>nul ^| find /c /v "Tables_in_n9e_v6"') do set table_count=%%i
echo [信息] 当前表数量: %table_count%

if %table_count% lss 100 (
    echo [警告] 表数量不足 ^(%table_count%/152^)，需要重新初始化
    goto fix_database
)

:: 检查users表数据
docker exec n9e-mysql mysql -uroot -p1234 -e "USE n9e_v6; SELECT COUNT(*) FROM users;" 2>nul | findstr "0" >nul
if not errorlevel 1 (
    echo [警告] users表为空，需要重新初始化
    goto fix_database
)

echo [成功] 数据库状态正常，无需修复
echo [信息] 可以访问: http://localhost:17000 ^(root/root.2020^)
pause
exit /b 0

:fix_database
echo.
echo [修复] 开始修复N9E数据库...
echo ----------------------------------------

:: 备份当前数据库（如果存在）
echo [备份] 创建数据库备份...
set backup_file=n9e_backup_%date:~0,4%%date:~5,2%%date:~8,2%_%time:~0,2%%time:~3,2%%time:~6,2%.sql
set backup_file=%backup_file: =0%
docker exec n9e-mysql mysqldump -uroot -p1234 --single-transaction n9e_v6 > "%backup_file%" 2>nul
if not errorlevel 1 (
    echo [成功] 备份已保存: %backup_file%
) else (
    echo [信息] 跳过备份（数据库可能不存在）
)

:: 删除现有数据库
echo [清理] 删除现有数据库...
docker exec n9e-mysql mysql -uroot -p1234 -e "DROP DATABASE IF EXISTS n9e_v6;"
if errorlevel 1 (
    echo [错误] 删除数据库失败
    pause
    exit /b 1
)

:: 检查初始化脚本
if not exist "services\n9e\init-scripts\a-n9e.sql" (
    echo [错误] 初始化脚本不存在: services\n9e\init-scripts\a-n9e.sql
    pause
    exit /b 1
)

:: 执行初始化脚本
echo [执行] 执行数据库初始化...
docker cp "services\n9e\init-scripts\a-n9e.sql" n9e-mysql:/tmp/a-n9e.sql
if errorlevel 1 (
    echo [错误] 复制主初始化脚本失败
    pause
    exit /b 1
)

docker cp "services\n9e\init-scripts\c-init.sql" n9e-mysql:/tmp/c-init.sql
if errorlevel 1 (
    echo [错误] 复制权限脚本失败
    pause
    exit /b 1
)

echo [执行] 执行主数据库脚本...
docker exec n9e-mysql mysql -uroot -p1234 -e "source /tmp/a-n9e.sql"
if errorlevel 1 (
    echo [错误] 执行主初始化脚本失败
    pause
    exit /b 1
)

echo [执行] 执行权限配置脚本...
docker exec n9e-mysql mysql -uroot -p1234 -e "source /tmp/c-init.sql"
if errorlevel 1 (
    echo [错误] 执行权限脚本失败
    pause
    exit /b 1
)

:: 验证结果
echo [验证] 验证数据库初始化结果...
for /f %%i in ('docker exec n9e-mysql mysql -uroot -p1234 -e "USE n9e_v6; SHOW TABLES;" 2^>nul ^| find /c /v "Tables_in_n9e_v6"') do set new_table_count=%%i
echo [信息] 新的表数量: %new_table_count%

if %new_table_count% lss 100 (
    echo [错误] 表数量仍然不足: %new_table_count%
    pause
    exit /b 1
)

:: 检查root用户
docker exec n9e-mysql mysql -uroot -p1234 -e "USE n9e_v6; SELECT username FROM users WHERE username='root';" 2>nul | findstr "root" >nul
if errorlevel 1 (
    echo [错误] root用户不存在
    pause
    exit /b 1
)

echo [成功] 数据库验证通过

:: 重启Nightingale服务
echo [重启] 重启Nightingale服务...
%DOCKER_COMPOSE% stop nightingale >nul 2>&1
%DOCKER_COMPOSE% up -d victoriametrics redis >nul 2>&1
timeout /t 5 /nobreak >nul
%DOCKER_COMPOSE% up -d nightingale >nul 2>&1

echo.
echo ========================================
echo           修复完成！
echo ========================================
echo [成功] N9E数据库修复完成
echo [信息] 表数量: %new_table_count%/152
if exist "%backup_file%" echo [备份] 备份文件: %backup_file%
echo [访问] Nightingale: http://localhost:17000
echo [账号] root/root.2020
echo.
pause