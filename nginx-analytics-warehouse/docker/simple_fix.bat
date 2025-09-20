@echo off
:: 简单的N9E数据库修复脚本
:: 一键修复新环境中N9E数据库问题

echo ==========================================
echo       N9E数据库一键修复工具
echo ==========================================
echo.

echo [1/5] 停止相关服务...
docker-compose stop nightingale 2>nul

echo [2/5] 清理N9E数据库...
docker exec n9e-mysql mysql -uroot -p1234 -e "DROP DATABASE IF EXISTS n9e_v6;" 2>nul

echo [3/5] 重新创建数据库...
docker cp services\n9e\init-scripts\a-n9e.sql n9e-mysql:/tmp/a-n9e.sql
docker cp services\n9e\init-scripts\c-init.sql n9e-mysql:/tmp/c-init.sql
echo 执行主数据库脚本...
docker exec n9e-mysql mysql -uroot -p1234 -e "source /tmp/a-n9e.sql"
echo 执行权限配置脚本...
docker exec n9e-mysql mysql -uroot -p1234 -e "source /tmp/c-init.sql"

echo [4/5] 验证修复结果...
for /f %%i in ('docker exec n9e-mysql mysql -uroot -p1234 -e "USE n9e_v6; SHOW TABLES;" 2^>nul ^| find /c /v "Tables_in_n9e_v6"') do set table_count=%%i
echo 表数量: %table_count%

echo [5/5] 重启服务...
docker-compose up -d

echo.
echo ==========================================
echo 修复完成！
echo 表数量: %table_count%/152
echo 访问地址: http://localhost:17000
echo 账号: root/root.2020
echo ==========================================
pause