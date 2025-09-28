@echo off
chcp 65001
echo =============================================
echo ETL调度脚本清理工具
echo =============================================
echo.

echo 📋 当前ETL目录下的调度相关文件:
echo.

REM 列出现有的调度相关文件
if exist "setup_etl_scheduler_admin.bat" echo   ❌ setup_etl_scheduler_admin.bat (将删除)
if exist "setup_etl_scheduler.ps1" echo   ❌ setup_etl_scheduler.ps1 (将删除)
if exist "run_auto_etl_portable.bat" echo   ❌ run_auto_etl_portable.bat (将删除)
if exist "run_auto_etl.bat" echo   ❌ run_auto_etl.bat (将保留为备份)

echo.
echo 📋 新的优化脚本:
if exist "run_etl_scheduler.bat" echo   ✅ run_etl_scheduler.bat (主执行脚本)
if exist "run_etl_scheduler.ps1" echo   ✅ run_etl_scheduler.ps1 (PowerShell版本)
if exist "setup_scheduler_task.bat" echo   ✅ setup_scheduler_task.bat (任务计划器设置)

echo.
echo 🔧 清理说明:
echo   - 删除有问题的旧脚本
echo   - 保留run_auto_etl.bat作为备份
echo   - 新脚本解决了clickhouse_connect依赖问题
echo   - 新脚本实现了日志双重输出功能
echo.

set /p CONFIRM="确认清理旧脚本? (Y/n): "
if /i "%CONFIRM%" neq "Y" if /i "%CONFIRM%" neq "" (
    echo 清理已取消
    pause
    exit /b 0
)

echo.
echo 🧹 开始清理...

REM 删除有问题的旧脚本
if exist "setup_etl_scheduler_admin.bat" (
    del "setup_etl_scheduler_admin.bat"
    if exist "setup_etl_scheduler_admin.bat" (
        echo ❌ 删除失败: setup_etl_scheduler_admin.bat
    ) else (
        echo ✅ 已删除: setup_etl_scheduler_admin.bat
    )
)

if exist "setup_etl_scheduler.ps1" (
    del "setup_etl_scheduler.ps1"
    if exist "setup_etl_scheduler.ps1" (
        echo ❌ 删除失败: setup_etl_scheduler.ps1
    ) else (
        echo ✅ 已删除: setup_etl_scheduler.ps1
    )
)

if exist "run_auto_etl_portable.bat" (
    del "run_auto_etl_portable.bat"
    if exist "run_auto_etl_portable.bat" (
        echo ❌ 删除失败: run_auto_etl_portable.bat
    ) else (
        echo ✅ 已删除: run_auto_etl_portable.bat
    )
)

REM 重命名run_auto_etl.bat为备份
if exist "run_auto_etl.bat" (
    if not exist "run_auto_etl_backup.bat" (
        ren "run_auto_etl.bat" "run_auto_etl_backup.bat"
        if exist "run_auto_etl_backup.bat" (
            echo ✅ 已重命名: run_auto_etl.bat -> run_auto_etl_backup.bat
        ) else (
            echo ❌ 重命名失败: run_auto_etl.bat
        )
    ) else (
        echo ⚠️  备份文件已存在，跳过重命名
    )
)

echo.
echo ✅ 清理完成！
echo.
echo 📋 现在请使用新的脚本:
echo   🚀 执行ETL: run_etl_scheduler.bat
echo   ⚙️  设置定时任务: setup_scheduler_task.bat
echo   🔧 PowerShell版本: run_etl_scheduler.ps1
echo.

echo 🎯 新脚本优势:
echo   ✅ 解决clickhouse_connect依赖问题
echo   ✅ 自动检查和安装依赖
echo   ✅ 日志双重输出（控制台+文件）
echo   ✅ 更详细的错误诊断
echo   ✅ 兼容zip目录的成功经验
echo.

echo =============================================
pause