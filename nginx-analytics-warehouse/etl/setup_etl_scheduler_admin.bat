@echo off
echo 正在请求管理员权限以配置ETL定时任务...
echo 请在UAC提示中点击"是"
echo.
powershell -Command "Start-Process PowerShell -ArgumentList '-ExecutionPolicy Bypass -File \"%~dp0setup_etl_scheduler.ps1\"' -Verb RunAs"
echo.
echo 如果没有弹出PowerShell窗口，请手动以管理员身份运行 setup_etl_scheduler.ps1
pause