@echo off
chcp 65001 >nul
set PYTHONIOENCODING=utf-8

echo ========================================
echo Nginx ETL Auto Processing System
echo Start Time: %date% %time%
echo ========================================

REM Switch to script directory
cd /d "%~dp0"

REM Activate conda environment
echo Activating conda environment...
call conda activate py39
if errorlevel 1 (
    echo ERROR: Failed to activate conda environment py39
    echo Please check if conda is installed and py39 environment exists
    pause
    exit /b 1
)

echo Conda environment activated successfully
echo.

REM Check if Python script exists
if not exist "controllers\integrated_ultra_etl_controller.py" (
    echo ERROR: integrated_ultra_etl_controller.py not found
    echo Current directory: %cd%
    pause
    exit /b 1
)

echo Starting ETL processing...
echo Script path: %cd%\controllers\integrated_ultra_etl_controller.py
echo.

REM Setup log directory
set LOG_DIR=%~dp0logs
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
set LOG_FILE=%LOG_DIR%\etl_auto_%date:~0,4%%date:~5,2%%date:~8,2%_%time:~0,2%%time:~3,2%.log

echo Log file: %LOG_FILE%
echo ========================================

REM Log start information
echo ======================================== > "%LOG_FILE%"
echo Nginx ETL Auto Processing System >> "%LOG_FILE%"
echo Start Time: %date% %time% >> "%LOG_FILE%"
echo Working Directory: %cd% >> "%LOG_FILE%"
echo ======================================== >> "%LOG_FILE%"
echo. >> "%LOG_FILE%"

REM Start auto-monitor mode, run for 2 hours (7200 seconds)
python controllers\integrated_ultra_etl_controller.py --auto-monitor --monitor-duration 7200 --batch-size 3000 --workers 6 --refresh-minutes 2 2>&1 | powershell -Command "$input | ForEach-Object { Write-Host $_; Add-Content -Path '%LOG_FILE%' -Value $_ -Encoding UTF8 }"
set ETL_EXIT_CODE=%errorlevel%

echo.
echo ========================================
echo ETL Processing Completed: %date% %time%
echo Exit Code: %ETL_EXIT_CODE%

REM Log completion
echo. >> "%LOG_FILE%"
echo ======================================== >> "%LOG_FILE%"
echo ETL Processing Completed: %date% %time% >> "%LOG_FILE%"
echo Exit Code: %ETL_EXIT_CODE% >> "%LOG_FILE%"

REM Check execution result
if %ETL_EXIT_CODE% equ 0 (
    echo [SUCCESS] ETL processing completed successfully\!
    echo [SUCCESS] ETL processing completed successfully\! >> "%LOG_FILE%"
) else (
    echo [ERROR] ETL processing failed with exit code: %ETL_EXIT_CODE%
    echo [ERROR] ETL processing failed with exit code: %ETL_EXIT_CODE% >> "%LOG_FILE%"
)

echo ======================================== >> "%LOG_FILE%"

echo.
echo [LOG] Detailed log saved to: %LOG_FILE%
echo ========================================

echo.
echo Press any key to exit...
pause >nul
