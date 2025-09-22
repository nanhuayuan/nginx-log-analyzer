@echo off
cd /d "%~dp0"

echo =============================================
echo Nginx Log Auto Processor
echo Current Time: %date% %time%
echo =============================================
echo.

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
if not exist "nginx_zip_log_processor.py" (
    echo ERROR: nginx_zip_log_processor.py not found
    echo Current directory: %cd%
    pause
    exit /b 1
)

echo Starting nginx log processing...
echo Script path: %cd%\nginx_zip_log_processor.py
echo.

REM Run Python script
python nginx_zip_log_processor.py

echo.
echo Exit code: %errorlevel%
if %errorlevel% equ 0 (
    echo Processing completed successfully!
) else (
    echo Processing failed with error code: %errorlevel%
)

echo.
echo Check nginx_log_processor.log for detailed information
echo Press any key to exit...
pause >nul