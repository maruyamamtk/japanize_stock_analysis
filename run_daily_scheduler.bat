@echo off
chcp 65001 >nul
echo ========================================
echo  Japan Stock Analysis
echo ========================================
echo.
cd /d "%~dp0"

echo Collecting data
python unified_main.py --mode data --data-mode all

echo Analyze data
python unified_main.py --mode analysis --top-n 50

echo Notification
python line_notifier.py

echo.
echo ========================================
echo         Processing Complete
echo ========================================

:end
pause