@echo off
echo ========================================
echo  Pipeline Autopilot - First Time Setup
echo ========================================

echo.
echo [1/3] Creating Python virtual environment...
python -m venv venv
call venv\Scripts\activate.bat

echo.
echo [2/3] Installing Python packages...
pip install -r backend\requirements.txt

echo.
echo [3/3] Installing frontend packages...
cd frontend
call npm install
cd ..

echo.
echo ========================================
echo  Setup complete!
echo  Now double-click start.bat to run.
echo ========================================
pause
