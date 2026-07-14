@echo off
echo ========================================
echo  Pipeline Autopilot - Starting...
echo ========================================

echo.
echo Starting Backend (FastAPI on port 8000)...
start "Backend" cmd /k "call venv\Scripts\activate.bat && cd backend && uvicorn server:app --reload --port 8000"

timeout /t 3 /nobreak >nul

echo Starting Frontend (React on port 3000)...
start "Frontend" cmd /k "cd frontend && npm start"

echo.
echo ========================================
echo  Opening in a few seconds...
echo.
echo  App  ->  http://localhost:3000
echo  API  ->  http://localhost:8000/docs
echo ========================================
pause
