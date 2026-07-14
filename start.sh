#!/bin/bash
echo "========================================"
echo " Pipeline Autopilot - Starting..."
echo "========================================"

source venv/bin/activate

echo "Starting Backend..."
cd backend && uvicorn server:app --reload --port 8000 &
BACK_PID=$!
cd ..

sleep 2

echo "Starting Frontend..."
cd frontend && npm start &
FRONT_PID=$!
cd ..

echo ""
echo "========================================"
echo " App  ->  http://localhost:3000"
echo " API  ->  http://localhost:8000/docs"
echo " Ctrl+C to stop"
echo "========================================"

trap "kill $BACK_PID $FRONT_PID 2>/dev/null; exit" INT
wait
