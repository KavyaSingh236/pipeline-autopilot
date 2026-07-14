#!/bin/bash
echo "========================================"
echo " Pipeline Autopilot - First Time Setup"
echo "========================================"

echo "[1/3] Creating Python virtual environment..."
python3 -m venv venv
source venv/bin/activate

echo "[2/3] Installing Python packages..."
pip install -r backend/requirements.txt

echo "[3/3] Installing frontend packages..."
cd frontend && npm install && cd ..

echo ""
echo "Setup complete! Run: ./start.sh"
