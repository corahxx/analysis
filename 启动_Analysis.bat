@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo Starting Analysis...
streamlit run app.py
pause
