@echo off
chcp 65001 >nul
d:
cd /d "d:\文件\充电代码工作\analysis"
echo Current directory: %CD%
git push -u origin main
pause
