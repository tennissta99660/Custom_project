@echo off
title Atlas - Starting All Databases
color 0A

echo ============================================
echo   ATLAS - Starting All Databases
echo ============================================
echo.

echo [1/3] PostgreSQL...
net start postgresql-x64-18 2>nul || echo    Already running
timeout /t 2 /nobreak >nul

echo [2/3] Memurai (Redis)...
net start memurai 2>nul || echo    Already running
timeout /t 2 /nobreak >nul

echo [3/3] ClickHouse (WSL)...
wsl sudo service clickhouse-server start
timeout /t 3 /nobreak >nul

echo.
echo ============================================
echo  ⚠️  NOW: Open Neo4j Desktop and click START
echo ============================================
echo  Press any key AFTER Neo4j shows RUNNING...
pause >nul

cd /d E:\terminal_proj
call conda activate quant 2>nul
python scripts\verify_connections.py
pause
