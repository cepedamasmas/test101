@echo off
echo ======================================================================
echo   DUCKLAKE - TechStore Argentina Pipeline
echo ======================================================================
echo.

cd /d "%~dp0docker"

echo [1/4] Limpiando data anterior...
if exist "output\datalake" rmdir /s /q "output\datalake"
if exist "output\techstore.duckdb" del /q "output\techstore.duckdb"
echo   Limpio!

echo.
echo [2/4] Levantando servicios (MySQL, SFTP, PostgreSQL)...
docker compose up -d mysql sftp postgres

echo.
echo [3/4] Ejecutando pipeline...
docker compose up --build ducklake

echo.
echo [4/4] Pipeline finalizado!
echo.
echo   DuckDB:      docker\output\techstore.duckdb
echo   Parquets:    docker\output\datalake\
echo   PostgreSQL:  localhost:5433 (user: techstore / pass: techstore123)
echo.
echo   Para parar los servicios:  docker compose -f docker\docker-compose.yml down
echo ======================================================================
pause
