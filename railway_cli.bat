@echo off
cd /d "%~dp0"

:: Forzar que la ventana no se cierre si se abre con doble click
if "%~1"=="--menu" goto MENU
cmd /k ""%~f0" --menu"
exit /b

:MENU
cls
echo =============================================
echo  Railway CLI - DuckLake
echo =============================================
echo.
echo  [1] Login
echo  [2] Linkear proyecto
echo  [3] Shell en ducklake (terminal dentro del contenedor)
echo  [4] Logs ducklake (en vivo)
echo  [5] Logs sftp (en vivo)
echo  [6] Ver variables ducklake
echo  [7] Abrir Railway en browser
echo  [8] Status del proyecto
echo  [0] Salir
echo.
set /p op="Opcion: "

if "%op%"=="1" goto LOGIN
if "%op%"=="2" goto LINK
if "%op%"=="3" goto SHELL
if "%op%"=="4" goto LOGS_DUCK
if "%op%"=="5" goto LOGS_SFTP
if "%op%"=="6" goto VARS
if "%op%"=="7" goto OPEN
if "%op%"=="8" goto STATUS
if "%op%"=="0" goto SALIR
goto MENU

:LOGIN
echo.
railway login
echo.
echo Login completado. Presiona cualquier tecla para continuar...
pause >nul
goto MENU

:LINK
echo.
echo Selecciona: organizacion ^> proyecto ^> environment (production)
railway link
echo.
pause
goto MENU

:SHELL
echo.
echo Abriendo terminal en ducklake...
echo (Para salir del shell: escribi "exit")
echo (Para consultar DuckDB: duckdb /app/output/techstore.duckdb)
echo.
railway shell --service ducklake
echo.
pause
goto MENU

:LOGS_DUCK
echo.
echo Mostrando logs... Ctrl+C para salir
echo.
railway logs --service ducklake --tail
pause
goto MENU

:LOGS_SFTP
echo.
echo Mostrando logs... Ctrl+C para salir
echo.
railway logs --service sftp --tail
pause
goto MENU

:VARS
echo.
railway variables --service ducklake
echo.
pause
goto MENU

:OPEN
railway open
goto MENU

:STATUS
echo.
railway status
echo.
pause
goto MENU

:SALIR
echo.
echo Hasta luego!
exit /b
