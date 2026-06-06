@echo off
setlocal

set ROOT=%~dp0
set BACKEND=%ROOT%backend
set FRONTEND=%ROOT%frontend
set VENV_PYTHON=%BACKEND%\.venv\Scripts\python.exe

echo ==============================================
echo  UC 28 - Inspection Augmentee
echo ==============================================
echo.

rem --- Backend ---
echo Demarrage du backend FastAPI...
start "Backend" cmd /k "cd /d %BACKEND% && %VENV_PYTHON% -m uvicorn main:app --host 0.0.0.0 --port 8000"

timeout /t 4 /nobreak > nul

rem --- Frontend ---
echo Demarrage du frontend Vite...
start "Frontend" cmd /k "cd /d %FRONTEND% && npm run dev"

timeout /t 4 /nobreak > nul

rem --- Navigateur ---
start http://localhost:3000

echo.
echo Application disponible sur http://localhost:3000
echo Fermez les 2 fenetres pour tout arreter.
echo.

endlocal
