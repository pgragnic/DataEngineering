@echo off
setlocal

echo ==============================================
echo  UC 28 - Inspection Augmentee (Code Resonance)
echo ==============================================

:: Dossier racine du projet (dossier contenant ce .bat)
set ROOT=%~dp0

:: Verification du .env backend
if not exist "%ROOT%backend\.env" (
    echo [ERREUR] backend\.env introuvable.
    echo Copiez backend\.env.example vers backend\.env et renseignez ANTHROPIC_API_KEY.
    pause
    exit /b 1
)

:: -- Backend --------------------------------------------------
echo.
echo [1/2] Demarrage du backend FastAPI sur http://localhost:8000 ...
start "Backend - FastAPI" cmd /k "cd /d "%ROOT%backend" && uv run uvicorn main:app --reload --host 0.0.0.0 --port 8000"

:: Pause pour laisser uvicorn demarrer avant le frontend
timeout /t 3 /nobreak > nul

:: -- Frontend -------------------------------------------------
echo [2/2] Demarrage du frontend Vite sur http://localhost:5173 ...
start "Frontend - Vite" cmd /k "cd /d "%ROOT%frontend" && npm run dev"

echo.
echo Tous les composants sont lances.
echo   Backend  : http://localhost:8000
echo   API docs : http://localhost:8000/docs
echo   Frontend : http://localhost:5173
echo.
echo Fermez les fenetres de terminal pour arreter les serveurs.
endlocal