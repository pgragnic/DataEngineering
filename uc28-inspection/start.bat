@echo off
chcp 65001 >nul
title UC 28 — Inspection Augmentée

echo.
echo ╔══════════════════════════════════════════╗
echo ║  UC 28 — Inspection Augmentée           ║
echo ╚══════════════════════════════════════════╝
echo.

set ROOT=%~dp0
set BACKEND_DIR=%ROOT%backend
set FRONTEND_DIR=%ROOT%frontend

:: ── Vérifier .env ────────────────────────────────────────────────────────────
if not exist "%BACKEND_DIR%\.env" (
    echo [!] backend\.env introuvable — copie depuis .env.example...
    copy "%ROOT%.env.example" "%BACKEND_DIR%\.env" >nul
    echo [!] IMPORTANT : editez backend\.env et renseignez ANTHROPIC_API_KEY
    pause
    exit /b 1
)
echo [OK] .env present

:: ── Vérifier Python ──────────────────────────────────────────────────────────
python --version >nul 2>&1
if errorlevel 1 (
    echo [!] Python introuvable. Installez Python 3.11+ depuis python.org
    pause
    exit /b 1
)
echo [OK] Python detecte

:: ── Installer deps Python ────────────────────────────────────────────────────
if not exist "%BACKEND_DIR%\.venv" (
    echo [>>] Creation du venv Python...
    python -m venv "%BACKEND_DIR%\.venv"
    echo [>>] Installation des dependances Python...
    "%BACKEND_DIR%\.venv\Scripts\pip" install -r "%BACKEND_DIR%\requirements.txt"
    echo [OK] Dependances Python installees
)

:: ── Vérifier Node ────────────────────────────────────────────────────────────
node --version >nul 2>&1
if errorlevel 1 (
    echo [!] Node.js introuvable. Installez Node.js 20+ depuis nodejs.org
    pause
    exit /b 1
)
echo [OK] Node.js detecte

:: ── Installer deps npm ───────────────────────────────────────────────────────
if not exist "%FRONTEND_DIR%\node_modules\.bin\vite" (
    echo [>>] Installation des dependances npm...
    pushd "%FRONTEND_DIR%"
    call npm install
    popd
    echo [OK] npm install termine
)

:: ── Démarrer le backend ──────────────────────────────────────────────────────
echo [>>] Demarrage du backend Flask sur http://localhost:8000
start "UC28-Backend" cmd /k "cd /d %BACKEND_DIR% && .venv\Scripts\python flask_app.py"

:: Attendre que Flask soit prêt
echo [>>] Attente du backend...
timeout /t 4 /nobreak >nul

:: ── Démarrer le frontend ─────────────────────────────────────────────────────
echo [>>] Demarrage du frontend Vite sur http://localhost:3000
start "UC28-Frontend" cmd /k "cd /d %FRONTEND_DIR% && npm run dev"

echo.
echo [OK] Services demarres !
echo.
echo   Backend  : http://localhost:8000/health
echo   Frontend : http://localhost:3000
echo.
echo Fermez les deux fenetres de terminal pour arreter les services.
echo.
pause
