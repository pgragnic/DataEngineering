@echo off
setlocal
chcp 65001 > nul

echo ==============================================
echo  UC 28 - Inspection Augmentee (Code Resonance)
echo ==============================================
echo.

set ROOT=%~dp0
set BACKEND=%ROOT%backend
set FRONTEND=%ROOT%frontend
set VENV_PYTHON=%BACKEND%\.venv\Scripts\python.exe
set VENV_PIP=%BACKEND%\.venv\Scripts\pip.exe

rem -- Verification .env -------------------------------------------------------
if not exist "%BACKEND%\.env" (
    if exist "%ROOT%.env.example" (
        copy "%ROOT%.env.example" "%BACKEND%\.env" > nul
        echo [ATTENTION] backend\.env cree depuis .env.example
        echo             Renseignez GEP_API_KEY dans backend\.env avant de continuer.
        pause
    ) else (
        echo [ERREUR] backend\.env introuvable.
        echo          Creez backend\.env avec GEP_API_KEY=votre_cle
        pause
        exit /b 1
    )
)

rem -- Verification Node.js ----------------------------------------------------
node --version > nul 2>&1
if errorlevel 1 (
    echo [ERREUR] Node.js introuvable. Installez Node.js 20+ depuis https://nodejs.org
    pause
    exit /b 1
)

rem -- Creation venv Python si absent ------------------------------------------
if not exist "%VENV_PYTHON%" (
    echo [1/4] Creation de l'environnement Python .venv ...
    python -m venv "%BACKEND%\.venv"
    if errorlevel 1 (
        echo [ERREUR] Echec creation venv. Verifiez que Python 3.11+ est installe.
        pause
        exit /b 1
    )
    echo [2/4] Installation des dependances Python ...
    "%VENV_PIP%" install -r "%BACKEND%\requirements.txt" --quiet
    if errorlevel 1 (
        echo [ERREUR] pip install a echoue. Verifiez requirements.txt.
        pause
        exit /b 1
    )
) else (
    echo [OK] Environnement Python .venv present.
)

rem -- npm install si absent ---------------------------------------------------
if not exist "%FRONTEND%\node_modules\vite" (
    echo [3/4] Installation des dependances npm (premiere fois ~1 min) ...
    cd /d "%FRONTEND%"
    npm install
    if errorlevel 1 (
        echo [ERREUR] npm install a echoue.
        pause
        exit /b 1
    )
    cd /d "%ROOT%"
) else (
    echo [OK] node_modules present.
)

rem -- Demarrage backend -------------------------------------------------------
echo.
echo [4/4] Demarrage des serveurs...
echo.
start "UC28 - Backend FastAPI" cmd /k "cd /d "%BACKEND%" && "%VENV_PYTHON%" -m uvicorn main:app --host 0.0.0.0 --port 8000"

timeout /t 4 /nobreak > nul

rem -- Demarrage frontend ------------------------------------------------------
start "UC28 - Frontend Vite" cmd /k "cd /d "%FRONTEND%" && npm run dev"

timeout /t 4 /nobreak > nul

rem -- Ouvrir le navigateur ----------------------------------------------------
start "" "http://localhost:3000"

echo.
echo Application lancee !
echo   Frontend  : http://localhost:3000
echo   Backend   : http://localhost:8000
echo   API docs  : http://localhost:8000/docs
echo.
echo Fermez les 2 fenetres de terminal pour tout arreter.
echo.

endlocal
