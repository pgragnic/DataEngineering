"""
UC 28 — Inspection Augmentée · Lanceur Windows
Double-cliquer ce fichier pour démarrer l'application.
Prérequis : Python 3.11+ avec tkinter, Node.js 20+
"""

import subprocess
import sys
import threading
import webbrowser
from pathlib import Path
import tkinter as tk
from tkinter import scrolledtext, messagebox

# ── Chemins ───────────────────────────────────────────────────────────────────

ROOT = Path(__file__).parent
BACKEND = ROOT / "backend"
FRONTEND = ROOT / "frontend"
VENV_PYTHON = BACKEND / ".venv" / "Scripts" / "python.exe"
VENV_PIP = BACKEND / ".venv" / "Scripts" / "pip.exe"

# ── Couleurs ──────────────────────────────────────────────────────────────────

C_BG = "#1a1f2e"
C_PANEL = "#252b3b"
C_ACCENT = "#10b981"       # vert Bureau Veritas
C_ACCENT_DIM = "#065f46"
C_ALERT = "#f59e0b"
C_DANGER = "#ef4444"
C_TEXT = "#e2e8f0"
C_MUTE = "#64748b"
C_BORDER = "#334155"

# ── Processus en cours ────────────────────────────────────────────────────────

_procs: list[subprocess.Popen] = []


def _kill_all():
    for p in _procs:
        try:
            p.terminate()
        except Exception:
            pass
    _procs.clear()


# ── Fenêtre principale ────────────────────────────────────────────────────────

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("UC 28 — Inspection Augmentée")
        self.configure(bg=C_BG)
        self.resizable(False, False)
        self._center(720, 540)

        self._build_header()
        self._build_status_row()
        self._build_log()
        self._build_footer()

        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.after(200, self._run_setup)

    # ── Layout ─────────────────────────────────────────────────────────────

    def _center(self, w, h):
        sw, sh = self.winfo_screenwidth(), self.winfo_screenheight()
        self.geometry(f"{w}x{h}+{(sw - w)//2}+{(sh - h)//2}")

    def _build_header(self):
        header = tk.Frame(self, bg=C_PANEL, pady=16)
        header.pack(fill="x")
        tk.Label(
            header, text="UC 28 — Inspection Augmentée",
            font=("Segoe UI", 16, "bold"), fg=C_ACCENT, bg=C_PANEL,
        ).pack()
        tk.Label(
            header, text="Bureau Veritas Certification · Démonstrateur IA",
            font=("Segoe UI", 9), fg=C_MUTE, bg=C_PANEL,
        ).pack()

    def _build_status_row(self):
        row = tk.Frame(self, bg=C_BG, pady=10, padx=20)
        row.pack(fill="x")
        row.columnconfigure(0, weight=1)
        row.columnconfigure(1, weight=1)

        self._backend_indicator = self._make_indicator(row, "Backend Flask", "localhost:8000", 0)
        self._frontend_indicator = self._make_indicator(row, "Frontend Vite", "localhost:3000", 1)

    def _make_indicator(self, parent, title, url, col):
        frame = tk.Frame(parent, bg=C_PANEL, padx=12, pady=10)
        frame.grid(row=0, column=col, padx=(0, 6) if col == 0 else (6, 0), sticky="ew")
        frame.columnconfigure(1, weight=1)

        dot = tk.Label(frame, text="●", font=("Segoe UI", 14), fg=C_MUTE, bg=C_PANEL)
        dot.grid(row=0, column=0, rowspan=2, padx=(0, 8))

        tk.Label(frame, text=title, font=("Segoe UI", 10, "bold"), fg=C_TEXT, bg=C_PANEL, anchor="w").grid(row=0, column=1, sticky="w")
        tk.Label(frame, text=url, font=("Consolas", 9), fg=C_MUTE, bg=C_PANEL, anchor="w").grid(row=1, column=1, sticky="w")

        return dot   # on retourne le dot pour le mettre à jour

    def _build_log(self):
        frame = tk.Frame(self, bg=C_BG, padx=20)
        frame.pack(fill="both", expand=True)

        tk.Label(frame, text="Journal", font=("Segoe UI", 8, "bold"),
                 fg=C_MUTE, bg=C_BG, anchor="w").pack(fill="x")

        self.log = scrolledtext.ScrolledText(
            frame, height=14, font=("Consolas", 9),
            bg=C_PANEL, fg=C_TEXT, insertbackground=C_TEXT,
            relief="flat", borderwidth=0, state="disabled",
        )
        self.log.pack(fill="both", expand=True, pady=(4, 0))

        self.log.tag_config("ok", foreground=C_ACCENT)
        self.log.tag_config("warn", foreground=C_ALERT)
        self.log.tag_config("err", foreground=C_DANGER)
        self.log.tag_config("info", foreground=C_TEXT)

    def _build_footer(self):
        footer = tk.Frame(self, bg=C_PANEL, pady=12, padx=20)
        footer.pack(fill="x", side="bottom")
        footer.columnconfigure(1, weight=1)

        self._btn_start = tk.Button(
            footer, text="▶  Démarrer", font=("Segoe UI", 10, "bold"),
            bg=C_ACCENT, fg="white", activebackground=C_ACCENT_DIM,
            activeforeground="white", relief="flat", padx=16, pady=8,
            cursor="hand2", command=self._start_services, state="disabled",
        )
        self._btn_start.grid(row=0, column=0, padx=(0, 8))

        self._btn_browser = tk.Button(
            footer, text="🌐  Ouvrir l'application", font=("Segoe UI", 10),
            bg=C_BORDER, fg=C_TEXT, activebackground="#475569",
            activeforeground="white", relief="flat", padx=16, pady=8,
            cursor="hand2", command=self._open_browser, state="disabled",
        )
        self._btn_browser.grid(row=0, column=1, padx=(0, 8), sticky="w")

        self._btn_stop = tk.Button(
            footer, text="■  Arrêter", font=("Segoe UI", 10),
            bg=C_BORDER, fg=C_MUTE, activebackground="#475569",
            activeforeground="white", relief="flat", padx=16, pady=8,
            cursor="hand2", command=self._stop_services, state="disabled",
        )
        self._btn_stop.grid(row=0, column=2)

    # ── Logging thread-safe ─────────────────────────────────────────────────

    def _log(self, msg: str, tag: str = "info"):
        def _do():
            self.log.config(state="normal")
            self.log.insert("end", msg + "\n", tag)
            self.log.see("end")
            self.log.config(state="disabled")
        self.after(0, _do)

    def _set_dot(self, indicator: tk.Label, color: str):
        self.after(0, lambda: indicator.config(fg=color))

    # ── Vérifications et setup ──────────────────────────────────────────────

    def _run_setup(self):
        threading.Thread(target=self._setup_worker, daemon=True).start()

    def _setup_worker(self):
        ok = True

        # .env
        env_file = BACKEND / ".env"
        example_file = ROOT / ".env.example"
        if not env_file.exists():
            if example_file.exists():
                import shutil
                shutil.copy(example_file, env_file)
                self._log("⚠  backend\\.env créé depuis .env.example — renseignez ANTHROPIC_API_KEY", "warn")
            else:
                self._log("✗  backend\\.env introuvable et .env.example absent", "err")
                ok = False
        else:
            # vérifier que la clé est renseignée
            content = env_file.read_text(encoding="utf-8", errors="ignore")
            if "ANTHROPIC_API_KEY=" in content and "your_key" in content.lower().replace("_", ""):
                self._log("⚠  ANTHROPIC_API_KEY semble vide dans backend\\.env", "warn")
            else:
                self._log("✓  backend\\.env présent", "ok")

        # Python
        self._log("→  Vérification Python…", "info")
        try:
            r = subprocess.run([sys.executable, "--version"], capture_output=True, text=True)
            self._log(f"✓  {r.stdout.strip()}", "ok")
        except Exception:
            self._log("✗  Python introuvable", "err")
            ok = False

        # Node.js
        self._log("→  Vérification Node.js…", "info")
        try:
            r = subprocess.run(["node", "--version"], capture_output=True, text=True)
            self._log(f"✓  Node.js {r.stdout.strip()}", "ok")
        except FileNotFoundError:
            self._log("✗  Node.js introuvable — installez Node.js 20+ depuis nodejs.org", "err")
            ok = False

        # venv Python
        if not VENV_PYTHON.exists():
            self._log("→  Création de l'environnement Python (.venv)…", "info")
            r = subprocess.run(
                [sys.executable, "-m", "venv", str(BACKEND / ".venv")],
                capture_output=True, text=True,
            )
            if r.returncode != 0:
                self._log(f"✗  Échec venv : {r.stderr.strip()}", "err")
                ok = False
            else:
                self._log("✓  .venv créé", "ok")
                self._log("→  Installation des dépendances Python…", "info")
                r2 = subprocess.run(
                    [str(VENV_PIP), "install", "-r", str(BACKEND / "requirements.txt")],
                    capture_output=True, text=True,
                )
                if r2.returncode != 0:
                    self._log(f"✗  pip install échoué\n{r2.stderr[-400:]}", "err")
                    ok = False
                else:
                    self._log("✓  Dépendances Python installées", "ok")
        else:
            self._log("✓  .venv déjà présent", "ok")

        # npm
        node_modules = FRONTEND / "node_modules" / ".bin" / "vite"
        if not node_modules.exists():
            self._log("→  Installation des dépendances npm (première fois, ~1 min)…", "info")
            r = subprocess.run(
                ["npm", "install"], cwd=str(FRONTEND),
                capture_output=True, text=True, shell=True,
            )
            if r.returncode != 0:
                self._log(f"✗  npm install échoué\n{r.stderr[-400:]}", "err")
                ok = False
            else:
                self._log("✓  npm install terminé", "ok")
        else:
            self._log("✓  node_modules déjà présent", "ok")

        if ok:
            self._log("\n✓  Prêt — cliquez sur Démarrer", "ok")
            self.after(0, lambda: self._btn_start.config(state="normal"))
        else:
            self._log("\n✗  Corrigez les erreurs ci-dessus puis relancez", "err")

    # ── Démarrage des services ──────────────────────────────────────────────

    def _start_services(self):
        self._btn_start.config(state="disabled")
        threading.Thread(target=self._start_worker, daemon=True).start()

    def _start_worker(self):
        import os

        env = os.environ.copy()
        dotenv_path = BACKEND / ".env"
        if dotenv_path.exists():
            for line in dotenv_path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, _, v = line.partition("=")
                    env[k.strip()] = v.strip()

        NO_WINDOW = subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0

        # Backend Flask
        self._log("→  Démarrage du backend Flask…", "info")
        try:
            p_backend = subprocess.Popen(
                [str(VENV_PYTHON), "flask_app.py"],
                cwd=str(BACKEND),
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                creationflags=NO_WINDOW,
            )
            _procs.append(p_backend)
            threading.Thread(target=self._tail_log, args=(p_backend, "backend"), daemon=True).start()
            self._log("✓  Backend démarré (pid %d)" % p_backend.pid, "ok")
            self._set_dot(self._backend_indicator, C_ACCENT)
        except Exception as e:
            self._log(f"✗  Backend : {e}", "err")
            self._set_dot(self._backend_indicator, C_DANGER)
            return

        # Attendre Flask
        import time
        import urllib.request
        for _ in range(20):
            time.sleep(0.5)
            try:
                urllib.request.urlopen("http://localhost:8000/health", timeout=1)
                break
            except Exception:
                pass

        # Frontend Vite
        self._log("→  Démarrage du frontend Vite…", "info")
        try:
            p_frontend = subprocess.Popen(
                ["npm", "run", "dev"],
                cwd=str(FRONTEND),
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                shell=True,
                creationflags=NO_WINDOW,
            )
            _procs.append(p_frontend)
            threading.Thread(target=self._tail_log, args=(p_frontend, "frontend"), daemon=True).start()
            self._log("✓  Frontend démarré (pid %d)" % p_frontend.pid, "ok")
            self._set_dot(self._frontend_indicator, C_ACCENT)
        except Exception as e:
            self._log(f"✗  Frontend : {e}", "err")
            self._set_dot(self._frontend_indicator, C_DANGER)
            return

        self._log("\n✓  Application disponible → http://localhost:3000", "ok")
        self.after(0, lambda: self._btn_browser.config(state="normal"))
        self.after(0, lambda: self._btn_stop.config(state="normal"))

        # Ouvrir le navigateur après 3 s
        time.sleep(3)
        self.after(0, self._open_browser)

    def _tail_log(self, proc: subprocess.Popen, label: str):
        try:
            for raw in proc.stdout:
                line = raw.decode("utf-8", errors="replace").rstrip()
                if line:
                    self._log(f"  [{label}] {line}", "info")
        except Exception:
            pass

    # ── Actions ─────────────────────────────────────────────────────────────

    def _open_browser(self):
        webbrowser.open("http://localhost:3000")

    def _stop_services(self):
        self._log("\n→  Arrêt des services…", "warn")
        _kill_all()
        self._set_dot(self._backend_indicator, C_MUTE)
        self._set_dot(self._frontend_indicator, C_MUTE)
        self._btn_browser.config(state="disabled")
        self._btn_stop.config(state="disabled")
        self._btn_start.config(state="normal")
        self._log("✓  Services arrêtés", "ok")

    def _on_close(self):
        if _procs:
            if messagebox.askyesno("UC 28", "Arrêter les services avant de fermer ?"):
                _kill_all()
        self.destroy()


# ── Point d'entrée ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app = App()
    app.mainloop()
