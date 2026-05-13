"""
Lance ce script UNE FOIS pour adapter App.jsx à la prod AWS.
Remplace les fetch('/api/...') par fetch(`${API_BASE}/api/...`)
afin que VITE_API_URL soit pris en compte au build.
"""
import os, re

path = os.path.expanduser("~/etoro-react/src/App.jsx")
with open(path) as f:
    src = f.read()

# Ajoute la constante API_BASE après les imports
if "API_BASE" not in src:
    src = src.replace(
        "const fmt =",
        "const API_BASE = import.meta.env.VITE_API_URL || ''\nconst fmt ="
    )

# Remplace tous les fetch('/api/...') par fetch(`${API_BASE}/api/...`)
src = re.sub(r"fetch\('(/api/[^']+)'\)", r"fetch(`${API_BASE}\1`)", src)
src = re.sub(r'fetch\("(/api/[^"]+)"\)', r"fetch(`${API_BASE}\1`)", src)
src = re.sub(r'fetch\(`(/api/[^`]+)`\)', r"fetch(`${API_BASE}\1`)", src)

with open(path, "w") as f:
    f.write(src)

print("✓ App.jsx patché — VITE_API_URL activé")
