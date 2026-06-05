#!/usr/bin/env python3
"""
Run from Termux: python3 ~/DataEngineering/etoro-react/patch_config.py
Adds GET/POST /api/config endpoints to ~/app.py for ratio management.
"""
import re, shutil, sys
from pathlib import Path

APP = Path.home() / "app.py"
if not APP.exists():
    print(f"Erreur : {APP} introuvable"); sys.exit(1)

shutil.copy(APP, APP.with_suffix(".py.bak"))
print(f"Backup : {APP}.bak")

src = APP.read_text()

# Check if already patched
if '/api/config' in src:
    print("Route /api/config déjà présente — rien à faire.")
    sys.exit(0)

CONFIG_ROUTES = '''
@app.route("/api/config", methods=["GET"])
def get_config():
    import json as _json
    from pathlib import Path
    cfg_file = Path.home() / "etoro_config.json"
    try:
        cfg = _json.loads(cfg_file.read_text())
    except:
        cfg = {"ratio": 1.0}
    return jsonify(cfg)

@app.route("/api/config", methods=["POST"])
def set_config():
    import json as _json
    from pathlib import Path
    cfg_file = Path.home() / "etoro_config.json"
    data = request.get_json(force=True)
    ratio = float(data.get("ratio", 1.0))
    if ratio <= 0:
        return jsonify({"error": "ratio must be > 0"}), 400
    cfg_file.write_text(_json.dumps({"ratio": ratio}))
    return jsonify({"ok": True, "ratio": ratio})
'''

# Insert before the last route or if __name__ block
insert_before = '\nif __name__'
if insert_before not in src:
    # fallback: append before last @app.route
    matches = list(re.finditer(r'\n@app\.route', src))
    if matches:
        pos = matches[-1].start()
        src = src[:pos] + '\n' + CONFIG_ROUTES + src[pos:]
    else:
        src = src + '\n' + CONFIG_ROUTES
else:
    src = src.replace(insert_before, '\n' + CONFIG_ROUTES + insert_before, 1)

# Also patch the ratio into /api/portfolio if a RATIO variable exists
# Try to find and update ratio usage, or add config loading to portfolio route
if 'RATIO' in src or 'ratio' in src.lower():
    print("Note: variable RATIO trouvée dans app.py — vérifiez manuellement qu'elle lit ~/etoro_config.json")

APP.write_text(src)
print("Patch /api/config appliqué.")
print("Redémarre Flask : pkill -f 'python.*app.py' && python3 ~/app.py &")
