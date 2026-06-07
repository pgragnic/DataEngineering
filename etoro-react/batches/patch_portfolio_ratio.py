#!/usr/bin/env python3
"""
Run from Termux: python3 ~/DataEngineering/etoro-react/patch_portfolio_ratio.py
Adds an after_request hook to ~/app.py that applies the stored ratio
(from ~/etoro_config.json) to all monetary values in /api/portfolio responses.
"""
import re, shutil, sys
from pathlib import Path

APP = Path.home() / "app.py"
if not APP.exists():
    print(f"Erreur : {APP} introuvable"); sys.exit(1)

shutil.copy(APP, APP.with_suffix(".py.bak"))
print(f"Backup : {APP}.bak")

src = APP.read_text()

if 'apply_portfolio_ratio' in src:
    print("Hook apply_portfolio_ratio déjà présent — rien à faire.")
    sys.exit(0)

RATIO_HOOK = '''
@app.after_request
def apply_portfolio_ratio(response):
    import json as _json
    from pathlib import Path as _Path
    if request.path != "/api/portfolio":
        return response
    if "application/json" not in response.content_type:
        return response
    try:
        cfg_file = _Path.home() / "etoro_config.json"
        ratio = float(_json.loads(cfg_file.read_text()).get("ratio", 1.0))
    except Exception:
        ratio = 1.0
    if ratio == 1.0:
        return response
    try:
        data = _json.loads(response.get_data(as_text=True))
        _fields = {"equity", "cash", "amount", "pnl"}

        def _mul(obj):
            if isinstance(obj, dict):
                return {
                    k: round(v * ratio, 2) if k in _fields and isinstance(v, (int, float)) else _mul(v)
                    for k, v in obj.items()
                }
            if isinstance(obj, list):
                return [_mul(item) for item in obj]
            return obj

        response.set_data(_json.dumps(_mul(data)))
    except Exception:
        pass
    return response
'''

# Insert just before the if __name__ block, or before the last route
insert_before = '\nif __name__'
if insert_before in src:
    src = src.replace(insert_before, '\n' + RATIO_HOOK + insert_before, 1)
else:
    matches = list(re.finditer(r'\n@app\.route', src))
    if matches:
        pos = matches[-1].start()
        src = src[:pos] + '\n' + RATIO_HOOK + src[pos:]
    else:
        src = src + '\n' + RATIO_HOOK

APP.write_text(src)
print("Hook apply_portfolio_ratio ajouté.")
print("Redémarre Flask : pkill -f 'python.*app.py' && python3 ~/app.py &")
