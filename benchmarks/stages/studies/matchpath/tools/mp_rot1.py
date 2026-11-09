#!/usr/bin/env python3
"""mp_rot1.py --dir D: after rotation 1, every cell censored there (> 10 s) is added to D/excluded.json for its system,
labelled "excluded: > 10 s in rotation 1 ...", so rotation 2 and the engine pass do not run it again. A cell censored in
either rotation is censored (the W1/W2 summary rule), so this changes no cell's class; it saves the re-priming of a fresh
worker after every censored call. Stated in the job's records and table."""
import argparse
import json
from pathlib import Path

LABEL = "excluded: > 10 s in rotation 1 (censored; not run again in rotation 2 or the engine pass)"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", required=True)
    a = ap.parse_args()
    d = Path(a.dir)
    ex = json.loads((d / "excluded.json").read_text()) if (d / "excluded.json").exists() else {}
    n = 0
    for f in sorted((d / "rot1").glob("*.json")):
        rec = json.loads(f.read_text())
        s = rec.get("system")
        for c in rec.get("results", []) if s else []:
            e = str(c.get("error", ""))
            if not c.get("ok") and (c.get("aborted") or c.get("censored") or e.startswith(">")) and not c.get("excluded"):
                k = f"{c['query']}#{c.get('instance')}"
                if k not in ex.setdefault(s, {}):
                    ex[s][k] = LABEL
                    n += 1
    (d / "excluded.json").write_text(json.dumps(ex, indent=1, sort_keys=True) + "\n")
    print(f"censored in rotation 1, not run again: {n}")


if __name__ == "__main__":
    main()
