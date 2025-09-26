# This file runs *inside* the sandbox container.
from __future__ import annotations
import os
import sys
import glob
import pickle

import pandas as pd

os.environ.setdefault("MPLCONFIGDIR", "/tmp")

VARS_DIR = "/inputs"
CODE_PATH = "/inputs/code.py"


def _load_var(path: str):
    if pd is not None:
        try:
            return pd.read_pickle(path)  # type: ignore[attr-defined]
        except Exception:
            pass
    with open(path, "rb") as f:
        return pickle.load(f)


def run():
    # 1) Load variables
    ns = {
        "__name__": "__main__",
        "__file__": CODE_PATH,
    }
    for p in sorted(glob.glob(os.path.join(VARS_DIR, "*.pkl"))):
        name = os.path.splitext(os.path.basename(p))[0]
        try:
            ns[name] = _load_var(p)
        except Exception as e:
            print(f"[prelude] Failed to load {name}: {e}", file=sys.stderr, flush=True)

    # 2) Execute user code
    try:
        with open(CODE_PATH, "r", encoding="utf-8") as f:
            code = f.read()
    except Exception as e:
        print(f"[prelude] Failed to read code: {e}", file=sys.stderr, flush=True)
        sys.exit(1)

    try:
        exec(compile(code, CODE_PATH, "exec"), ns, ns)
    except SystemExit as e:
        # Preserve explicit exits
        raise e
    except Exception as e:
        # Let the traceback go to stderr naturally
        raise


if __name__ == "__main__":
    run()
