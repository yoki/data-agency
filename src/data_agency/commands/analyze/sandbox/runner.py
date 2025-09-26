import os
import re
import shutil
import tempfile
import pickle
from pathlib import Path
from typing import Dict, Any, Set
from datetime import datetime

import pandas as pd


from ..models import ExecutionResult
from .docker_runtime import DockerRuntime
from .prelude import run as _PRELUDE_RUN  # only to access source file path
from ....common.load_env import CONTAINER_IO_PATH


def _write_prelude_to(path: Path) -> None:
    # Find our installed prelude.py file content and copy under inputs
    prelude_src = Path(__file__).with_name("prelude.py")
    shutil.copyfile(prelude_src, path)


def _find_used_variables(code: str, namespace: Dict[str, Any]) -> Dict[str, Any]:
    # Very simple identifier scan; mirrors your earlier heuristic
    variable_pattern = r"\b([a-zA-Z_][a-zA-Z0-9_]*)\b"
    used: Set[str] = set(re.findall(variable_pattern, code))
    return {k: v for k, v in namespace.items() if k in used}


def _save_var(path: Path, value: Any) -> None:
    if pd is not None and hasattr(pd, "DataFrame") and isinstance(value, getattr(pd, "DataFrame")):
        value.to_pickle(path)  # type: ignore[attr-defined]
        return
    # generic pickle
    with open(path, "wb") as f:
        pickle.dump(value, f, protocol=pickle.HIGHEST_PROTOCOL)


def _cleanup_old_runs(max_runs: int = 50) -> None:
    """Keep only the most recent max_runs execution folders."""
    run_dirs = []
    for item in CONTAINER_IO_PATH.iterdir():
        if item.is_dir() and item.name.startswith("run_"):
            try:
                # Extract timestamp from folder name for sorting
                timestamp_str = item.name[4:]  # Remove "run_" prefix
                datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S_%f")
                run_dirs.append(item)
            except ValueError:
                # Skip folders that don't match our timestamp format
                continue

    # Sort by name (which sorts by timestamp due to format)
    run_dirs.sort(key=lambda x: x.name)

    # Remove oldest runs if we exceed max_runs
    while len(run_dirs) > max_runs:
        oldest = run_dirs.pop(0)
        shutil.rmtree(oldest, ignore_errors=True)


def execute(code: str, variables: Dict[str, Any], *, image: str = "codegen-agent-runner:py313") -> ExecutionResult:
    """Execute code inside a disposable Docker container with RO inputs and RW outputs.

    Returns ExecutionResult(stdout, stderr, returncode).
    """
    # Create timestamped run directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    run_root = CONTAINER_IO_PATH / f"run_{timestamp}"
    inputs = run_root / "inputs"
    outputs = run_root / "outputs"

    try:
        run_root.mkdir()
        inputs.mkdir()
        outputs.mkdir()

        # Write code
        (inputs / "code.py").write_text(code, encoding="utf-8")
        # Write prelude
        _write_prelude_to(inputs / "prelude.py")

        # Filter and serialize used variables
        filtered = _find_used_variables(code, variables)
        for name, val in filtered.items():
            _save_var(inputs / f"{name}.pkl", val)

        # Run container
        rt = DockerRuntime(image=image)
        rt.ensure_image()
        proc = rt.run(str(inputs), str(outputs))

        return ExecutionResult(stdout=proc.stdout, stderr=proc.stderr, returncode=proc.returncode)
    finally:
        # Always clean up old runs, regardless of success/failure
        _cleanup_old_runs(50)
