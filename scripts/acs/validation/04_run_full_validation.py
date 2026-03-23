from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from validation_utils import cli_error, load_settings, log


SCRIPT_ORDER = [
    "01_validate_stg_tables.py",
    "02_validate_int_tables.py",
    "03_validate_fact_tables.py",
]


def run_full_validation() -> None:
    settings = load_settings()
    current_dir = Path(__file__).resolve().parent

    log("Starting full ACS validation run")
    log(f"Project root: {settings.project_root}")
    log(f"Output folder: {settings.output_root}")

    for script_name in SCRIPT_ORDER:
        script_path = current_dir / script_name
        if not script_path.exists():
            raise FileNotFoundError(f"Missing validation script: {script_path}")

        log(f"Running {script_name}")
        result = subprocess.run([sys.executable, str(script_path)], check=False)
        if result.returncode != 0:
            raise RuntimeError(f"Validation step failed: {script_name}")

    log("Completed full ACS validation run")


if __name__ == "__main__":
    try:
        run_full_validation()
    except Exception as exc:  # pragma: no cover
        cli_error(exc)
