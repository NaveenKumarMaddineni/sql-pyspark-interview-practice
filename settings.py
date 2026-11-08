"""
settings.py - reads your personal settings from the .env file in the repo root.

Values in real environment variables win over values in .env.
"""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"


def _load_env_file(path=ROOT / ".env"):
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.split(" #")[0].strip()          # drop comments
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


_load_env_file()


def get(key, default=None):
    return os.environ.get(key, default)
