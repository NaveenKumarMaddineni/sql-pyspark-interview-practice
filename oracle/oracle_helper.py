"""
oracle_helper.py - run Oracle queries from Python or a Jupyter notebook.

Settings come from the .env file in the repo root (see .env.example):
  ORACLE_DSN   e.g. localhost:1521/FREEPDB1   (host:port/service_name)
  ORACLE_USER  e.g. shield
  password: ORACLE_PASSWORD, or ORACLE_KEYCHAIN_ITEM (macOS Keychain),
            or you are asked to type it.

In a notebook:

    %load_ext oracle_helper

    %%oracle
    SELECT * FROM employees FETCH FIRST 5 ROWS ONLY

Notes:
  - A ";" at the end is removed for you (the Python driver does not want it).
  - One statement per cell. A PL/SQL block (BEGIN ... END;) also works.
"""
import getpass
import subprocess
import sys
from pathlib import Path

import oracledb
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import settings  # noqa: E402

DSN = settings.get("ORACLE_DSN", "localhost:1521/FREEPDB1")
USER = settings.get("ORACLE_USER", "shield")
_conn = None
_typed_password = None


def _password():
    global _typed_password
    if settings.get("ORACLE_PASSWORD"):
        return settings.get("ORACLE_PASSWORD")
    item = settings.get("ORACLE_KEYCHAIN_ITEM")
    if item:
        return subprocess.run(
            ["security", "find-generic-password", "-s", item, "-a", item, "-w"],
            capture_output=True, text=True, check=True,
        ).stdout.strip()
    if _typed_password is None:
        _typed_password = getpass.getpass(f"Oracle password for {USER}: ")
    return _typed_password


def connect():
    """Open (or reuse) one connection to Oracle."""
    global _conn
    if _conn is None or not _conn.is_healthy():
        _conn = oracledb.connect(user=USER, password=_password(), dsn=DSN)
    return _conn


def run_oracle(query):
    """Run one SQL statement. SELECT returns a pandas DataFrame."""
    query = query.strip()
    is_plsql = query.upper().startswith(("BEGIN", "DECLARE"))
    if not is_plsql:
        query = query.rstrip(";").strip()
    with connect().cursor() as cur:
        cur.execute(query)
        if cur.description is None:          # INSERT / UPDATE / CREATE ...
            connect().commit()
            return f"OK, {cur.rowcount} row(s) affected" if cur.rowcount > 0 else "OK"
        cols = [c[0].lower() for c in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


def load_ipython_extension(ipython):
    """Lets you write %%oracle at the top of a notebook cell."""
    from IPython.core.magic import register_cell_magic

    @register_cell_magic
    def oracle(line, cell):
        return run_oracle(cell)
