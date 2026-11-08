"""
load_tables.py - (re)build the 7 practice tables in local Oracle.
Drops them, creates them from create_tables.sql, and loads data/*.tsv.

Run from the repo root:  python3 oracle/load_tables.py
"""
import csv
import datetime as dt
import os
import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))
from oracle_helper import connect

HERE = Path(__file__).resolve().parent
DATA = HERE.parent / "data"
TABLES = ["departments", "employees", "customers", "orders",
          "products", "order_items", "user_logins"]


def convert(value, oracle_type):
    if value == "NULL":
        return None
    if oracle_type == "DB_TYPE_DATE":
        return dt.date.fromisoformat(value)
    if oracle_type == "DB_TYPE_NUMBER":
        return Decimal(value)
    return value


conn = connect()
cur = conn.cursor()

for t in TABLES:  # drop old tables, ignore "table does not exist"
    try:
        cur.execute(f"DROP TABLE {t} PURGE")
    except Exception:
        pass

for stmt in (HERE / "create_tables.sql").read_text().split(";"):
    lines = [l for l in stmt.splitlines() if not l.strip().startswith("--")]
    if "".join(lines).strip():
        cur.execute("\n".join(lines))

for t in TABLES:
    with open(DATA / f"{t}.tsv", newline="") as f:
        rows = list(csv.reader(f, delimiter="\t"))
    header, rows = rows[0], rows[1:]
    cur.execute(f"SELECT {', '.join(header)} FROM {t} WHERE 1 = 0")
    types = [d.type_code.name for d in cur.description]
    data = [[convert(v, ty) for v, ty in zip(r, types)] for r in rows]
    binds = ", ".join(f":{i + 1}" for i in range(len(header)))
    cur.executemany(f"INSERT INTO {t} ({', '.join(header)}) VALUES ({binds})", data)
    print(f"{t:12} {len(data):>4} rows")

conn.commit()
