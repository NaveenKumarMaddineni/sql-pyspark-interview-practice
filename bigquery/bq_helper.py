"""
bq_helper.py - run BigQuery queries from Python or a Jupyter notebook.

Settings come from the .env file in the repo root (see .env.example):
  BQ_PROJECT, BQ_DATASET, BQ_LOCATION, and optional BQ_GCLOUD_CONFIG.

Login:
  - BQ_GCLOUD_CONFIG set   -> uses that gcloud configuration (your default one is not changed)
  - not set                -> uses "application default" login:
                              gcloud auth application-default login

In a notebook:

    %load_ext bq_helper

    %%bigquery
    SELECT * FROM practice.employees LIMIT 5

    %%bigquery dry
    SELECT * FROM practice.orders        -- only shows how much data it WOULD read (free)

Every query prints how much data it read (BigQuery charges by bytes read).
"""
import os
import shutil
import subprocess
import sys
import time
import warnings
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import settings  # noqa: E402

from google.cloud import bigquery  # noqa: E402

# Harmless notices about faster download options (not needed for small data).
warnings.filterwarnings("ignore", message=".*DataFrames via google-cloud-bigquery.*")
warnings.filterwarnings("ignore", message=".*BigQuery Storage module not found.*")

PROJECT = settings.get("BQ_PROJECT")
DATASET = settings.get("BQ_DATASET", "practice")
LOCATION = settings.get("BQ_LOCATION", "US")
GCLOUD_CONFIG = settings.get("BQ_GCLOUD_CONFIG")

_client, _made_at = None, 0.0


def _gcloud():
    return shutil.which("gcloud") or os.path.expanduser(
        "~/google-cloud-sdk/bin/gcloud")


def client():
    """BigQuery client. With BQ_GCLOUD_CONFIG it gets a fresh token every 45 minutes."""
    global _client, _made_at
    if not PROJECT:
        raise RuntimeError("BQ_PROJECT is not set. Copy .env.example to .env and fill it in.")
    if _client is None or (GCLOUD_CONFIG and time.time() - _made_at > 45 * 60):
        credentials = None
        if GCLOUD_CONFIG:
            from google.oauth2.credentials import Credentials
            token = subprocess.run(
                [_gcloud(), "auth", "print-access-token", f"--configuration={GCLOUD_CONFIG}"],
                capture_output=True, text=True, check=True,
            ).stdout.strip()
            credentials = Credentials(token)
        _client = bigquery.Client(project=PROJECT, location=LOCATION, credentials=credentials)
        _made_at = time.time()
    return _client


def run_bq(query, dry_run=False, quiet=False):
    """Run SQL in BigQuery. SELECT returns a pandas DataFrame.
    dry_run=True only shows how much data the query WOULD read (free)."""
    config = bigquery.QueryJobConfig(dry_run=dry_run, use_query_cache=not dry_run)
    job = client().query(query, job_config=config)
    if dry_run:
        print(f"Would read {_size(job.total_bytes_processed)}")
        return None
    rows = job.result()
    if not quiet:
        print(f"Read {_size(job.total_bytes_processed)}"
              + (" (from cache, free)" if job.cache_hit else ""))
    kind = job.statement_type or ""
    if kind in ("INSERT", "UPDATE", "DELETE", "MERGE"):
        return f"OK, {job.num_dml_affected_rows or 0} row(s) affected"
    if kind == "SELECT" or (kind == "SCRIPT" and rows.schema):
        return rows.to_dataframe()
    return f"OK ({kind.replace('_', ' ').lower()})" if kind else "OK"


def _size(n):
    n = n or 0
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024:
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def load_ipython_extension(ipython):
    """Lets you write %%bigquery (or %%bigquery dry) at the top of a cell."""
    from IPython.core.magic import register_cell_magic

    @register_cell_magic
    def bigquery(line, cell):
        return run_bq(cell, dry_run=line.strip() == "dry")
