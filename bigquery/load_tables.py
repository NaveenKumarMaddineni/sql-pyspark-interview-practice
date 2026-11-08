"""
load_tables.py - create the BigQuery dataset (if needed) and load the 7 practice tables
from data/*.tsv. Existing tables with the same names are replaced.

Settings come from .env (BQ_PROJECT, BQ_DATASET, BQ_LOCATION). Run from the repo root:

    python3 bigquery/load_tables.py

Load jobs are free in BigQuery; storing this small data costs almost nothing.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from google.cloud import bigquery  # noqa: E402

import bq_helper  # noqa: E402
from settings import DATA_DIR  # noqa: E402

S = bigquery.SchemaField
SCHEMAS = {
    "departments": [S("dept_id", "INT64"), S("dept_name", "STRING")],
    "employees":   [S("emp_id", "INT64"), S("name", "STRING"), S("dept_id", "INT64"),
                    S("manager_id", "INT64"), S("salary", "NUMERIC"), S("hire_date", "DATE")],
    "customers":   [S("customer_id", "INT64"), S("name", "STRING"), S("city", "STRING"),
                    S("signup_date", "DATE")],
    "orders":      [S("order_id", "INT64"), S("customer_id", "INT64"), S("order_date", "DATE"),
                    S("amount", "NUMERIC"), S("status", "STRING")],
    "products":    [S("product_id", "INT64"), S("product_name", "STRING"), S("category", "STRING"),
                    S("price", "NUMERIC")],
    "order_items": [S("order_id", "INT64"), S("product_id", "INT64"), S("quantity", "INT64")],
    "user_logins": [S("user_id", "INT64"), S("login_date", "DATE")],
}


def main():
    client = bq_helper.client()
    dataset_id = f"{bq_helper.PROJECT}.{bq_helper.DATASET}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = bq_helper.LOCATION
    client.create_dataset(dataset, exists_ok=True)
    print(f"dataset {dataset_id} ({bq_helper.LOCATION})")

    for name, schema in SCHEMAS.items():
        config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.CSV,
            field_delimiter="\t",
            skip_leading_rows=1,
            null_marker="NULL",
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        with open(DATA_DIR / f"{name}.tsv", "rb") as f:
            job = client.load_table_from_file(f, f"{dataset_id}.{name}", job_config=config)
        job.result()
        print(f"{name:12} {client.get_table(f'{dataset_id}.{name}').num_rows:>4} rows")


if __name__ == "__main__":
    main()
