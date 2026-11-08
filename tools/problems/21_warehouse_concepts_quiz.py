TOPIC = dict(
    title="21 · Data warehouse, modeling and BigQuery/Oracle concepts quiz",
    engines=["bigquery", "oracle"],
    intro="""
Theory that comes up in data engineering interviews: modeling, warehouse design, BigQuery internals, Oracle basics.
Answer each one **out loud** in 3–5 sentences, then compare with the solutions notebook.
""",
    questions=[
        Q("OLTP vs OLAP",
          "What is the difference between OLTP and OLAP systems? Give an example of each.",
          level="easy",
          answer="""
- **OLTP** (online transaction processing): many small reads/writes of single rows, strict consistency,
  normalized tables, row storage. Example: Oracle database behind a banking app.
- **OLAP** (online analytical processing): few big queries that scan and aggregate millions of rows,
  denormalized tables, **columnar** storage. Example: BigQuery data warehouse.
Pipelines usually move data from OLTP sources into OLAP for reporting."""),

        Q("Star schema: facts and dimensions",
          "What is a star schema? What is a fact table and a dimension table? What is the **grain**?",
          level="easy",
          answer="""
A **star schema** has one central **fact** table (measurements/events: order lines, payments) with foreign keys to
**dimension** tables (descriptions: customer, product, date).
The **grain** = what one fact row means ("one row per order line per day"). Decide it first; mixing grains
causes double counting.
A **snowflake** schema normalizes dimensions further (product → category table)."""),

        Q("Normalization vs denormalization",
          "What is normalization (1NF/2NF/3NF in short)? Why do warehouses often denormalize?",
          level="medium",
          answer="""
**Normalization** removes repeated data: 1NF = atomic values, no repeating groups; 2NF = every column depends on the
whole key; 3NF = no column depends on another non-key column. Good for OLTP: updates in one place.
**Denormalization** repeats data (wide tables, nested fields) to avoid joins. Warehouses do it because storage is
cheap and joins on huge tables are expensive. In BigQuery, nested/repeated fields (STRUCT/ARRAY) are a common way."""),

        Q("Slowly changing dimensions",
          "Explain SCD Type 1, 2 and 3.",
          level="easy",
          answer="""
- **Type 1**: overwrite the old value. No history. (Fix a typo.)
- **Type 2**: close the old row (`valid_to`, `is_current = 'N'`) and insert a new row. Full history; facts join to
  the version valid at their date. Most asked.
- **Type 3**: keep the previous value in an extra column (`previous_city`). Only one step of history.
(Type 0 = never change; Type 4 = separate history table; Type 6 = 1+2+3 combined.)"""),

        Q("Surrogate keys",
          "What is a surrogate key and why use it instead of the natural key?",
          level="medium",
          answer="""
A **surrogate key** is a meaningless id made by the warehouse (sequence, hash, UUID). A **natural key** comes from the
business (customer number).
Use surrogates because: SCD Type 2 needs several rows per natural key; natural keys can change or collide between
source systems; integer keys join fast. In BigQuery people often use `GENERATE_UUID()` or a hash
(`FARM_FINGERPRINT`) of the natural key + valid_from."""),

        Q("ETL vs ELT",
          "What is the difference between ETL and ELT? Which fits BigQuery?",
          level="easy",
          answer="""
**ETL**: extract, transform outside the warehouse (Spark, Informatica), then load the result.
**ELT**: extract, load raw data into the warehouse first, then transform **inside** it with SQL (dbt, scheduled queries).
BigQuery fits ELT well (cheap storage, huge SQL engine). Spark/Dataproc is still used for heavy logic, non-SQL work,
or when the output goes elsewhere (files, Oracle)."""),

        Q("How BigQuery works",
          "Describe BigQuery's architecture in a few sentences. What are slots?",
          level="medium",
          answer="""
BigQuery separates **storage** (Colossus, columnar format Capacitor) and **compute** (Dremel engine), linked by a
fast network (Jupiter). A query is split into a tree of stages; workers read only the needed columns.
A **slot** = a unit of compute (CPU + memory). On-demand pricing charges per **TB scanned** with a shared slot pool;
capacity pricing (editions) buys a number of slots. Data is shuffled between stages in memory."""),

        Q("Partitioning vs clustering in BigQuery",
          "When do you partition, when do you cluster, and how do they reduce cost?",
          level="medium",
          answer="""
**Partitioning** splits a table into parts by a DATE/TIMESTAMP column, ingestion time, or an integer range.
Queries filtering on it skip whole partitions → fewer bytes billed (known **before** running: dry run shows it).
Max 10,000 partitions. **Clustering** sorts data by up to 4 columns inside each partition; filters/joins on them read
fewer blocks (savings show only after running). Typical: partition by date, cluster by customer_id/status.
Use `require_partition_filter` to block expensive full scans."""),

        Q("BigQuery cost control",
          "List ways to keep BigQuery costs low.",
          level="easy",
          answer="""
- never `SELECT *`; select the columns you need (LIMIT does **not** reduce bytes)
- partition + cluster big tables; filter on the partition column
- **dry run** (`--dry_run`) before big queries; set **maximum bytes billed** on jobs
- use cached results; materialize repeated heavy logic into tables
- set budgets/alerts and per-user quotas; check `INFORMATION_SCHEMA.JOBS` for expensive queries
- table expiration for temp data; long-term storage is cheaper automatically after 90 days unchanged"""),

        Q("Nested and repeated fields",
          "What are STRUCT and ARRAY in BigQuery and why are they useful? How do you query them?",
          level="medium",
          answer="""
A **STRUCT** is a record inside a column; an **ARRAY** is a list inside a column (a "repeated" field). An order
row can hold its line items as `ARRAY<STRUCT<sku, qty>>`: no join needed and the data stays together.
Query with `UNNEST`: `SELECT o.order_id, li.sku FROM orders o, UNNEST(o.lines) AS li`. Build with `ARRAY_AGG` and
`STRUCT(...)`. It is BigQuery's way to denormalize without repeating parent rows."""),

        Q("Oracle: schema, user, tablespace, PDB",
          "In Oracle, what are a schema, a user, a tablespace and a PDB?",
          level="medium",
          answer="""
- **User** = an account that can log in. **Schema** = the set of objects (tables, views...) owned by that user:
  in Oracle user and schema have the same name.
- **Tablespace** = a logical storage area made of data files; tables are stored in a tablespace.
- **PDB** (pluggable database) = a database inside a container database (CDB). `FREEPDB1` is the PDB you connect to
  with a **service name**.
(In BigQuery the closest ideas are project → dataset → table.)"""),

        Q("Loading files into Oracle",
          "A pipe-delimited CSV file arrives every day. How can you load it into Oracle? How do you make reruns safe?",
          level="hard",
          answer="""
Options: **SQL*Loader** (`sqlldr` with a control file: fast, direct-path load), an **external table**
(`ORGANIZATION EXTERNAL` → query the file with SQL, then `INSERT ... SELECT` or MERGE), or a client program
(Python `oracledb` `executemany` in batches).

Safe reruns: load into a **staging** table first; check count and sum against the **control file**; then in one
transaction delete the slice (date + load id) from the target and insert it, or MERGE. Keep a load log table.
Watch date formats (`TO_DATE(..., 'YYYY-MM-DD')`), NULL vs empty string (Oracle treats '' as NULL), and character set."""),

        Q("Indexes",
          "What is an index? When does it help, and when does it hurt?",
          level="easy",
          answer="""
An index (usually a **B-tree**) is a sorted copy of some columns with pointers to the rows, like a book index.
It helps queries that return **few rows** (lookups by key, selective filters, joins on keys). It hurts write speed
(every INSERT/UPDATE also updates the index) and uses space; for queries reading a large part of the table a full
scan is faster. Functions on the column (`UPPER(col)`) stop index use unless you create a function-based index.
BigQuery has no classic indexes: it uses partitioning/clustering (and optional search indexes)."""),

        Q("ACID and transactions",
          "What does ACID mean? Does BigQuery support transactions?",
          level="medium",
          answer="""
**Atomicity** (all or nothing), **Consistency** (rules always hold), **Isolation** (parallel transactions don't see
each other's half work), **Durability** (committed data survives crashes). Oracle is fully ACID: changes are visible
to others only after `COMMIT`.
BigQuery: each DML statement is atomic, and multi-statement transactions exist (`BEGIN TRANSACTION ... COMMIT`)
with snapshot isolation, but it is not built for many small concurrent updates."""),

        Q("Window function vs GROUP BY",
          "When would you use a window function instead of GROUP BY?",
          level="easy",
          answer="""
GROUP BY **collapses** rows into one per group. A window function computes over a group but **keeps every row**.
Use a window when you need row-level detail plus a group value: rank within a group, running total, previous row
(LAG), % of group total, latest row per key (ROW_NUMBER). You can't use it in WHERE: filter in an outer query or with
QUALIFY."""),

        Q("Late-arriving data and backfills",
          "How do you handle data that arrives late, and how do you backfill a month of history?",
          level="hard",
          answer="""
Late data: use a **lookback** (re-process the last N days each run), or watermark on **load time** instead of event
time; keep loads idempotent (overwrite/MERGE by slice) so reprocessing is safe. For dimensions, late facts must join
to the dimension version valid at the event date (SCD2).

Backfill: run the same idempotent job for each date (Airflow `catchup`/backfill with a run_date parameter), limit
parallel runs to protect sources, reconcile each slice, and compare totals with the source at the end."""),
    ],
)
