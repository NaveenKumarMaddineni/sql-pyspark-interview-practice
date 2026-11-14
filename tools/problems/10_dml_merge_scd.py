def two(sql):
    """Same statement for BigQuery (with practice.) and Oracle. {d} = dataset prefix."""
    return sql.replace("{d}", "practice."), sql.replace("{d}", "")


def copy_of(table, src, bq_select=None, ora_select=None):
    sel = bq_select or f"SELECT * FROM practice.{src}"
    osel = ora_select or f"SELECT * FROM {src}"
    return {"bigquery": [f"CREATE OR REPLACE TABLE practice.{table} AS\n{sel}"],
            "oracle": [f"DROP TABLE IF EXISTS {table} PURGE", f"CREATE TABLE {table} AS\n{osel}"]}


def merge_setups(*setups):
    out = {}
    for s in setups:
        for k, v in s.items():
            out.setdefault(k, []).extend(v)
    return out


CUST = copy_of("prac_customers", "customers")

UPDATES = {
    "bigquery": ["""
CREATE OR REPLACE TABLE practice.prac_customer_updates (
    customer_id INT64, name STRING, city STRING, signup_date DATE)""", """
INSERT INTO practice.prac_customer_updates VALUES
    (1,  'Aarav Nguyen',    'Denver',  DATE '2023-01-26'),
    (2,  'Priya Silva-Rao', 'Seattle', DATE '2023-09-08'),
    (41, 'Nina Patel',      'Boston',  DATE '2024-12-05'),
    (42, 'Omar Haddad',     'Austin',  DATE '2024-12-06')"""],
    "oracle": ["DROP TABLE IF EXISTS prac_customer_updates PURGE", """
CREATE TABLE prac_customer_updates (
    customer_id NUMBER(10), name VARCHAR2(100), city VARCHAR2(50), signup_date DATE)""", """
INSERT INTO prac_customer_updates
SELECT 1,  'Aarav Nguyen',    'Denver',  DATE '2023-01-26' FROM dual UNION ALL
SELECT 2,  'Priya Silva-Rao', 'Seattle', DATE '2023-09-08' FROM dual UNION ALL
SELECT 41, 'Nina Patel',      'Boston',  DATE '2024-12-05' FROM dual UNION ALL
SELECT 42, 'Omar Haddad',     'Austin',  DATE '2024-12-06' FROM dual"""],
}
UPDATES_DF = """
updates = spark.createDataFrame(
    [(1, "Aarav Nguyen", "Denver", "2023-01-26"), (2, "Priya Silva-Rao", "Seattle", "2023-09-08"),
     (41, "Nina Patel", "Boston", "2024-12-05"), (42, "Omar Haddad", "Austin", "2024-12-06")],
    "customer_id int, name string, city string, signup_date string"
).withColumn("signup_date", F.to_date("signup_date"))
"""

DIM = copy_of(
    "prac_dim_customer", "customers",
    bq_select="SELECT customer_id, city, signup_date AS valid_from,\n"
              "       DATE '9999-12-31' AS valid_to, 'Y' AS is_current\nFROM practice.customers",
    ora_select="SELECT customer_id, city, signup_date AS valid_from,\n"
               "       DATE '9999-12-31' AS valid_to, CAST('Y' AS VARCHAR2(1)) AS is_current\nFROM customers")
CHANGES = {
    "bigquery": ["""
CREATE OR REPLACE TABLE practice.prac_city_changes AS
SELECT 1 AS customer_id, 'Denver' AS city UNION ALL
SELECT 3, 'Phoenix' UNION ALL      -- same city as now: NOT a change
SELECT 13, 'Austin'                -- old city is NULL"""],
    "oracle": ["DROP TABLE IF EXISTS prac_city_changes PURGE", """
CREATE TABLE prac_city_changes (customer_id NUMBER(10), city VARCHAR2(50))""", """
INSERT INTO prac_city_changes
SELECT 1, 'Denver' FROM dual UNION ALL
SELECT 3, 'Phoenix' FROM dual UNION ALL      -- same city as now: NOT a change
SELECT 13, 'Austin' FROM dual                -- old city is NULL"""],
}

SCD_CLOSE = """
UPDATE {d}prac_dim_customer d
SET valid_to = DATE '2025-01-14',
    is_current = 'N'
WHERE d.is_current = 'Y'
  AND EXISTS (SELECT 1
              FROM {d}prac_city_changes s
              WHERE s.customer_id = d.customer_id
                AND COALESCE(s.city, '~') <> COALESCE(d.city, '~'))"""
SCD_INSERT = """
INSERT INTO {d}prac_dim_customer (customer_id, city, valid_from, valid_to, is_current)
SELECT s.customer_id, s.city, DATE '2025-01-15', DATE '9999-12-31', 'Y'
FROM {d}prac_city_changes s
WHERE NOT EXISTS (SELECT 1
                  FROM {d}prac_dim_customer d
                  WHERE d.customer_id = s.customer_id
                    AND d.is_current = 'Y')"""
SCD_SHOW = """
SELECT customer_id, city, valid_from, valid_to, is_current
FROM {d}prac_dim_customer
WHERE customer_id IN (1, 3, 13)
ORDER BY customer_id, valid_from"""

TOPIC = dict(
    title="10 · Changing data: INSERT, UPDATE, MERGE, SCD Type 2, incremental loads",
    nocache=True,
    engines=["bigquery", "oracle", "spark"],
    intro="""
**DML** (Data Manipulation Language) changes rows: `INSERT`, `UPDATE`, `DELETE`, `MERGE`.
**DDL** (Data Definition Language) changes tables: `CREATE`, `ALTER`, `DROP`.

This notebook works on **copies** named `prac_*`, so the 7 practice tables never change.
Each question has a setup cell that rebuilds its copy: run it first, and again to start over.

Data engineering words used here:
- **upsert** = update the row if the key exists, insert it if not (`MERGE`)
- **SCD Type 2** (slowly changing dimension) = keep the **history**: close the old row, add a new current row
- **incremental load** = load only the new rows since last time (using a **watermark**, e.g. the max date already loaded)
- **idempotent** = running the job twice gives the same result as running it once
""",
    questions=[
        Q("INSERT a row",
          "Add customer **41**, `Test User`, city `Austin`, signup `2024-12-01` to `prac_customers`. "
          "Then show `num_customers` (the row count).",
          level="easy",
          setup=CUST,
          bq=["""
INSERT INTO practice.prac_customers (customer_id, name, city, signup_date)
VALUES (41, 'Test User', 'Austin', DATE '2024-12-01')""",
              "SELECT COUNT(*) AS num_customers FROM practice.prac_customers"],
          ora=["""
INSERT INTO prac_customers (customer_id, name, city, signup_date)
VALUES (41, 'Test User', 'Austin', DATE '2024-12-01')""",
               "SELECT COUNT(*) AS num_customers FROM prac_customers"],
          ssql="SKIP",
          sdf="""
new_row = spark.createDataFrame([(41, "Test User", "Austin", "2024-12-01")],
                                "customer_id int, name string, city string, signup_date string") \\
               .withColumn("signup_date", F.to_date("signup_date"))
prac_customers = customers.unionByName(new_row)
res = prac_customers.agg(F.count("*").alias("num_customers"))""",
          note="Always list the columns in INSERT. If someone adds a column later, your insert still works.",
          notes={"spark": "DataFrames are **immutable** (they never change). You make a new DataFrame. "
                          "To change stored data you write files again, or use a table format such as "
                          "Delta Lake / Apache Iceberg that supports INSERT/UPDATE/MERGE.",
                 "oracle": "The notebook helper commits after each statement. In SQL*Plus or SQL Developer "
                           "you must run `COMMIT;` yourself (or `ROLLBACK;` to undo)."}),

        Q("UPDATE with a condition",
          "In `prac_customers`, set `city` to `Unknown` where it is NULL. "
          "Then show `customer_id`, `name`, `city` of the changed rows.",
          level="easy",
          setup=CUST,
          bq=["UPDATE practice.prac_customers SET city = 'Unknown' WHERE city IS NULL",
              "SELECT customer_id, name, city FROM practice.prac_customers WHERE city = 'Unknown'"],
          ora=["UPDATE prac_customers SET city = 'Unknown' WHERE city IS NULL",
               "SELECT customer_id, name, city FROM prac_customers WHERE city = 'Unknown'"],
          ssql="SKIP",
          sdf="""
prac_customers = customers.withColumn("city", F.coalesce("city", F.lit("Unknown")))
res = prac_customers.filter("city = 'Unknown'").select("customer_id", "name", "city")""",
          note="Write the WHERE first and test it as a SELECT. An UPDATE without WHERE changes **every** row.",
          notes={"bigquery": "BigQuery requires a WHERE in UPDATE and DELETE. To change all rows write `WHERE TRUE`."}),

        Q("DELETE with NOT EXISTS",
          "Delete from `prac_customers` everyone who **never ordered**. Then show `num_customers`.",
          level="easy",
          setup=CUST,
          bq=["""
DELETE FROM practice.prac_customers c
WHERE NOT EXISTS (SELECT 1 FROM practice.orders o WHERE o.customer_id = c.customer_id)""",
              "SELECT COUNT(*) AS num_customers FROM practice.prac_customers"],
          ora=["""
DELETE FROM prac_customers c
WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id)""",
               "SELECT COUNT(*) AS num_customers FROM prac_customers"],
          ssql="SKIP",
          sdf="""
prac_customers = customers.join(orders, "customer_id", "left_semi")
res = prac_customers.agg(F.count("*").alias("num_customers"))""",
          note="`TRUNCATE TABLE` removes **all** rows fast (no WHERE). In Oracle it cannot be rolled back."),

        Q("Upsert with MERGE",
          "`prac_customer_updates` has 4 rows: customers 1 and 2 changed, 41 and 42 are new. "
          "MERGE them into `prac_customers`: update `name` and `city` when the id exists, insert when not. "
          "Then show customers 1, 2, 41, 42.",
          level="medium",
          setup=merge_setups(CUST, UPDATES),
          bq=["""
MERGE practice.prac_customers t
USING practice.prac_customer_updates s
ON t.customer_id = s.customer_id
WHEN MATCHED THEN
  UPDATE SET name = s.name, city = s.city
WHEN NOT MATCHED THEN
  INSERT (customer_id, name, city, signup_date)
  VALUES (s.customer_id, s.name, s.city, s.signup_date)""", """
SELECT * FROM practice.prac_customers
WHERE customer_id IN (1, 2, 41, 42)
ORDER BY customer_id"""],
          ora=["""
MERGE INTO prac_customers t
USING prac_customer_updates s
ON (t.customer_id = s.customer_id)
WHEN MATCHED THEN
  UPDATE SET t.name = s.name, t.city = s.city
WHEN NOT MATCHED THEN
  INSERT (customer_id, name, city, signup_date)
  VALUES (s.customer_id, s.name, s.city, s.signup_date)""", """
SELECT * FROM prac_customers
WHERE customer_id IN (1, 2, 41, 42)
ORDER BY customer_id"""],
          ssql="SKIP",
          sdf=UPDATES_DF + """
# upsert = (old rows whose key is NOT in updates) + (all update rows)
prac_customers = customers.join(updates, "customer_id", "left_anti").unionByName(updates)
res = prac_customers.filter(F.col("customer_id").isin(1, 2, 41, 42)).orderBy("customer_id")""",
          note="MERGE fails (or does random things) if the **source has 2 rows for one key**. "
               "Dedupe the source first. MERGE is the main tool for upserts in data warehouses.",
          notes={"oracle": "Oracle needs brackets around the ON condition: `ON (t.id = s.id)`.",
                 "bigquery": "BigQuery MERGE can also `WHEN NOT MATCHED BY SOURCE THEN DELETE` "
                             "(delete target rows that are missing in the source).",
                 "spark": "With Delta Lake or Iceberg tables, Spark SQL has the same `MERGE INTO t USING s ON ...`."},
          ordered=True),

        Q("Idempotent reload of one slice",
          "`prac_orders` is a raw copy of `orders` (with duplicates). Reload **October 2025** from a clean "
          "source (`SELECT DISTINCT * FROM orders`): first DELETE the slice, then INSERT it again. "
          "Run both cells twice: the result must not change. Show `oct_rows` and `oct_orders` for October 2025.",
          level="medium",
          setup=copy_of("prac_orders", "orders"),
          bq=["""
DELETE FROM practice.prac_orders
WHERE order_date >= DATE '2025-10-01' AND order_date < DATE '2025-11-01'""", """
INSERT INTO practice.prac_orders (order_id, customer_id, order_date, amount, status)
SELECT DISTINCT order_id, customer_id, order_date, amount, status
FROM practice.orders
WHERE order_date >= DATE '2025-10-01' AND order_date < DATE '2025-11-01'""", """
SELECT COUNT(*) AS oct_rows, COUNT(DISTINCT order_id) AS oct_orders
FROM practice.prac_orders
WHERE order_date >= DATE '2025-10-01' AND order_date < DATE '2025-11-01'"""],
          ora=["""
DELETE FROM prac_orders
WHERE order_date >= DATE '2025-10-01' AND order_date < DATE '2025-11-01'""", """
INSERT INTO prac_orders (order_id, customer_id, order_date, amount, status)
SELECT DISTINCT order_id, customer_id, order_date, amount, status
FROM orders
WHERE order_date >= DATE '2025-10-01' AND order_date < DATE '2025-11-01'""", """
SELECT COUNT(*) AS oct_rows, COUNT(DISTINCT order_id) AS oct_orders
FROM prac_orders
WHERE order_date >= DATE '2025-10-01' AND order_date < DATE '2025-11-01'"""],
          ssql="SKIP",
          sdf="""
in_oct = (F.col("order_date") >= "2025-10-01") & (F.col("order_date") < "2025-11-01")
new_slice = orders.filter(in_oct).distinct()
prac_orders = orders.filter(~in_oct).unionByName(new_slice)      # replace the slice
res = prac_orders.filter(in_oct).agg(F.count("*").alias("oct_rows"),
                                     F.countDistinct("order_id").alias("oct_orders"))""",
          note="**Delete-then-insert by slice** (date, load id, ...) makes a job **idempotent**: a rerun "
               "replaces the slice instead of adding it twice. A plain INSERT on rerun = duplicates.",
          notes={"spark": "With files, the same idea is `partitionBy(\"order_date\")` + "
                          "`spark.sql.sources.partitionOverwriteMode=dynamic` (topic 14).",
                 "oracle": "In Oracle, run DELETE + INSERT in **one transaction** (commit once at the end), "
                           "so readers never see the slice half-loaded."}),

        Q("Incremental load with a watermark",
          "`prac_orders_target` holds clean orders up to 2025-06-30. Load only orders **newer than the "
          "latest date already in the target** (the watermark). Then show `num_rows` and `max_date` of the target.",
          level="medium",
          setup=copy_of("prac_orders_target", "orders",
                        bq_select="SELECT DISTINCT * FROM practice.orders WHERE order_date <= DATE '2025-06-30'",
                        ora_select="SELECT DISTINCT * FROM orders WHERE order_date <= DATE '2025-06-30'"),
          bq=["""
INSERT INTO practice.prac_orders_target (order_id, customer_id, order_date, amount, status)
SELECT DISTINCT order_id, customer_id, order_date, amount, status
FROM practice.orders
WHERE order_date > (SELECT MAX(order_date) FROM practice.prac_orders_target)""",
              "SELECT COUNT(*) AS num_rows, MAX(order_date) AS max_date FROM practice.prac_orders_target"],
          ora=["""
INSERT INTO prac_orders_target (order_id, customer_id, order_date, amount, status)
SELECT DISTINCT order_id, customer_id, order_date, amount, status
FROM orders
WHERE order_date > (SELECT MAX(order_date) FROM prac_orders_target)""",
               "SELECT COUNT(*) AS num_rows, MAX(order_date) AS max_date FROM prac_orders_target"],
          ssql="SKIP",
          sdf="""
target = orders.filter(F.col("order_date") <= "2025-06-30").distinct()
watermark = target.agg(F.max("order_date")).first()[0]
target = target.unionByName(orders.filter(F.col("order_date") > F.lit(watermark)).distinct())
res = target.agg(F.count("*").alias("num_rows"), F.max("order_date").alias("max_date"))""",
          note="Weak spot: a row that arrives **late** with an old date is never loaded. Real pipelines use "
               "a load timestamp / load id as the watermark, or reload the last N days (idempotent)."),

        Q("SCD Type 2: keep the history",
          "`prac_dim_customer` has one current row per customer (`valid_to = 9999-12-31`, `is_current = 'Y'`). "
          "`prac_city_changes` says: customer 1 → Denver, customer 3 → Phoenix (no change!), "
          "customer 13 → Austin (old city is NULL). Changes start on **2025-01-15**.\n\n"
          "1. **Close** the current row of customers whose city really changed: `valid_to = 2025-01-14`, `is_current = 'N'`.\n"
          "2. **Insert** a new current row for them from 2025-01-15.\n3. Show the rows of customers 1, 3, 13.",
          level="hard",
          setup=merge_setups(DIM, CHANGES),
          bq=[two(SCD_CLOSE)[0], two(SCD_INSERT)[0], two(SCD_SHOW)[0]],
          ora=[two(SCD_CLOSE)[1], two(SCD_INSERT)[1], two(SCD_SHOW)[1]],
          ssql="SKIP",
          sdf="""
dim = customers.select("customer_id", "city", F.col("signup_date").alias("valid_from"),
                       F.lit("9999-12-31").cast("date").alias("valid_to"), F.lit("Y").alias("is_current"))
changes = spark.createDataFrame([(1, "Denver"), (3, "Phoenix"), (13, "Austin")],
                                "customer_id int, city string")

cur = dim.alias("d").join(changes.alias("s"), "customer_id", "left")
changed = ~F.col("d.city").eqNullSafe(F.col("s.city")) & F.col("s.city").isNotNull()
closed = cur.select(
    "customer_id", "d.city", "d.valid_from",
    F.when(changed, F.lit("2025-01-14").cast("date")).otherwise(F.col("d.valid_to")).alias("valid_to"),
    F.when(changed, "N").otherwise(F.col("d.is_current")).alias("is_current"))
new_rows = (cur.filter(changed)
            .select("customer_id", F.col("s.city").alias("city"),
                    F.lit("2025-01-15").cast("date").alias("valid_from"),
                    F.lit("9999-12-31").cast("date").alias("valid_to"), F.lit("Y").alias("is_current")))
dim = closed.unionByName(new_rows)
res = dim.filter(F.col("customer_id").isin(1, 3, 13)).orderBy("customer_id", "valid_from")""",
          note="NULL trap: `s.city <> d.city` is **unknown** when the old city is NULL, so customer 13 would be "
               "missed. COALESCE both sides (or a null-safe compare). Customer 3 must **not** get a new row. "
               "Order matters: close first, then insert the rows that have no current version.",
          notes={"spark": "`eqNullSafe` (SQL `<=>`) is a NULL-safe equals: NULL <=> NULL is true."},
          ordered=True),

        Q("ALTER TABLE: add a flag column",
          "Add a column `is_active` (1 letter) to `prac_customers`. Set it to `Y` for customers with at "
          "least one order, `N` for the rest. Show `is_active` and `num_customers`.",
          level="medium",
          setup=CUST,
          bq=["ALTER TABLE practice.prac_customers ADD COLUMN is_active STRING", """
UPDATE practice.prac_customers c
SET is_active = CASE WHEN EXISTS (SELECT 1 FROM practice.orders o WHERE o.customer_id = c.customer_id)
                     THEN 'Y' ELSE 'N' END
WHERE TRUE""", """
SELECT is_active, COUNT(*) AS num_customers
FROM practice.prac_customers
GROUP BY is_active
ORDER BY is_active"""],
          ora=["ALTER TABLE prac_customers ADD (is_active VARCHAR2(1))", """
UPDATE prac_customers c
SET is_active = CASE WHEN EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id)
                     THEN 'Y' ELSE 'N' END""", """
SELECT is_active, COUNT(*) AS num_customers
FROM prac_customers
GROUP BY is_active
ORDER BY is_active"""],
          ssql="SKIP",
          sdf="""
buyers = orders.select("customer_id").distinct().withColumn("is_active", F.lit("Y"))
prac_customers = (customers.join(buyers, "customer_id", "left")
                  .withColumn("is_active", F.coalesce("is_active", F.lit("N"))))
res = prac_customers.groupBy("is_active").agg(F.count("*").alias("num_customers")).orderBy("is_active")""",
          note="This is a **soft delete** style flag: keep the row, mark it inactive.",
          ordered=True),
    ],
)

# "How it works" text for each question, in order (shown in the solutions).
HOW = [
    "`INSERT INTO table (columns) VALUES (...)` adds one row; the SELECT COUNT checks it is there.",
    "`UPDATE ... SET city = 'Unknown' WHERE city IS NULL` changes only the matching rows.",
    "`DELETE ... WHERE NOT EXISTS (order for this customer)` removes customers who never ordered.",
    "MERGE matches source to target on the key: `WHEN MATCHED` → UPDATE the row, `WHEN NOT MATCHED` → INSERT it. One statement does both.",
    "Delete the whole October slice, then insert the clean October rows. Running both again gives the same rows (idempotent).",
    "The watermark = `MAX(order_date)` already in the target. Insert only source rows newer than it.",
    "Step 1 closes the current row of customers whose city really changed (NULL-safe compare). Step 2 inserts a new current row for every change that now has no current row.",
    "`ALTER TABLE ... ADD` creates the empty column; one UPDATE fills it with CASE + EXISTS; GROUP BY checks the result.",
]
