S = "SKIP"

TOPIC = dict(
    title="12 · Performance: plans, partitions, cost",
    intro="""
Each engine is fast in a different way, so this notebook has **different questions per engine**.

- **BigQuery** charges by **bytes read**. Columnar storage (each column stored apart): read fewer columns = pay less.
  **Partitioning** (split a table by date) and **clustering** (sort inside partitions) let it skip data.
- **Oracle** uses an **execution plan** made by the cost-based optimizer: full table scan, index, join method.
  `EXPLAIN PLAN` shows it.
- **Spark** builds a plan too (`explain()`). Look for **pushed filters**, **partition filters**,
  **broadcast joins** and **exchanges** (shuffles: data moving between machines, the slow part).

The tables here are tiny, so speed differences are small. Learn to **read** the plans: that is what interviews ask.
""",
    questions=[
        # ---------------- BigQuery
        Q("What will this query cost? (dry run)",
          "Use `%%bigquery dry` (free) to see how many bytes these read: (1) `SELECT *` from orders, "
          "(2) only `order_id, amount`, (3) `SELECT *` with `LIMIT 5`.",
          level="easy", compare=False, ora=S, ssql=S,
          bq=["%%bigquery dry\nSELECT * FROM shield.orders",
              "%%bigquery dry\nSELECT order_id, amount FROM shield.orders",
              "%%bigquery dry\nSELECT * FROM shield.orders LIMIT 5"],
          note="Fewer columns = fewer bytes. **LIMIT does not reduce the bytes billed**: BigQuery still reads "
               "the whole columns. (Every query is billed at least 10 MB.)"),

        Q("Create a partitioned and clustered table",
          "Create `shield.prac_orders_part` from the clean orders, **partitioned by month** of order_date and "
          "**clustered by** customer_id. Then list its first partitions from `INFORMATION_SCHEMA.PARTITIONS`.",
          level="medium", compare=False, ora=S, ssql=S,
          bq=["""
CREATE OR REPLACE TABLE shield.prac_orders_part
PARTITION BY DATE_TRUNC(order_date, MONTH)
CLUSTER BY customer_id
AS SELECT DISTINCT * FROM shield.orders""", """
SELECT partition_id, total_rows
FROM shield.INFORMATION_SCHEMA.PARTITIONS
WHERE table_name = 'prac_orders_part'
ORDER BY partition_id
LIMIT 6"""],
          note="Partition by the column you **filter on most** (usually a date). Cluster by columns used in "
               "filters/joins with many values (customer_id). Monthly partitions keep the partition count "
               "low for small data; daily is common for big tables (max 10,000 partitions per table)."),

        Q("Partition pruning",
          "Dry-run 3 filters on `prac_orders_part` and compare the bytes: (1) no filter, "
          "(2) `order_date` between 2025-10-01 and 2025-10-31, (3) `EXTRACT(YEAR FROM order_date) = 2025`.",
          level="medium", compare=False, ora=S, ssql=S,
          bq=["%%bigquery dry\nSELECT order_id, amount FROM shield.prac_orders_part",
              "%%bigquery dry\nSELECT order_id, amount FROM shield.prac_orders_part\n"
              "WHERE order_date BETWEEN DATE '2025-10-01' AND DATE '2025-10-31'",
              "%%bigquery dry\nSELECT order_id, amount FROM shield.prac_orders_part\n"
              "WHERE EXTRACT(YEAR FROM order_date) = 2025"],
          note="**Partition pruning** = skipping partitions that cannot match. A plain range on the partition "
               "column prunes. Wrapping the column in a function may stop pruning. For safety on big tables "
               "set `require_partition_filter = TRUE` so a query without a date filter fails instead of costing a lot."),

        Q("Exact vs approximate distinct count",
          "Count different customers in `orders` two ways: `COUNT(DISTINCT customer_id)` and "
          "`APPROX_COUNT_DISTINCT(customer_id)`.",
          level="easy", compare=False, ora=S, ssql=S,
          bq="""
SELECT COUNT(DISTINCT customer_id)        AS exact_customers,
       APPROX_COUNT_DISTINCT(customer_id) AS approx_customers
FROM shield.orders""",
          note="On billions of rows the approximate version (HyperLogLog++) is much faster and uses little "
               "memory, with ~1% error. Same idea: Oracle `APPROX_COUNT_DISTINCT`, Spark `approx_count_distinct`."),

        Q("What did my queries cost?",
          "List your last 5 query jobs with `bytes_billed` from `INFORMATION_SCHEMA.JOBS_BY_USER`.\n\n"
          "Use **your dataset's region** in the name, e.g. `region-us`, `region-eu`, `region-europe-west2`.",
          level="medium", compare=False, ora=S, ssql=S,
          bq="""
SELECT creation_time, statement_type, total_bytes_billed AS bytes_billed,
       SUBSTR(query, 1, 60) AS query_start
FROM `region-europe-west2`.INFORMATION_SCHEMA.JOBS_BY_USER
WHERE job_type = 'QUERY'
ORDER BY creation_time DESC
LIMIT 5""",
          note="Watch this when you learn: it shows which queries were expensive. Cached results cost 0."),

        # ---------------- Oracle
        Q("Read an execution plan",
          "Show the plan Oracle picks for: employees joined to departments, with dept_name = 'Sales'. "
          "Use `EXPLAIN PLAN FOR ...`, then `DBMS_XPLAN.DISPLAY`.",
          level="medium", compare=False, bq=S, ssql=S,
          ora=["""
EXPLAIN PLAN FOR
SELECT e.name, d.dept_name
FROM employees e
JOIN departments d ON d.dept_id = e.dept_id
WHERE d.dept_name = 'Sales'""",
               "SELECT plan_table_output FROM TABLE(DBMS_XPLAN.DISPLAY(format => 'BASIC +ROWS +COST'))"],
          note="Read the plan from the **most indented** line up. `TABLE ACCESS FULL` = read the whole table. "
               "`INDEX UNIQUE SCAN` = use the primary key index. `HASH JOIN` / `NESTED LOOPS` = join methods. "
               "Rows and cost are the optimizer's **estimates**."),

        Q("Index and statistics",
          "Copy orders to `prac_orders`, create an **index** on customer_id, gather **statistics**, "
          "then explain `SELECT * FROM prac_orders WHERE customer_id = 7`.",
          level="medium", compare=False, bq=S, ssql=S,
          ora=["DROP TABLE IF EXISTS prac_orders PURGE",
               "CREATE TABLE prac_orders AS SELECT * FROM orders",
               "CREATE INDEX prac_orders_cust_ix ON prac_orders (customer_id)",
               "BEGIN DBMS_STATS.GATHER_TABLE_STATS(USER, 'PRAC_ORDERS'); END;",
               "EXPLAIN PLAN FOR SELECT * FROM prac_orders WHERE customer_id = 7",
               "SELECT plan_table_output FROM TABLE(DBMS_XPLAN.DISPLAY(format => 'BASIC +ROWS'))"],
          note="**Statistics** (row counts, value spread) let the optimizer guess well. An index helps when a "
               "query returns **few** rows. On a tiny table a full scan can still be cheaper. "
               "An index is not used when you wrap the column in a function (e.g. `TRUNC(order_date)`), "
               "unless you create a function-based index."),

        Q("Oracle partitioned table",
          "Create `prac_orders_part` with **interval partitioning** by month on order_date, from the clean orders. "
          "Count its partitions, then explain a query for October 2025 only.",
          level="hard", compare=False, bq=S, ssql=S,
          ora=["DROP TABLE IF EXISTS prac_orders_part PURGE", """
CREATE TABLE prac_orders_part
PARTITION BY RANGE (order_date) INTERVAL (NUMTOYMINTERVAL(1, 'MONTH'))
  (PARTITION p_old VALUES LESS THAN (DATE '2023-01-01'))
AS SELECT DISTINCT * FROM orders""",
               "SELECT COUNT(*) AS num_partitions FROM user_tab_partitions WHERE table_name = 'PRAC_ORDERS_PART'",
               """
EXPLAIN PLAN FOR
SELECT * FROM prac_orders_part
WHERE order_date >= DATE '2025-10-01' AND order_date < DATE '2025-11-01'""",
               "SELECT plan_table_output FROM TABLE(DBMS_XPLAN.DISPLAY(format => 'BASIC +PARTITION'))"],
          note="**Interval partitioning** creates a new monthly partition by itself when data arrives. "
               "`PARTITION RANGE SINGLE` with Pstart = Pstop means only one partition is read (pruning)."),

        # ---------------- Spark
        Q("Read a Spark plan: pushed filters",
          "Write `orders` to Parquet (a columnar file format) in `tmp/`, read it back, filter `status = 'completed'`, "
          "select 2 columns, and call `explain()`. Find **PushedFilters** and **ReadSchema** in the plan.",
          level="medium", compare=False, bq=S, ora=S, ssql=S,
          sdf="""
path = str(ROOT / "tmp" / "orders_parquet")
orders.write.mode("overwrite").parquet(path)
df = spark.read.parquet(path).filter(F.col("status") == "completed").select("order_id", "amount")
df.explain()""",
          note="**Predicate pushdown**: the filter is sent to the file reader, so it skips data early. "
               "**Column pruning**: ReadSchema has only the columns you need. Both also happen with the "
               "BigQuery connector for Spark (that is why you select columns before reading)."),

        Q("Broadcast join",
          "Join `order_items` to `products` with a **broadcast** hint and `explain()` it. "
          "Find `BroadcastHashJoin`. Then do the same in Spark SQL with `/*+ BROADCAST(p) */`.",
          level="medium", compare=False, bq=S, ora=S,
          ssql="""
SELECT /*+ BROADCAST(p) */ i.order_id, p.product_name, i.quantity
FROM order_items i
JOIN products p ON p.product_id = i.product_id
LIMIT 5""",
          sdf="""
order_items.join(F.broadcast(products), "product_id").explain()""",
          note="**Broadcast join**: the small table is copied to every executor, so the big table does **not** "
               "need a shuffle. Spark does it by itself under `spark.sql.autoBroadcastJoinThreshold` (10 MB). "
               "Without it you see `SortMergeJoin` with two `Exchange` steps."),

        Q("Partition pruning on files",
          "Write `orders` as Parquet **partitioned by** `order_year` into `tmp/orders_by_year`. "
          "Read it back, filter `order_year = 2025`, explain. Find **PartitionFilters**.",
          level="medium", compare=False, bq=S, ora=S, ssql=S,
          sdf="""
path = str(ROOT / "tmp" / "orders_by_year")
(orders.withColumn("order_year", F.year("order_date"))
       .write.mode("overwrite").partitionBy("order_year").parquet(path))
df = spark.read.parquet(path).filter("order_year = 2025")
df.explain()
print(sorted(p.name for p in (ROOT / "tmp" / "orders_by_year").iterdir() if p.is_dir()))""",
          note="`partitionBy` makes one folder per value (`order_year=2025/`). A filter on that column "
               "reads only the matching folders. Pick a column with **few** values (dates, not ids), or you "
               "get millions of tiny files."),
    ],
)

# "How it works" text for each question, in order (shown in the solutions).
HOW = [
    "A dry run plans the query without running it and reports the bytes it would read. Compare the numbers.",
    "CREATE TABLE ... PARTITION BY (month) CLUSTER BY (customer_id) AS SELECT ...; INFORMATION_SCHEMA.PARTITIONS lists the partitions and their rows.",
    "Dry-run the same query with different filters. Fewer bytes = BigQuery skipped partitions.",
    "Run both counts side by side; on small data they match, on big data the approximate one is much cheaper.",
    "`INFORMATION_SCHEMA.JOBS_BY_USER` keeps your query history with the bytes billed for each job.",
    "`EXPLAIN PLAN FOR` saves the plan into PLAN_TABLE; `DBMS_XPLAN.DISPLAY` prints it as text.",
    "Copy the table, add an index, gather statistics, then explain the query and see if the plan uses the index.",
    "Interval partitioning makes one partition per month; the plan for a one-month filter shows only that partition is read.",
    "Write Parquet, read it back with a filter + select, and `explain()` shows the filter and columns pushed into the file scan.",
    "`F.broadcast(products)` (or the SQL hint) tells Spark to copy the small table to all executors; the plan shows BroadcastHashJoin.",
    "partitionBy writes one folder per year; filtering on the year makes Spark read only that folder (PartitionFilters).",
]
