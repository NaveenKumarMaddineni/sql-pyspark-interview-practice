DIFF_CTE = """
WITH src AS (
    SELECT DISTINCT order_id, amount FROM orders
),
tgt AS (   -- pretend this is the loaded target: 1005 is missing, 1010 has a wrong amount
    SELECT order_id, CASE WHEN order_id = 1010 THEN amount + 10 ELSE amount END AS amount
    FROM src
    WHERE order_id <> 1005
)"""

TOPIC = dict(
    title="11 · Data quality checks and reconciliation",
    intro="""
A pipeline must **prove** its data is right. These SQL checks are the same ones you put in a
data quality step (in Composer/Airflow, dbt tests, or Great Expectations).

Good checks return **a number you can compare**: 0 bad rows = pass.

- completeness: NULLs where there should be none
- uniqueness: duplicate keys
- referential integrity: child rows whose parent is missing (**orphans**)
- validity: values outside the allowed range/list
- **reconciliation**: source and target have the same row count and the same SUM
""",
    questions=[
        Q("NULL profile of a table",
          "For `orders`, in one row show `total_rows` and the number of NULLs in each column: "
          "`null_order_id`, `null_customer_id`, `null_order_date`, `null_amount`, `null_status`.",
          level="easy",
          sql="""
SELECT COUNT(*)                     AS total_rows,
       COUNT(*) - COUNT(order_id)    AS null_order_id,
       COUNT(*) - COUNT(customer_id) AS null_customer_id,
       COUNT(*) - COUNT(order_date)  AS null_order_date,
       COUNT(*) - COUNT(amount)      AS null_amount,
       COUNT(*) - COUNT(status)      AS null_status
FROM orders""",
          sdf="""
res = orders.select(F.count("*").alias("total_rows"),
                    *[(F.count("*") - F.count(c)).alias(f"null_{c}") for c in orders.columns])""",
          note="`COUNT(*) - COUNT(col)` = number of NULLs. One scan for all columns.",
          notes={"spark": "A list comprehension builds one expression per column: works for any table."}),

        Q("Is the key unique?",
          "Check if `order_id` is a unique key in `orders`. Show `dup_keys` = how many order_ids appear "
          "more than once, and `extra_rows` = how many rows too many.",
          level="easy",
          sql="""
SELECT COUNT(*)            AS dup_keys,
       SUM(copies - 1)     AS extra_rows
FROM (SELECT order_id, COUNT(*) AS copies
      FROM orders
      GROUP BY order_id
      HAVING COUNT(*) > 1) d""",
          sdf="""
d = orders.groupBy("order_id").count().filter("count > 1")
res = d.agg(F.count("*").alias("dup_keys"), F.sum(F.col("count") - 1).alias("extra_rows"))""",
          note="Pass = 0. Faster check: `COUNT(*) = COUNT(DISTINCT order_id)`."),

        Q("Orphan rows (referential integrity)",
          "Make one test-report query with UNION ALL. Rows: `check_name`, `bad_rows`:\n"
          "- `items_without_order`: order_items whose order_id is not in orders\n"
          "- `items_without_product`: order_items whose product_id is not in products\n"
          "- `employee_bad_dept`: employees whose dept_id is **not NULL** but not in departments\n"
          "- `employee_bad_manager`: employees whose manager_id is not NULL but not an emp_id",
          level="medium",
          sql="""
SELECT 'items_without_order' AS check_name, COUNT(*) AS bad_rows
FROM order_items i
WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.order_id = i.order_id)
UNION ALL
SELECT 'items_without_product', COUNT(*)
FROM order_items i
WHERE NOT EXISTS (SELECT 1 FROM products p WHERE p.product_id = i.product_id)
UNION ALL
SELECT 'employee_bad_dept', COUNT(*)
FROM employees e
WHERE e.dept_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM departments d WHERE d.dept_id = e.dept_id)
UNION ALL
SELECT 'employee_bad_manager', COUNT(*)
FROM employees e
WHERE e.manager_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM employees m WHERE m.emp_id = e.manager_id)""",
          sdf="""
def orphans(child, parent, ck, pk):
    c = child.filter(F.col(ck).isNotNull())
    return c.join(parent.select(F.col(pk).alias(ck)).distinct(), ck, "left_anti").count()

checks = [("items_without_order",   orphans(order_items, orders, "order_id", "order_id")),
          ("items_without_product", orphans(order_items, products, "product_id", "product_id")),
          ("employee_bad_dept",     orphans(employees, departments, "dept_id", "dept_id")),
          ("employee_bad_manager",  orphans(employees, employees, "manager_id", "emp_id"))]
res = spark.createDataFrame(checks, "check_name string, bad_rows long")""",
          note="A NULL foreign key is **not** an orphan (it means \"no parent\"). Data warehouses like "
               "BigQuery do **not enforce** foreign keys, so you must test them yourself.",
          notes={"bigquery": "In BigQuery a single query with UNION ALL scans each table once per branch. "
                             "Fine for small tables; for big ones use one pass with LEFT JOINs."}),

        Q("Validity rules",
          "Count rows of `orders` that break each rule. One row with: `negative_amount` (amount < 0), "
          "`huge_amount` (amount > 1000), `future_date` (order_date after 2025-12-31), "
          "`bad_status` (status not in completed / cancelled / pending / returned, or NULL).",
          level="medium",
          sql="""
SELECT SUM(CASE WHEN amount < 0 THEN 1 ELSE 0 END)                         AS negative_amount,
       SUM(CASE WHEN amount > 1000 THEN 1 ELSE 0 END)                      AS huge_amount,
       SUM(CASE WHEN order_date > DATE '2025-12-31' THEN 1 ELSE 0 END)     AS future_date,
       SUM(CASE WHEN status IS NULL
                  OR status NOT IN ('completed', 'cancelled', 'pending', 'returned')
                THEN 1 ELSE 0 END)                                          AS bad_status
FROM orders""",
          sdf="""
flag = lambda cond: F.sum(F.when(cond, 1).otherwise(0))
res = orders.select(
    flag(F.col("amount") < 0).alias("negative_amount"),
    flag(F.col("amount") > 1000).alias("huge_amount"),
    flag(F.col("order_date") > F.lit("2025-12-31").cast("date")).alias("future_date"),
    flag(F.col("status").isNull()
         | ~F.col("status").isin("completed", "cancelled", "pending", "returned")).alias("bad_status"))""",
          note="`status NOT IN (...)` is unknown for a NULL status, so NULL must be checked separately."),

        Q("Reconciliation: source vs target",
          "The target is a daily summary built from completed orders. Compare it with the source:\n"
          "`src_rows`, `tgt_rows` (number of orders in each), `src_amount`, `tgt_amount`, and `result` = "
          "`MATCH` if both pairs are equal, else `MISMATCH`.",
          level="medium",
          sql="""
WITH src AS (
    SELECT DISTINCT order_id, order_date, amount
    FROM orders
    WHERE status = 'completed'
),
tgt AS (   -- the "loaded" table
    SELECT order_date, COUNT(*) AS num_orders, SUM(amount) AS amount
    FROM src
    GROUP BY order_date
),
s AS (SELECT COUNT(*) AS n, SUM(amount) AS amt FROM src),
t AS (SELECT SUM(num_orders) AS n, SUM(amount) AS amt FROM tgt)
SELECT s.n AS src_rows, t.n AS tgt_rows, s.amt AS src_amount, t.amt AS tgt_amount,
       CASE WHEN s.n = t.n AND s.amt = t.amt THEN 'MATCH' ELSE 'MISMATCH' END AS result
FROM s CROSS JOIN t""",
          sdf="""
src = orders.filter("status = 'completed'").select("order_id", "order_date", "amount").distinct()
tgt = src.groupBy("order_date").agg(F.count("*").alias("num_orders"), F.sum("amount").alias("amount"))
s = src.agg(F.count("*").alias("src_rows"), F.sum("amount").alias("src_amount"))
t = tgt.agg(F.sum("num_orders").alias("tgt_rows"), F.sum("amount").alias("tgt_amount"))
res = (s.crossJoin(t)
       .withColumn("result", F.when((F.col("src_rows") == F.col("tgt_rows"))
                                    & (F.col("src_amount") == F.col("tgt_amount")), "MATCH")
                              .otherwise("MISMATCH"))
       .select("src_rows", "tgt_rows", "src_amount", "tgt_amount", "result"))""",
          note="Count + sum reconciliation catches lost rows, duplicated rows and changed amounts. "
               "In a pipeline, write the result to an audit table and **fail the job** on MISMATCH."),

        Q("Row-level differences between source and target",
          "Using the `src` and `tgt` CTEs given in the answer cell (tgt is missing order 1005 and has a wrong "
          "amount for 1010), list `order_id` and `issue` = `missing_in_target`, `missing_in_source` or "
          "`amount_differs`. Careful: some amounts are NULL on both sides: that is **not** a difference.",
          level="hard",
          hint="FULL OUTER JOIN on order_id. Then compare amounts with a NULL-safe test.",
          sql=DIFF_CTE + """
SELECT COALESCE(s.order_id, t.order_id) AS order_id,
       CASE WHEN t.order_id IS NULL THEN 'missing_in_target'
            WHEN s.order_id IS NULL THEN 'missing_in_source'
            ELSE 'amount_differs'
       END AS issue
FROM src s
FULL OUTER JOIN tgt t ON t.order_id = s.order_id
WHERE s.order_id IS NULL
   OR t.order_id IS NULL
   OR NOT (s.amount = t.amount OR (s.amount IS NULL AND t.amount IS NULL))
ORDER BY order_id""",
          sdf="""
src = orders.select("order_id", "amount").distinct()
tgt = (src.filter("order_id <> 1005")
          .withColumn("amount", F.when(F.col("order_id") == 1010, F.col("amount") + 10)
                                 .otherwise(F.col("amount"))))
j = (src.withColumn("in_src", F.lit(1)).alias("s")
     .join(tgt.withColumn("in_tgt", F.lit(1)).alias("t"), "order_id", "full"))
res = (j.withColumn("issue",
            F.when(F.col("in_tgt").isNull(), "missing_in_target")
             .when(F.col("in_src").isNull(), "missing_in_source")
             .when(~F.col("s.amount").eqNullSafe(F.col("t.amount")), "amount_differs"))
        .filter(F.col("issue").isNotNull())
        .select("order_id", "issue").orderBy("order_id"))""",
          note="`s.amount <> t.amount` is unknown when one side is NULL, so a NULL vs 100 difference would be "
               "missed, and NULL vs NULL must count as equal. Engines have helpers: BigQuery "
               "`IS DISTINCT FROM`, Spark `<=>` / `eqNullSafe`, Oracle 23ai `IS DISTINCT FROM` "
               "(19c: `DECODE(a, b, 0, 1) = 1`).",
          notes={"spark": "A flag column on each side (`in_src`, `in_tgt`) tells \"row missing\" apart from \"amount is NULL\"."},
          ordered=True),

        Q("Freshness check",
          "How fresh is `orders`? Show `latest_order_date` and `days_behind` = days from it to **2025-12-31** "
          "(pretend that is today). Show `status` = `STALE` if more than 2 days behind, else `FRESH`.",
          level="easy",
          bq="""
SELECT MAX(order_date) AS latest_order_date,
       DATE_DIFF(DATE '2025-12-31', MAX(order_date), DAY) AS days_behind,
       CASE WHEN DATE_DIFF(DATE '2025-12-31', MAX(order_date), DAY) > 2 THEN 'STALE' ELSE 'FRESH' END AS status
FROM shield.orders""",
          ora="""
SELECT MAX(order_date) AS latest_order_date,
       DATE '2025-12-31' - MAX(order_date) AS days_behind,
       CASE WHEN DATE '2025-12-31' - MAX(order_date) > 2 THEN 'STALE' ELSE 'FRESH' END AS status
FROM orders""",
          ssql="""
SELECT MAX(order_date) AS latest_order_date,
       DATEDIFF(DATE '2025-12-31', MAX(order_date)) AS days_behind,
       CASE WHEN DATEDIFF(DATE '2025-12-31', MAX(order_date)) > 2 THEN 'STALE' ELSE 'FRESH' END AS status
FROM orders""",
          sdf="""
today = F.lit("2025-12-31").cast("date")
res = (orders.agg(F.max("order_date").alias("latest_order_date"))
       .withColumn("days_behind", F.datediff(today, "latest_order_date"))
       .withColumn("status", F.when(F.col("days_behind") > 2, "STALE").otherwise("FRESH")))""",
          note="In real checks use `CURRENT_DATE`. Freshness alerts catch an upstream job that silently stopped."),

        Q("Outliers",
          "Show `order_id`, `customer_id`, `amount` of different orders whose amount is more than "
          "**3 times the average** amount.",
          level="easy",
          sql="""
SELECT DISTINCT order_id, customer_id, amount
FROM orders
WHERE amount > 3 * (SELECT AVG(amount) FROM orders)""",
          sdf="""
avg_amt = orders.agg(F.avg("amount")).first()[0]
res = orders.filter(F.col("amount") > 3 * avg_amt).select("order_id", "customer_id", "amount").distinct()"""),

        Q("Values that fail a type cast",
          "Raw files bring text. From the 4 values `'12'`, `'x7'`, `'3.5'`, `NULL`, show `raw_value` and "
          "`as_number` = the number, or NULL when the text is **not a number**. The query must not fail.",
          level="medium",
          bq="""
SELECT raw_value, SAFE_CAST(raw_value AS NUMERIC) AS as_number
FROM UNNEST(['12', 'x7', '3.5', NULL]) AS raw_value""",
          ora="""
SELECT raw_value, TO_NUMBER(raw_value DEFAULT NULL ON CONVERSION ERROR) AS as_number
FROM (SELECT '12' AS raw_value FROM dual UNION ALL
      SELECT 'x7' FROM dual UNION ALL
      SELECT '3.5' FROM dual UNION ALL
      SELECT NULL FROM dual)""",
          ssql="""
SELECT raw_value, TRY_CAST(raw_value AS DECIMAL(10,2)) AS as_number
FROM VALUES ('12'), ('x7'), ('3.5'), (NULL) AS t(raw_value)""",
          sdf="""
raw_df = spark.createDataFrame([("12",), ("x7",), ("3.5",), (None,)], "raw_value string")
res = raw_df.withColumn("as_number", F.col("raw_value").try_cast("decimal(10,2)"))""",
          note="A bad row should not stop the whole load. Load it as NULL **and** count/route bad rows to an "
               "error table, so they are not silently lost.",
          notes={"bigquery": "`SAFE_CAST` returns NULL instead of an error. Also `SAFE.` prefix on functions, "
                             "e.g. `SAFE.PARSE_DATE(...)`.",
                 "oracle": "`DEFAULT ... ON CONVERSION ERROR` exists from Oracle 12.2. `VALIDATE_CONVERSION(raw_value AS NUMBER)` returns 1/0.",
                 "spark": "With ANSI mode on (default in Spark 4), a plain CAST of bad text **fails**. Use `TRY_CAST`."}),
    ],
)

# "How it works" text for each question, in order (shown in the solutions).
HOW = [
    "For each column, `COUNT(*) − COUNT(col)` = NULLs. All columns in one SELECT = one table scan.",
    "Group by `order_id` and keep keys with more than 1 row. Count those keys, and sum `copies − 1` for the extra rows.",
    "One small query per rule, each `COUNT(*)` of rows with no parent (NOT EXISTS), stacked with UNION ALL into a report.",
    "One `SUM(CASE WHEN rule broken THEN 1 ELSE 0 END)` per rule, all in one scan.",
    "Count and sum the source; count and sum the target; compare both pairs in a CASE → MATCH / MISMATCH.",
    "FULL OUTER JOIN source and target on the key. Missing side = missing row. Both present but amounts not equal (NULL-safe) = amount differs.",
    "Take `MAX(order_date)`, compute days to 'today', and label STALE when the gap is too big.",
    "Compute the overall average in a subquery and keep orders above 3 × that average.",
    "A safe cast returns NULL instead of an error for bad text: `SAFE_CAST`, `TO_NUMBER(... DEFAULT NULL ON CONVERSION ERROR)`, `TRY_CAST`.",
]
