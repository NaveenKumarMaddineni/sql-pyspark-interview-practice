# SQL Notes — BigQuery · Oracle · Spark

Open this when you are stuck. Every example was tested on your setup
(BigQuery, Oracle 26ai, Spark 4.1). Tables: see `tables_guide.md`.

**Contents**
1. How to solve any SQL problem
2. Order SQL runs in
3. JOINs
4. NULL rules
5. GROUP BY and aggregates
6. Window functions
7. Common problem patterns (with code)
8. Dates
9. Strings
10. BigQuery vs Oracle vs Spark (cheat sheet)
11. PySpark DataFrame API (SQL → code)
12. Mistakes checklist

---

## 1. How to solve any SQL problem

1. **Read the question 2 times.** Underline: what to show, per what, any rules (ties? NULLs?).
2. **Find the grain of the answer:** what does ONE row of the result mean?
   e.g. "one row per department" or "one row per employee".
3. **Find the tables** you need and how they join (see `tables_guide.md`).
4. **Look at the data first:** `SELECT * ... LIMIT 10`. Look for NULLs, duplicates, ties.
5. **Build step by step with CTEs.** Run each step before adding the next.
6. **Check the result:** row count makes sense? ties? NULL rows? duplicates?

```sql
WITH step1 AS (...),        -- join / clean
     step2 AS (...)         -- rank / aggregate
SELECT ... FROM step2 WHERE ...;
```

**Ask these questions (interviewers like them):**
- What if there is a **tie**? Show all or one?
- What about **NULL** values?
- Can there be **duplicate** rows?
- Groups with **zero** rows: show them with 0, or skip?

---

## 2. Order SQL runs in

You **write**: `SELECT → FROM → WHERE → GROUP BY → HAVING → ORDER BY`
SQL **runs**:

```
1. FROM / JOIN      get rows
2. WHERE            filter rows        (no aggregates, no window functions here)
3. GROUP BY         make groups
4. HAVING           filter groups      (aggregates allowed)
5. SELECT           calculate columns  (window functions run here)
6. QUALIFY          filter on window functions (BigQuery; Oracle 23ai+; NOT Spark)
7. DISTINCT
8. ORDER BY         (can use SELECT aliases)
9. LIMIT / FETCH FIRST
```

**Why this matters:**
- You **cannot** use a window function in `WHERE` → use a CTE/subquery (or `QUALIFY`).
- You **cannot** use `SUM(...)` in `WHERE` → use `HAVING`.
- A `SELECT` alias **cannot** be used in `WHERE` (it does not exist yet).

---

## 3. JOINs

| Join | Keeps |
|---|---|
| `INNER JOIN` | only rows that match on both sides |
| `LEFT JOIN` | all rows from left + matches (no match → NULLs) |
| `RIGHT JOIN` | all from right (just swap tables and use LEFT) |
| `FULL OUTER JOIN` | all rows from both sides |
| `CROSS JOIN` | every row × every row |
| Self join | table joined to itself (employee → manager) |
| Anti join | left rows with **no** match (customers who never ordered) |
| Semi join | left rows that **have** a match, each only once |

**Anti join — 3 ways:**
```sql
-- 1. LEFT JOIN + IS NULL  (works everywhere)
SELECT c.* FROM customers c
LEFT JOIN orders o ON o.customer_id = c.customer_id
WHERE o.customer_id IS NULL;

-- 2. NOT EXISTS  (safe with NULLs — use this one)
SELECT c.* FROM customers c
WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id);

-- 3. Spark only
SELECT c.* FROM customers c LEFT ANTI JOIN orders o ON o.customer_id = c.customer_id;
```

⚠️ **`NOT IN` + NULL = no rows.** Tested on our data:
```sql
-- Returns 0 rows! (employees.dept_id has a NULL)
SELECT * FROM departments WHERE dept_id NOT IN (SELECT dept_id FROM employees);
-- Returns Legal (correct)
SELECT * FROM departments d
WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.dept_id);
```

⚠️ **LEFT JOIN + WHERE on the right table = INNER JOIN.** Put the filter in `ON`:
```sql
-- WRONG: customers with no completed orders disappear
LEFT JOIN orders o ON o.customer_id = c.customer_id
WHERE o.status = 'completed'
-- RIGHT: keeps all customers
LEFT JOIN orders o ON o.customer_id = c.customer_id AND o.status = 'completed'
```

⚠️ **Join fan-out:** joining one-to-many **repeats** rows, so `SUM` gets too big.
Aggregate first, then join. Or check: row count before and after the join.

⚠️ **NULL never matches in a join** (`NULL = NULL` is not true).

---

## 4. NULL rules

- `NULL = NULL` → **unknown** (not true). Use `IS NULL` / `IS NOT NULL`.
- Any math with NULL → NULL: `5 + NULL = NULL`.
- `COUNT(*)` counts rows. `COUNT(col)` **skips NULLs**.
- `SUM`, `AVG`, `MIN`, `MAX` **skip NULLs**. `AVG` of (10, NULL, 20) = 15, not 10.
- `WHERE col <> 'x'` **also drops NULL rows**. Add `OR col IS NULL` if you need them.
- Replace NULL: `COALESCE(col, 0)` works everywhere.

| | BigQuery | Oracle | Spark |
|---|---|---|---|
| replace NULL | `IFNULL(a,b)`, `COALESCE` | `NVL(a,b)`, `COALESCE` | `nvl`, `ifnull`, `COALESCE` |
| NULL-safe equal | `a IS NOT DISTINCT FROM b` | `DECODE(a,b,1,0)=1` | `a <=> b` or `IS NOT DISTINCT FROM` |
| empty string `''` | not NULL | **is NULL!** | not NULL |
| NULLs in `ORDER BY x ASC` | **first** | **last** | **first** |

Control it: `ORDER BY x ASC NULLS LAST` (works in all 3).

---

## 5. GROUP BY and aggregates

```sql
SELECT dept_id, COUNT(*) AS n, AVG(salary) AS avg_sal
FROM employees
GROUP BY dept_id
HAVING COUNT(*) >= 5;          -- filter AFTER grouping
```
- Every column in `SELECT` must be in `GROUP BY` or inside an aggregate.
- `WHERE` filters rows before grouping; `HAVING` filters groups after.

**Conditional aggregation (count/sum only some rows):**
```sql
-- Works in all 3
SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END)  AS completed_cnt
SUM(CASE WHEN status = 'completed' THEN amount END)    AS completed_amt
-- Shorter
COUNTIF(status = 'completed')     -- BigQuery
COUNT_IF(status = 'completed')    -- Spark
```
⚠️ `SUM(salary > 100000)` → **error in BigQuery and Spark** (cannot SUM true/false).

**Group by alias / position:**
| | `GROUP BY 1` | `GROUP BY alias` |
|---|---|---|
| BigQuery | ✅ | ✅ |
| Spark | ✅ | ✅ |
| Oracle | ❌ | ✅ only 23ai+ (not 19c) — safest: repeat the expression |

**Useful aggregates:**
| Need | BigQuery | Oracle | Spark |
|---|---|---|---|
| name with max salary | `MAX_BY(name, salary)` | `MAX(name) KEEP (DENSE_RANK FIRST ORDER BY salary DESC)` | `max_by(name, salary)` |
| median | `PERCENTILE_CONT(x, 0.5) OVER ()` | `MEDIAN(x)` | `median(x)` |
| list in one string | `STRING_AGG(x, ',' ORDER BY x)` | `LISTAGG(x, ',') WITHIN GROUP (ORDER BY x)` | `string_agg(x, ',')` / `concat_ws(',', collect_list(x))` |
| count distinct | `COUNT(DISTINCT x)` | same | same |

---

## 6. Window functions

Window function = calculate over a group of rows, but **keep every row**.

```sql
FUNCTION() OVER (
    PARTITION BY group_col      -- restart for each group (optional)
    ORDER BY sort_col           -- order inside the group
    ROWS BETWEEN ... AND ...    -- which rows to use (optional)
)
```

### Ranking — the most asked difference
Salaries 300, 200, 200, 100:

| salary | ROW_NUMBER | RANK | DENSE_RANK |
|---|---|---|---|
| 300 | 1 | 1 | 1 |
| 200 | 2 | 2 | 2 |
| 200 | 3 | 2 | 2 |
| 100 | 4 | **4** | **3** |

- `ROW_NUMBER` → always unique. Use for **dedupe** and "exactly one per group".
- `RANK` → ties share; then **skips** numbers. Use for "top 1 **with ties**".
- `DENSE_RANK` → ties share; **no gaps**. Use for "**Nth highest** salary".
- `NTILE(4)` → split into 4 buckets (quartiles).

### Value functions
- `LAG(col, 1) OVER (ORDER BY d)` → previous row's value.
- `LEAD(col, 1) OVER (ORDER BY d)` → next row's value.
- `FIRST_VALUE(col) OVER (...)`, `LAST_VALUE(col) OVER (...)`.

### Running totals and moving averages
```sql
SUM(amount) OVER (ORDER BY order_date
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
AVG(amount) OVER (ORDER BY order_date
                  ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)         AS moving_avg_3
SUM(salary) OVER (PARTITION BY dept_id)                              AS dept_total  -- no ORDER BY = whole group
salary / SUM(salary) OVER ()                                         AS pct_of_total
```

⚠️ **ROWS vs RANGE:** with `ORDER BY` but no frame, the default is `RANGE ... CURRENT ROW`.
Rows with the **same** order value are added **together** (ties jump at once).
Write `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` to go row by row.

⚠️ **LAST_VALUE trap:** default frame ends at the current row, so `LAST_VALUE`
returns the current row. Fix: `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`,
or use `FIRST_VALUE(...) ORDER BY ... DESC`.

### Filter on a window function
```sql
-- Works everywhere (BigQuery, Oracle, Spark)
SELECT * FROM (
    SELECT e.*, RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS rnk
    FROM employees e
) t
WHERE rnk = 1;

-- BigQuery (and Oracle 23ai+) short way. NOT in Spark, NOT in Oracle 19c.
SELECT * FROM employees
QUALIFY RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) = 1;
```
BigQuery note: `QUALIFY` works with or without a `WHERE`.

---

## 7. Common problem patterns

### 7.1 Top N per group (with ties)
```sql
WITH r AS (
  SELECT dept_id, name, salary,
         DENSE_RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS rnk
  FROM employees
  WHERE dept_id IS NOT NULL
)
SELECT * FROM r WHERE rnk <= 3;
```
Exactly N rows (no ties) → `ROW_NUMBER`. Ties shown → `RANK`/`DENSE_RANK`.

### 7.2 Nth highest salary (overall)
```sql
SELECT DISTINCT salary FROM (
  SELECT salary, DENSE_RANK() OVER (ORDER BY salary DESC) AS rnk FROM employees
) WHERE rnk = 2;
```
Oracle also: `ORDER BY salary DESC OFFSET 1 ROWS FETCH NEXT 1 ROWS ONLY` (on distinct salaries).

### 7.3 Remove duplicates (keep one)
```sql
-- Exact duplicate rows
SELECT DISTINCT * FROM orders;

-- Keep one row per key (choose which with ORDER BY)
SELECT * FROM (
  SELECT o.*, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_date DESC) AS rn
  FROM orders o
) WHERE rn = 1;

-- Find the duplicates
SELECT order_id, COUNT(*) FROM orders GROUP BY order_id HAVING COUNT(*) > 1;
```
Real pipelines: duplicate events from a message queue, and "latest load ID per date" use the same pattern.

### 7.4 Latest row per key
Same as 7.3: `ROW_NUMBER() OVER (PARTITION BY key ORDER BY date DESC) = 1`.
BigQuery also: `ARRAY_AGG(t ORDER BY date DESC LIMIT 1)[OFFSET(0)]`.

### 7.5 Groups with zero rows (show 0)
```sql
SELECT d.dept_name, COUNT(e.emp_id) AS n     -- COUNT(column), NOT COUNT(*)
FROM departments d
LEFT JOIN employees e ON e.dept_id = d.dept_id
GROUP BY d.dept_name;
```
`COUNT(*)` would give **1** for Legal (it counts the NULL row).

### 7.6 Self join: employee earns more than manager
```sql
SELECT e.name, e.salary, m.name AS manager, m.salary AS manager_salary
FROM employees e
JOIN employees m ON m.emp_id = e.manager_id
WHERE e.salary > m.salary;
```

### 7.7 Previous / next and % change (month over month)
```sql
WITH monthly AS (
  SELECT DATE_TRUNC(order_date, MONTH) AS month, SUM(amount) AS revenue   -- BigQuery
  FROM orders GROUP BY 1
)
SELECT month, revenue,
       LAG(revenue) OVER (ORDER BY month) AS prev,
       ROUND(100 * (revenue - LAG(revenue) OVER (ORDER BY month))
             / LAG(revenue) OVER (ORDER BY month), 1) AS pct_change
FROM monthly;
```
Divide by zero safe: BigQuery `SAFE_DIVIDE(a, b)`, Spark `try_divide(a, b)`,
Oracle `a / NULLIF(b, 0)` (works everywhere).

### 7.8 Gaps and islands (login streaks)
Idea: for days in a row, `date − row_number` is the **same** value.
```sql
WITH d AS (SELECT DISTINCT user_id, login_date FROM user_logins),    -- remove same-day duplicates first!
g AS (
  SELECT user_id, login_date,
         DATE_SUB(login_date, INTERVAL ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date) DAY) AS grp
         -- Oracle: login_date - ROW_NUMBER() OVER (...)
         -- Spark:  date_sub(login_date, ROW_NUMBER() OVER (...))
  FROM d
)
SELECT user_id, MIN(login_date) AS start_date, MAX(login_date) AS end_date, COUNT(*) AS days
FROM g
GROUP BY user_id, grp;
```

### 7.9 Hierarchy (all levels under a manager)
```sql
-- BigQuery / Spark: WITH RECURSIVE.  Oracle: WITH (no RECURSIVE word) + column list
WITH RECURSIVE r AS (
  SELECT emp_id, name, manager_id, 1 AS lvl FROM employees WHERE manager_id IS NULL
  UNION ALL
  SELECT e.emp_id, e.name, e.manager_id, r.lvl + 1
  FROM employees e JOIN r ON e.manager_id = r.emp_id
)
SELECT * FROM r;

-- Oracle classic
SELECT emp_id, name, LEVEL AS lvl
FROM employees
START WITH manager_id IS NULL
CONNECT BY PRIOR emp_id = manager_id;
```
Oracle recursive CTE needs a column list: `WITH r (emp_id, name, manager_id, lvl) AS (...)`.

### 7.10 Reconciliation: source vs target
```sql
-- 1. Counts and totals
SELECT 'source' AS side, COUNT(*) AS rows_cnt, SUM(amount) AS total FROM source_t
UNION ALL
SELECT 'target', COUNT(*), SUM(amount) FROM target_t;

-- 2. Rows in source but not in target (and the other way)
SELECT * FROM source_t EXCEPT DISTINCT SELECT * FROM target_t;   -- BigQuery
SELECT * FROM source_t MINUS SELECT * FROM target_t;             -- Oracle (EXCEPT also works in 23ai+)
SELECT * FROM source_t EXCEPT SELECT * FROM target_t;            -- Spark

-- 3. Which keys differ
SELECT COALESCE(s.id, t.id) AS id, s.amount AS src, t.amount AS tgt
FROM source_t s FULL OUTER JOIN target_t t ON s.id = t.id
WHERE s.id IS NULL OR t.id IS NULL OR s.amount <> t.amount;
```

### 7.11 Pivot (rows → columns)
```sql
-- Works everywhere
SELECT customer_id,
       SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed,
       SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled
FROM orders GROUP BY customer_id;

-- PIVOT keyword
SELECT * FROM (SELECT customer_id, status FROM orders)
PIVOT (COUNT(*) FOR status IN ('completed', 'cancelled'));              -- BigQuery, Spark
PIVOT (COUNT(*) FOR status IN ('completed' AS completed, 'cancelled' AS cancelled));  -- Oracle
```
⚠️ Spark `PIVOT` gives **NULL** (not 0) when there are no rows. Wrap with `COALESCE`.

### 7.12 Fill missing dates (calendar)
```sql
-- BigQuery
SELECT d FROM UNNEST(GENERATE_DATE_ARRAY('2026-01-01', '2026-01-31')) AS d
-- Spark
SELECT explode(sequence(DATE'2026-01-01', DATE'2026-01-31', INTERVAL 1 DAY)) AS d
-- Oracle
SELECT DATE '2026-01-01' + LEVEL - 1 AS d FROM dual CONNECT BY LEVEL <= 31
```
Then `LEFT JOIN` your data to the calendar → missing days show as 0/NULL.

---

## 8. Dates

| Need | BigQuery | Oracle | Spark |
|---|---|---|---|
| today | `CURRENT_DATE()` | `TRUNC(SYSDATE)` | `current_date()` |
| date literal | `DATE '2026-01-31'` | `DATE '2026-01-31'` | `DATE '2026-01-31'` |
| text → date | `PARSE_DATE('%d-%m-%Y', s)` | `TO_DATE(s, 'DD-MM-YYYY')` | `to_date(s, 'dd-MM-yyyy')` |
| date → text | `FORMAT_DATE('%Y-%m', d)` | `TO_CHAR(d, 'YYYY-MM')` | `date_format(d, 'yyyy-MM')` |
| days between | `DATE_DIFF(end, start, DAY)` | `end - start` | `datediff(end, start)` |
| months between | `DATE_DIFF(end, start, MONTH)` | `MONTHS_BETWEEN(end, start)` | `months_between(end, start)` |
| add days | `DATE_ADD(d, INTERVAL 7 DAY)` | `d + 7` | `date_add(d, 7)` |
| add months | `DATE_ADD(d, INTERVAL 1 MONTH)` | `ADD_MONTHS(d, 1)` | `add_months(d, 1)` |
| first day of month | `DATE_TRUNC(d, MONTH)` | `TRUNC(d, 'MM')` | `trunc(d, 'MM')` (date) |
| last day of month | `LAST_DAY(d)` | `LAST_DAY(d)` | `last_day(d)` |
| year / month | `EXTRACT(YEAR FROM d)` | `EXTRACT(YEAR FROM d)` | `year(d)`, `month(d)` |
| day of week | `EXTRACT(DAYOFWEEK FROM d)` (1=Sun) | `TO_CHAR(d, 'D')` | `dayofweek(d)` (1=Sun) |

⚠️ **Oracle `DATE` also stores a time.** `WHERE d = DATE '2026-01-31'` can miss rows
that have a time. Use `TRUNC(d) = DATE '2026-01-31'` or a range: `d >= X AND d < X + 1`.
⚠️ Spark `date_trunc('MONTH', d)` returns a **timestamp**; `trunc(d, 'MM')` returns a **date**.
⚠️ Format codes are different: BigQuery `%Y-%m-%d`, Oracle `YYYY-MM-DD`, Spark `yyyy-MM-dd`
(Spark: `MM` = month, `mm` = minutes!).

---

## 9. Strings

| Need | BigQuery | Oracle | Spark |
|---|---|---|---|
| join text | `CONCAT(a, b)` or `a \|\| b` | `a \|\| b` | `concat(a, b)` or `a \|\| b` |
| part of text | `SUBSTR(s, 1, 3)` | `SUBSTR(s, 1, 3)` | `substr(s, 1, 3)` |
| find position | `STRPOS(s, 'x')` | `INSTR(s, 'x')` | `instr(s, 'x')` |
| length | `LENGTH(s)` | `LENGTH(s)` | `length(s)` |
| upper / lower / trim | `UPPER`, `LOWER`, `TRIM` | same | same |
| replace | `REPLACE(s, 'a', 'b')` | same | same |
| split | `SPLIT(s, ',')` → array | `REGEXP_SUBSTR(s, '[^,]+', 1, n)` | `split(s, ',')` → array |
| regex match | `REGEXP_CONTAINS(s, r'^A')` | `REGEXP_LIKE(s, '^A')` | `s RLIKE '^A'` |
| starts with | `STARTS_WITH(s, 'A')` | `s LIKE 'A%'` | `startswith(s, 'A')` |

⚠️ Text compare is **case-sensitive** in BigQuery, Oracle and Spark: `'abc' = 'ABC'` is false.
Use `LOWER(a) = LOWER(b)`.
⚠️ Oracle: `CONCAT` takes only 2 arguments. Use `||`.

---

## 10. BigQuery vs Oracle vs Spark — cheat sheet

| Topic | BigQuery | Oracle | Spark SQL |
|---|---|---|---|
| table name | `project.dataset.table` (or `dataset.table`) | `schema.table` | `db.table` / view |
| limit rows | `LIMIT 10` | `FETCH FIRST 10 ROWS ONLY` (old: `WHERE ROWNUM <= 10`) | `LIMIT 10` |
| top with ties | use `RANK` | `FETCH FIRST 1 ROWS WITH TIES` | use `RANK` |
| SELECT without table | `SELECT 1` | `SELECT 1 FROM dual` (23ai+ allows no FROM) | `SELECT 1` |
| table alias | `FROM t AS x` or `FROM t x` | `FROM t x` only (**`AS` = error**) | both |
| column alias | `AS x` | `AS x` | `AS x` |
| `QUALIFY` | ✅ | ✅ 23ai+ only | ❌ |
| set difference | `EXCEPT DISTINCT` | `MINUS` (23ai+: `EXCEPT`) | `EXCEPT` |
| `IF` | `IF(cond, a, b)` | `CASE` / `DECODE` | `IF(cond, a, b)` |
| safe cast | `SAFE_CAST(x AS INT64)` | `CAST(x AS NUMBER DEFAULT NULL ON CONVERSION ERROR)` | `try_cast(x AS INT)` |
| integer divide | `DIV(7, 2)` = 3 | `TRUNC(7 / 2)` | `7 div 2` |
| `7 / 2` | 3.5 | 3.5 | 3.5 |
| boolean type | `BOOL` | none in 19c (23ai+ has `BOOLEAN`) | `BOOLEAN` |
| all columns except | `SELECT * EXCEPT(a, b)` | ❌ | `SELECT * EXCEPT(a, b)` |
| quote names | `` `name` `` | `"NAME"` (unquoted names are stored in CAPITALS) | `` `name` `` |
| upsert | `MERGE` | `MERGE` | `MERGE` (Delta/Iceberg tables) |
| speed tools | partition + cluster | indexes + partitions | partitions, broadcast, cache |
| cost | **bytes read** | your server | your cluster |

**Oracle 19c vs 23ai/26ai:** your local Oracle is 26ai, which allows `QUALIFY`, no `FROM dual`,
`GROUP BY alias`, `BOOLEAN`, `EXCEPT`. Many companies still run **19c**, where these give errors.
In interviews and at work, write the **classic** way first.

### BigQuery extras
- `SELECT *` costs more (reads all columns). `LIMIT` does **not** reduce cost.
- Partition by date → `WHERE date = ...` reads only that partition (cheaper).
- Cluster by columns you filter/join on often.
- `INFORMATION_SCHEMA` queries are billed at least 10 MB.
- Arrays: `UNNEST(arr)` turns an array into rows. `ARRAY_AGG(x)` turns rows into an array.
- Struct: `STRUCT(a, b)`, access with `s.a`.

### Oracle extras
- `ROWNUM` is given **before** `ORDER BY`. Top-N the old way needs a subquery:
  `SELECT * FROM (SELECT ... ORDER BY salary DESC) WHERE ROWNUM <= 5`.
- `NVL2(x, if_not_null, if_null)`, `DECODE(x, 1, 'a', 2, 'b', 'other')`.
- `RATIO_TO_REPORT(x) OVER ()` = x / total.
- Data dictionary: `user_tables`, `user_tab_columns` (names in CAPITALS).

---

## 11. PySpark DataFrame API (SQL → code)

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window
e, d = t["employees"], t["departments"]
```

| SQL | PySpark |
|---|---|
| `SELECT a, b` | `df.select("a", "b")` |
| `SELECT a AS x` | `df.select(F.col("a").alias("x"))` |
| new column | `df.withColumn("x", F.col("a") * 2)` |
| `WHERE a > 5 AND b = 'x'` | `df.where((F.col("a") > 5) & (F.col("b") == "x"))` |
| `OR` / `NOT` | `\|` / `~` (always use brackets!) |
| `IS NULL` | `F.col("a").isNull()` |
| `IN (1, 2)` | `F.col("a").isin(1, 2)` |
| `LIKE 'A%'` | `F.col("a").like("A%")` |
| `CASE WHEN` | `F.when(cond, "x").when(cond2, "y").otherwise("z")` |
| `COALESCE` | `F.coalesce("a", F.lit(0))` |
| `DISTINCT` | `df.distinct()` |
| dedupe by key | `df.dropDuplicates(["order_id"])` (keeps any one row) |
| `GROUP BY` | `df.groupBy("dept_id").agg(F.count("*").alias("n"), F.avg("salary"))` |
| `COUNT(col)` | `F.count("col")` (skips NULLs) |
| `COUNT(DISTINCT x)` | `F.countDistinct("x")` |
| `COUNT_IF` | `F.count_if(F.col("salary") > 100000)` or `F.sum(F.when(cond, 1).otherwise(0))` |
| `HAVING` | `.groupBy(...).agg(...).where(F.col("n") > 5)` |
| `ORDER BY a DESC` | `df.orderBy(F.col("a").desc())` / `.desc_nulls_last()` |
| `LIMIT 10` | `df.limit(10)` |
| `INNER JOIN` | `a.join(b, "key")` or `a.join(b, a.k == b.k, "inner")` |
| `LEFT JOIN` | `a.join(b, "key", "left")` |
| anti join | `a.join(b, "key", "left_anti")` |
| semi join | `a.join(b, "key", "left_semi")` |
| NULL-safe join | `a.join(b, a.k.eqNullSafe(b.k))` |
| broadcast join | `a.join(F.broadcast(b), "key")` |
| `UNION ALL` | `a.unionByName(b)` |
| `EXCEPT` | `a.exceptAll(b)` / `a.subtract(b)` (distinct) |
| run raw SQL | `spark.sql("SELECT ...")` |

**Window functions:**
```python
w = Window.partitionBy("dept_id").orderBy(F.col("salary").desc())

(e.withColumn("rnk", F.rank().over(w))          # F.row_number(), F.dense_rank()
  .withColumn("prev", F.lag("salary", 1).over(w))
  .where(F.col("rnk") == 1))                     # this is how you "QUALIFY" in Spark

# running total (row by row)
w_run = (Window.partitionBy("customer_id").orderBy("order_date")
         .rowsBetween(Window.unboundedPreceding, Window.currentRow))
orders.withColumn("running", F.sum("amount").over(w_run))

# group total on every row (no orderBy)
e.withColumn("dept_total", F.sum("salary").over(Window.partitionBy("dept_id")))
```

**Useful:**
- `df.show(20, truncate=False)`, `df.printSchema()`, `df.count()`
- `df.explain()` → see the plan (look for `BroadcastHashJoin` vs `SortMergeJoin`, `Exchange` = shuffle)
- Dates: `F.datediff(end, start)`, `F.add_months`, `F.trunc("d", "month")`, `F.date_format("d", "yyyy-MM")`
- Arrays: `F.explode`, `F.collect_list`, `F.collect_set`, `F.split`
- Never use `==` with `None`: use `.isNull()`.

---

## 12. Mistakes checklist (read before you submit)

- [ ] **Ties:** did I pick the right rank function (`ROW_NUMBER` / `RANK` / `DENSE_RANK`)?
- [ ] **NULLs:** do NULLs in keys, filters or `NOT IN` break my result?
- [ ] **Duplicates:** does the source have duplicate rows? Should I `DISTINCT` first?
- [ ] **Join fan-out:** did a one-to-many join repeat rows and inflate `SUM`/`COUNT`?
- [ ] **LEFT JOIN + WHERE** on the right table? Move the filter to `ON`.
- [ ] **`COUNT(*)` vs `COUNT(col)`** for groups with zero rows.
- [ ] **Window in WHERE?** Not allowed → CTE/subquery (or `QUALIFY` in BigQuery).
- [ ] **Running total ties:** need `ROWS BETWEEN ...`?
- [ ] **Integer vs decimal**, rounding (`ROUND(x, 2)`), divide by zero (`NULLIF(b, 0)`).
- [ ] **Dates:** Oracle `DATE` has time; Spark `MM` vs `mm`; `datediff` argument order.
- [ ] **Dialect:** Oracle → no `LIMIT`, no `AS` for table alias, `''` is NULL.
  Spark → no `QUALIFY`. BigQuery → `COUNTIF`, `dataset.table`.
- [ ] **Check the row count** of the result. Does it make sense?
