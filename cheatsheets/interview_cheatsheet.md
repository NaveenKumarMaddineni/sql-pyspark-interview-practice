# Interview cheatsheet — quick review

Read this in the 30 minutes before an interview. Every pattern here was run on BigQuery, Oracle and Spark
in this repo (see `solutions/`). Longer reference with more examples: [`sql_cheatsheet.md`](sql_cheatsheet.md).

---

## 1. Before you write any SQL (say it out loud)

1. **Grain**: "One row of the result is one ___ per ___."
2. **Ties?** Top N people → `ROW_NUMBER`. Top N values, keep ties → `DENSE_RANK`.
3. **NULLs?** In filters (`<>`, `NOT IN`), joins (NULL never matches), aggregates (skipped).
4. **Duplicates?** Is the key unique? If not, dedupe **before** SUM or join.
5. **Fan-out?** Joining 1 → many repeats the "1" side. Aggregate each side to the join key first.
6. **Edge cases**: empty groups (show 0?), dates at the boundary (`>=` start, `<` end).

## 2. Order SQL runs in

`FROM/JOIN → WHERE → GROUP BY → HAVING → window functions → SELECT → DISTINCT → ORDER BY → LIMIT`

- WHERE can't use aggregates (use HAVING) or window functions (use a subquery / QUALIFY).
- ORDER BY can use SELECT aliases. GROUP BY alias: yes in BigQuery/Spark, **no in Oracle 19c**.

## 3. NULL rules

| Thing | Result |
|---|---|
| `NULL = NULL`, `NULL <> 1` | unknown → row dropped by WHERE |
| `x NOT IN (1, NULL)` | **0 rows** → use `NOT EXISTS` |
| `COUNT(*)` / `COUNT(col)` | all rows / non-NULL only |
| `SUM/AVG/MIN/MAX` | skip NULLs; SUM of no rows = NULL, COUNT = 0 |
| `'a' \|\| NULL` | NULL in BigQuery/Spark, `'a'` in Oracle |
| `''` in Oracle | is NULL |
| ORDER BY ASC | NULLs **first** in BigQuery/Spark, **last** in Oracle → `NULLS LAST` |
| NULL-safe equal | Spark `<=>` / `eqNullSafe`, BigQuery & Oracle 23ai `IS NOT DISTINCT FROM` |

## 4. Joins in one line each

- **inner**: matches only · **left**: all left + NULLs · **full**: all both sides
- **anti** (no match): `LEFT JOIN ... WHERE right.key IS NULL` or `NOT EXISTS` · Spark `left_anti`
- **semi** (has a match, no repeats): `EXISTS` / `IN` · Spark `left_semi`
- **self join**: employee ↔ manager: `JOIN employees m ON m.emp_id = e.manager_id`
- **pairs once**: `ON a.k = b.k AND b.id > a.id`
- **grid with zeros**: `CROSS JOIN` all dims, then `LEFT JOIN` facts, `COALESCE(x, 0)`
- Filter on the right table of a LEFT JOIN goes in **ON**, not WHERE (WHERE turns it into an inner join).

## 5. Window functions

```sql
fn() OVER (PARTITION BY grp ORDER BY col ROWS BETWEEN ... AND ...)
```

| Salaries 100, 90, 90, 80 | ROW_NUMBER | RANK | DENSE_RANK |
|---|---|---|---|
| | 1, 2, 3, 4 | 1, 2, 2, **4** | 1, 2, 2, **3** |

- Running total: `SUM(x) OVER (ORDER BY d ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`
- Moving avg (3 rows): `AVG(x) OVER (ORDER BY d ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)`
- Group total on every row: `SUM(x) OVER (PARTITION BY g)` (no ORDER BY)
- Previous / next: `LAG(x)`, `LEAD(x)` · first in group: `FIRST_VALUE(x)`
- **Trap**: default frame with ORDER BY is `RANGE ... CURRENT ROW` → ties get the same running total;
  `LAST_VALUE` returns the current row unless the frame goes to `UNBOUNDED FOLLOWING`.
- Filter: subquery + `WHERE rn = 1` (all engines) · `QUALIFY` in BigQuery & Oracle 26ai · **not** in Spark SQL.

## 6. The patterns (templates)

**Top N per group (keep ties)**
```sql
SELECT * FROM (SELECT t.*, DENSE_RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) rk FROM employees t) x
WHERE rk <= 2
```

**Dedupe / latest row per key**
```sql
SELECT * FROM (SELECT t.*, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) rn FROM t) x
WHERE rn = 1
```

**Nth highest**: `DENSE_RANK() ... = N` + DISTINCT · 2nd without windows: `MAX(s) WHERE s < (SELECT MAX(s) ...)`

**Find duplicates**: `GROUP BY all_cols HAVING COUNT(*) > 1`

**Gaps and islands (streaks)**: dedupe days → `rn = ROW_NUMBER() OVER (PARTITION BY user ORDER BY d)` →
group by `d - rn` (BigQuery `DATE_SUB(d, INTERVAL rn DAY)`, Spark `DATE_SUB(d, rn)`) → `MIN, MAX, COUNT`.

**Gaps**: `LEAD(d) OVER (PARTITION BY user ORDER BY d) - d > 1`

**Month over month**: aggregate per month → `(rev - LAG(rev)) / LAG(rev) * 100` · divide safely with `NULLIF(x, 0)`

**Retention**: start group `LEFT JOIN` later group → `COUNT(later.user_id) / COUNT(*)`

**Pivot**: `SUM(CASE WHEN yr = 2024 THEN qty ELSE 0 END) AS y2024`

**Median**: Oracle/Spark `MEDIAN(x)` · BigQuery `PERCENTILE_CONT(x, 0.5) OVER (PARTITION BY g)` + DISTINCT

**Fill missing dates**: BigQuery `UNNEST(GENERATE_DATE_ARRAY(a, b))` · Oracle `DATE 'a' + LEVEL - 1 FROM dual CONNECT BY LEVEL <= n`
· Spark `EXPLODE(SEQUENCE(a, b, INTERVAL 1 DAY))` → LEFT JOIN → COUNT

**Hierarchy**: `WITH RECURSIVE r AS (anchor UNION ALL SELECT ... FROM t JOIN r ON t.parent = r.id)`
(Oracle: `WITH r (cols) AS ...` without RECURSIVE, or `CONNECT BY PRIOR id = parent`)

**Upsert**: `MERGE INTO t USING s ON (t.id = s.id) WHEN MATCHED THEN UPDATE SET ... WHEN NOT MATCHED THEN INSERT ...`
(dedupe the source first)

**SCD Type 2**: (1) UPDATE current rows that changed → `valid_to = d-1, is_current = 'N'` (NULL-safe compare!)
(2) INSERT new current rows for keys with no current row.

**Idempotent load**: delete the slice (date / load id) + insert it again, in one transaction · Spark: dynamic partition overwrite.

**Reconciliation**: `COUNT(*)` and `SUM(amount)` on source vs target → MATCH / MISMATCH; row level: FULL OUTER JOIN + NULL-safe compare.

## 7. Engine differences that interviews catch

| | BigQuery | Oracle | Spark SQL |
|---|---|---|---|
| first n rows | `LIMIT n` | `FETCH FIRST n ROWS ONLY` | `LIMIT n` |
| page | `LIMIT n OFFSET m` | `OFFSET m ROWS FETCH NEXT n ROWS ONLY` | `LIMIT n OFFSET m` |
| table alias | `t` or `AS t` | `t` only (**no AS**) | both |
| no table | `SELECT 1` | `SELECT 1 FROM dual` (19c) | `SELECT 1` |
| add days | `DATE_ADD(d, INTERVAL 7 DAY)` | `d + 7` | `DATE_ADD(d, 7)` |
| day difference | `DATE_DIFF(a, b, DAY)` | `a - b` | `DATEDIFF(a, b)` |
| month start | `DATE_TRUNC(d, MONTH)` | `TRUNC(d, 'MM')` | `TRUNC(d, 'MM')` |
| full years | months/12 (not `DATE_DIFF(YEAR)`) | `FLOOR(MONTHS_BETWEEN(a, b) / 12)` | same as Oracle |
| date → text | `FORMAT_DATE('%d/%m/%Y', d)` | `TO_CHAR(d, 'DD/MM/YYYY')` | `DATE_FORMAT(d, 'dd/MM/yyyy')` |
| text → date | `PARSE_DATE('%Y-%m-%d', s)` | `TO_DATE(s, 'YYYY-MM-DD')` | `TO_DATE(s, 'yyyy-MM-dd')` |
| safe cast | `SAFE_CAST(x AS NUMERIC)` | `TO_NUMBER(x DEFAULT NULL ON CONVERSION ERROR)` | `TRY_CAST(x AS DECIMAL)` |
| list to text | `STRING_AGG(x, ', ' ORDER BY x)` | `LISTAGG(x, ', ') WITHIN GROUP (ORDER BY x)` | `LISTAGG` (Spark 4) |
| regex match | `REGEXP_CONTAINS(s, r'..')` | `REGEXP_LIKE(s, '..')` | `s RLIKE '..'` |
| set ops | `UNION DISTINCT`, `EXCEPT DISTINCT` | `UNION`, `MINUS` | `UNION`, `EXCEPT` |
| count if | `COUNTIF(cond)` | `SUM(CASE ...)` | `COUNT_IF(cond)` (no SUM of boolean) |

## 8. PySpark DataFrame API ↔ SQL

| SQL | DataFrame |
|---|---|
| `SELECT a, b*2 AS c` | `df.select("a", (F.col("b") * 2).alias("c"))` |
| `WHERE a = 1 AND b IS NULL` | `.filter((F.col("a") == 1) & F.col("b").isNull())` |
| `CASE WHEN` | `F.when(cond, x).otherwise(y)` |
| `GROUP BY a` + `HAVING` | `.groupBy("a").agg(F.sum("x").alias("s")).filter("s > 10")` |
| `COUNT(DISTINCT x)` | `F.countDistinct("x")` |
| `LEFT JOIN ... USING (k)` | `a.join(b, "k", "left")` |
| `NOT EXISTS` / `EXISTS` | `a.join(b, "k", "left_anti")` / `"left_semi"` |
| window | `w = Window.partitionBy("g").orderBy(F.desc("x"))` → `F.row_number().over(w)` |
| `UNION ALL` / `UNION` | `.unionByName(other)` / `+ .distinct()` |
| `DISTINCT` / dedupe on keys | `.distinct()` / `.dropDuplicates(["k"])` (keeps any one row) |
| `COALESCE(x, 0)` | `F.coalesce("x", F.lit(0))` / `.fillna({"x": 0})` |
| rows → list / list → rows | `F.collect_list("x")` / `F.explode("arr")` |

## 9. Spark performance in one line each

- **Lazy**: transformations build a plan (DAG); actions run it.
- **Narrow** (filter, select) = no data movement · **wide** (groupBy, join, distinct, orderBy) = **shuffle** = new stage.
- Job (per action) → stages (cut at shuffles) → tasks (one per partition).
- **Broadcast join** for small tables (auto under 10 MB) = no shuffle of the big side.
- **Skew**: one key much bigger → broadcast, AQE skew join, or **salting**.
- **AQE**: coalesces shuffle partitions, switches to broadcast, splits skew (on by default).
- `repartition(n)` = shuffle, even sizes · `coalesce(n)` = merge, no shuffle, only down.
- Partition size ~100–200 MB; shuffle partitions ≈ 2–3 × cores; default 200.
- **cache** only reused data; `unpersist()` after. Python **UDFs** are slow → built-ins, then pandas UDFs.
- Read less: select columns early (**column pruning**), filter early (**predicate pushdown**), partition filters.
- Avoid `collect()` / `toPandas()` on big data (driver OOM). Avoid many small files.
- Pipelines: explicit schema, audit columns (`run_id`), quality gate, reconcile, idempotent overwrite.

## 10. BigQuery and Oracle in one line each

- BigQuery charges **bytes read** (on-demand): no `SELECT *`, LIMIT doesn't reduce cost, use **dry run**.
- **Partition** by date (skip partitions, cost known before running) · **cluster** by filter/join columns (≤ 4).
- `require_partition_filter`, max bytes billed, budgets, `INFORMATION_SCHEMA.JOBS` to find expensive queries.
- Nested data: `ARRAY<STRUCT<...>>` + `UNNEST`. Storage and compute are separate; **slots** = compute units.
- Oracle: user = schema; PDB via service name; DATE has a time part; `''` is NULL; commit your DML.
- Oracle plan: `EXPLAIN PLAN FOR ...` + `DBMS_XPLAN.DISPLAY`; read from the most indented line;
  gather stats with `DBMS_STATS`; functions on a column block normal index use.
- Loading files into Oracle: SQL*Loader / external table → staging → check control file → delete slice + insert / MERGE.

## 11. Modeling in one line each

- OLTP = many small transactions, normalized · OLAP = big scans, columnar, denormalized.
- Star schema: **fact** (events, measures, at a stated grain) + **dimensions** (who/what/when).
- SCD 1 = overwrite · SCD 2 = new row + valid_from/valid_to/is_current · SCD 3 = previous-value column.
- Surrogate key = warehouse id (needed for SCD 2) · natural key = business id.
- ETL = transform before load · ELT = load raw, transform in the warehouse (fits BigQuery).
- Late data: lookback window + idempotent reruns · backfill = same job per date, reconciled.
