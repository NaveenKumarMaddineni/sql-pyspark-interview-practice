LOOP_DF = """
# Walk the tree level by level with joins, until no new rows come.
level = employees.filter(F.col("manager_id").isNull()) \\
                 .select("emp_id", "name", F.lit(1).alias("lvl"), F.col("name").alias("path"))
all_levels = level
while True:
    nxt = (employees.alias("e")
           .join(level.alias("r"), F.col("e.manager_id") == F.col("r.emp_id"))
           .select("e.emp_id", "e.name", (F.col("r.lvl") + 1).alias("lvl"),
                   F.concat_ws(" > ", "r.path", "e.name").alias("path")))
    if nxt.isEmpty():
        break
    all_levels, level = all_levels.unionByName(nxt), nxt
"""

TOPIC = dict(
    title="09 · Recursive queries and hierarchies",
    intro="""
`employees.manager_id` points to another employee: a **hierarchy** (a tree, like an org chart).
To walk it to any depth you need a **recursive CTE**:

```
WITH RECURSIVE r AS (
    <anchor: the starting rows>
    UNION ALL
    <recursive part: join the table to r, one level deeper>
)
```
It repeats the recursive part until it returns no new rows.

- BigQuery and Spark: `WITH RECURSIVE r AS (...)`
- Oracle: `WITH r (col1, col2, ...) AS (...)`: no word RECURSIVE, but you **must** list the columns.
  Oracle also has its own older syntax: `CONNECT BY PRIOR`.
""",
    questions=[
        Q("Numbers 1 to 10",
          "Make the numbers 1 to 10 (`n`) with a recursive CTE. No table needed.",
          level="easy",
          bq="""
WITH RECURSIVE nums AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM nums WHERE n < 10
)
SELECT n FROM nums ORDER BY n""",
          ora="""
WITH nums (n) AS (
    SELECT 1 FROM dual
    UNION ALL
    SELECT n + 1 FROM nums WHERE n < 10
)
SELECT n FROM nums ORDER BY n""",
          ssql="""
WITH RECURSIVE nums AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM nums WHERE n < 10
)
SELECT n FROM nums ORDER BY n""",
          sdf="""
res = spark.range(1, 11).withColumnRenamed("id", "n")""",
          note="The `WHERE n < 10` stops the recursion. Forget it and the query runs until it hits a limit "
               "(BigQuery: 500 steps, Spark: 100 by default).",
          notes={"spark": "For number or date lists you do not need recursion: `spark.range()` or `SEQUENCE()`."},
          ordered=True),

        Q("Level of every employee",
          "Show `emp_id`, `name`, `lvl` for everyone who can be reached from a top boss "
          "(an employee with no manager is level 1, their reports level 2, ...).",
          level="medium",
          bq="""
WITH RECURSIVE r AS (
    SELECT emp_id, name, 1 AS lvl
    FROM practice.employees
    WHERE manager_id IS NULL
    UNION ALL
    SELECT e.emp_id, e.name, r.lvl + 1
    FROM practice.employees e
    JOIN r ON e.manager_id = r.emp_id
)
SELECT emp_id, name, lvl FROM r ORDER BY lvl, emp_id""",
          ora="""
WITH r (emp_id, name, lvl) AS (
    SELECT emp_id, name, 1
    FROM employees
    WHERE manager_id IS NULL
    UNION ALL
    SELECT e.emp_id, e.name, r.lvl + 1
    FROM employees e
    JOIN r ON e.manager_id = r.emp_id
)
SELECT emp_id, name, lvl FROM r ORDER BY lvl, emp_id""",
          ssql="""
WITH RECURSIVE r AS (
    SELECT emp_id, name, 1 AS lvl
    FROM employees
    WHERE manager_id IS NULL
    UNION ALL
    SELECT e.emp_id, e.name, r.lvl + 1
    FROM employees e
    JOIN r ON e.manager_id = r.emp_id
)
SELECT emp_id, name, lvl FROM r ORDER BY lvl, emp_id""",
          sdf=LOOP_DF + """res = all_levels.select("emp_id", "name", "lvl").orderBy("lvl", "emp_id")""",
          note="Chloe Collins has no manager, so she is also a level-1 \"top\". Data has 2 trees.",
          notes={"spark": "The DataFrame API has no recursion: loop in Python, one join per level. "
                          "Fine for shallow trees; each loop adds to the plan, so `cache()` / `checkpoint()` "
                          "helps when it is deep.",
                 "oracle": "Classic Oracle: `SELECT emp_id, name, LEVEL AS lvl FROM employees "
                           "START WITH manager_id IS NULL CONNECT BY PRIOR emp_id = manager_id`."},
          ordered=True),

        Q("Path from the top",
          "Show `emp_id` and `path` like `Alice Johnson > Bob Smith > Carol White` for everyone in the trees.",
          level="medium",
          bq="""
WITH RECURSIVE r AS (
    SELECT emp_id, name, name AS path
    FROM practice.employees
    WHERE manager_id IS NULL
    UNION ALL
    SELECT e.emp_id, e.name, r.path || ' > ' || e.name
    FROM practice.employees e
    JOIN r ON e.manager_id = r.emp_id
)
SELECT emp_id, path FROM r ORDER BY path""",
          ora="""
WITH r (emp_id, name, path) AS (
    SELECT emp_id, name, CAST(name AS VARCHAR2(4000))
    FROM employees
    WHERE manager_id IS NULL
    UNION ALL
    SELECT e.emp_id, e.name, r.path || ' > ' || e.name
    FROM employees e
    JOIN r ON e.manager_id = r.emp_id
)
SELECT emp_id, path FROM r ORDER BY path""",
          ssql="""
WITH RECURSIVE r AS (
    SELECT emp_id, name, name AS path
    FROM employees
    WHERE manager_id IS NULL
    UNION ALL
    SELECT e.emp_id, e.name, r.path || ' > ' || e.name
    FROM employees e
    JOIN r ON e.manager_id = r.emp_id
)
SELECT emp_id, path FROM r ORDER BY path""",
          sdf=LOOP_DF + """res = all_levels.select("emp_id", "path").orderBy("path")""",
          notes={"oracle": "CAST the anchor text to a long VARCHAR2. Otherwise the column is only as wide as the "
                           "longest **name**, and longer paths fail. CONNECT BY version: "
                           "`SUBSTR(SYS_CONNECT_BY_PATH(name, ' > '), 4)`."},
          ordered=True),

        Q("Everyone under Bob Smith",
          "Show `emp_id` and `name` of **all** people below Bob Smith (emp_id 2) at any depth "
          "(not Bob himself).",
          level="medium",
          bq="""
WITH RECURSIVE r AS (
    SELECT emp_id, name FROM practice.employees WHERE manager_id = 2
    UNION ALL
    SELECT e.emp_id, e.name
    FROM practice.employees e
    JOIN r ON e.manager_id = r.emp_id
)
SELECT emp_id, name FROM r ORDER BY emp_id""",
          ora="""
WITH r (emp_id, name) AS (
    SELECT emp_id, name FROM employees WHERE manager_id = 2
    UNION ALL
    SELECT e.emp_id, e.name
    FROM employees e
    JOIN r ON e.manager_id = r.emp_id
)
SELECT emp_id, name FROM r ORDER BY emp_id""",
          ssql="""
WITH RECURSIVE r AS (
    SELECT emp_id, name FROM employees WHERE manager_id = 2
    UNION ALL
    SELECT e.emp_id, e.name
    FROM employees e
    JOIN r ON e.manager_id = r.emp_id
)
SELECT emp_id, name FROM r ORDER BY emp_id""",
          sdf="""
level = employees.filter("manager_id = 2").select("emp_id", "name")
found = level
while True:
    level = employees.join(level.select(F.col("emp_id").alias("manager_id")), "manager_id") \\
                     .select("emp_id", "name")
    if level.isEmpty():
        break
    found = found.unionByName(level)
res = found.orderBy("emp_id")""",
          note="Start (anchor) at Bob's **direct reports**, then go down.",
          ordered=True),

        Q("The chain of bosses above Grace Lee",
          "Walk **up** from Grace Lee (emp_id 7) to the top. Show `step` (1 = her direct manager), "
          "`emp_id`, `name` of each boss.",
          level="medium",
          bq="""
WITH RECURSIVE up AS (
    SELECT 1 AS step, m.emp_id, m.name, m.manager_id
    FROM practice.employees e
    JOIN practice.employees m ON m.emp_id = e.manager_id
    WHERE e.emp_id = 7
    UNION ALL
    SELECT up.step + 1, m.emp_id, m.name, m.manager_id
    FROM up
    JOIN practice.employees m ON m.emp_id = up.manager_id
)
SELECT step, emp_id, name FROM up ORDER BY step""",
          ora="""
WITH up (step, emp_id, name, manager_id) AS (
    SELECT 1, m.emp_id, m.name, m.manager_id
    FROM employees e
    JOIN employees m ON m.emp_id = e.manager_id
    WHERE e.emp_id = 7
    UNION ALL
    SELECT up.step + 1, m.emp_id, m.name, m.manager_id
    FROM up
    JOIN employees m ON m.emp_id = up.manager_id
)
SELECT step, emp_id, name FROM up ORDER BY step""",
          ssql="""
WITH RECURSIVE up AS (
    SELECT 1 AS step, m.emp_id, m.name, m.manager_id
    FROM employees e
    JOIN employees m ON m.emp_id = e.manager_id
    WHERE e.emp_id = 7
    UNION ALL
    SELECT up.step + 1, m.emp_id, m.name, m.manager_id
    FROM up
    JOIN employees m ON m.emp_id = up.manager_id
)
SELECT step, emp_id, name FROM up ORDER BY step""",
          sdf="""
lookup = {r.emp_id: (r.name, r.manager_id) for r in employees.collect()}   # tiny table: OK to collect
rows, boss, step = [], lookup[7][1], 1
while boss is not None:
    rows.append((step, boss, lookup[boss][0]))
    boss, step = lookup[boss][1], step + 1
res = spark.createDataFrame(rows, "step int, emp_id int, name string")""",
          note="Going **up** is the same idea: join on `m.emp_id = up.manager_id` instead.",
          notes={"spark": "For a tiny lookup table, `collect()` to Python and walk it there. Never collect big tables."},
          ordered=True),

        Q("Team size per manager (all levels)",
          "For every manager, show `manager_id`, `name` and `team_size` = number of people below them "
          "at **any** depth. Biggest team first.",
          level="hard",
          hint="Recursive CTE of pairs (top_manager, employee): anchor = direct reports; "
               "recursive part keeps the same top_manager and goes one level down.",
          bq="""
WITH RECURSIVE pairs AS (
    SELECT manager_id AS boss, emp_id
    FROM practice.employees
    WHERE manager_id IS NOT NULL
    UNION ALL
    SELECT p.boss, e.emp_id
    FROM pairs p
    JOIN practice.employees e ON e.manager_id = p.emp_id
)
SELECT p.boss AS manager_id, m.name, COUNT(*) AS team_size
FROM pairs p
JOIN practice.employees m ON m.emp_id = p.boss
GROUP BY p.boss, m.name
ORDER BY team_size DESC, manager_id""",
          ora="""
WITH pairs (boss, emp_id) AS (
    SELECT manager_id, emp_id
    FROM employees
    WHERE manager_id IS NOT NULL
    UNION ALL
    SELECT p.boss, e.emp_id
    FROM pairs p
    JOIN employees e ON e.manager_id = p.emp_id
)
SELECT p.boss AS manager_id, m.name, COUNT(*) AS team_size
FROM pairs p
JOIN employees m ON m.emp_id = p.boss
GROUP BY p.boss, m.name
ORDER BY team_size DESC, manager_id""",
          ssql="""
WITH RECURSIVE pairs AS (
    SELECT manager_id AS boss, emp_id
    FROM employees
    WHERE manager_id IS NOT NULL
    UNION ALL
    SELECT p.boss, e.emp_id
    FROM pairs p
    JOIN employees e ON e.manager_id = p.emp_id
)
SELECT p.boss AS manager_id, m.name, COUNT(*) AS team_size
FROM pairs p
JOIN employees m ON m.emp_id = p.boss
GROUP BY p.boss, m.name
ORDER BY team_size DESC, manager_id""",
          sdf="""
direct = employees.filter(F.col("manager_id").isNotNull()).select(F.col("manager_id").alias("boss"), "emp_id")
pairs, level = direct, direct
while True:
    level = (level.alias("p").join(employees.alias("e"), F.col("e.manager_id") == F.col("p.emp_id"))
             .select("p.boss", "e.emp_id"))
    if level.isEmpty():
        break
    pairs = pairs.unionByName(level)
res = (pairs.groupBy("boss").agg(F.count("*").alias("team_size"))
       .join(employees.select(F.col("emp_id").alias("boss"), "name"), "boss")
       .select(F.col("boss").alias("manager_id"), "name", "team_size")
       .orderBy(F.desc("team_size"), "manager_id"))""",
          note="Alice: 28 people (everyone except herself and Chloe). That includes Daniel (no department) but not Chloe (another tree).",
          ordered=True),

        Q("Cycle guard",
          "If bad data had a loop (A manages B, B manages A), the recursion would never end. "
          "Rewrite **Q2 (level of every employee)** so it also stops at **level 10**, "
          "and show the deepest `max_level` found.",
          level="medium",
          bq="""
WITH RECURSIVE r AS (
    SELECT emp_id, 1 AS lvl FROM practice.employees WHERE manager_id IS NULL
    UNION ALL
    SELECT e.emp_id, r.lvl + 1
    FROM practice.employees e
    JOIN r ON e.manager_id = r.emp_id
    WHERE r.lvl < 10
)
SELECT MAX(lvl) AS max_level FROM r""",
          ora="""
WITH r (emp_id, lvl) AS (
    SELECT emp_id, 1 FROM employees WHERE manager_id IS NULL
    UNION ALL
    SELECT e.emp_id, r.lvl + 1
    FROM employees e
    JOIN r ON e.manager_id = r.emp_id
    WHERE r.lvl < 10
)
CYCLE emp_id SET is_cycle TO 'Y' DEFAULT 'N'
SELECT MAX(lvl) AS max_level FROM r""",
          ssql="""
WITH RECURSIVE r AS (
    SELECT emp_id, 1 AS lvl FROM employees WHERE manager_id IS NULL
    UNION ALL
    SELECT e.emp_id, r.lvl + 1
    FROM employees e
    JOIN r ON e.manager_id = r.emp_id
    WHERE r.lvl < 10
)
SELECT MAX(lvl) AS max_level FROM r""",
          sdf=LOOP_DF.replace("while True:", "while True:  # add a depth limit in real code") +
          """res = all_levels.agg(F.max("lvl").alias("max_level"))""",
          note="A depth limit is the simple guard in every engine.",
          notes={"oracle": "Oracle also has a real cycle check: the `CYCLE ... SET ... TO ... DEFAULT ...` "
                           "clause (recursive CTE) or `CONNECT BY NOCYCLE`."}),
    ],
)

# "How it works" text for each question, in order (shown in the solutions).
HOW = [
    "Anchor: the row `1`. Recursive part: `n + 1` from the previous row while `n < 10`. Stops when no new row comes.",
    "Anchor = people with no manager (level 1). Each step joins employees whose manager is in the previous level, with `lvl + 1`.",
    "Same walk as Q2, but carry text: each step adds `' > ' || name` to the parent's path.",
    "Anchor = Bob's direct reports. Each step adds the reports of people already found.",
    "Anchor = Grace's manager (step 1). Each step joins to the manager of the previous person. Stops at a top boss (manager is NULL).",
    "Build all (boss, employee-below) pairs: start with direct pairs, then extend each pair one level down keeping the same boss. Count per boss.",
    "Add `WHERE r.lvl < 10` in the recursive part: the walk stops at 10 levels even if the data has a loop.",
]
