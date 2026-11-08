SOLD = """
sold AS (
    SELECT p.category, p.product_name, EXTRACT(YEAR FROM o.order_date) AS yr, i.quantity
    FROM (SELECT DISTINCT order_id, order_date, status FROM orders) o
    JOIN order_items i ON i.order_id = o.order_id
    JOIN products p    ON p.product_id = i.product_id
    WHERE o.status = 'completed'
)"""
SOLD_DF = """
sold = (orders.filter(F.col("status") == "completed")
        .select("order_id", "order_date").distinct()
        .join(order_items, "order_id").join(products, "product_id")
        .withColumn("yr", F.year("order_date")))
"""

STREAK_TAIL = """
SELECT user_id, MIN(login_date) AS streak_start, MAX(login_date) AS streak_end, COUNT(*) AS days
FROM grouped
GROUP BY user_id, grp
HAVING COUNT(*) >= 3
ORDER BY user_id, streak_start"""


def streak(sub):
    return f"""
WITH days AS (
    SELECT DISTINCT user_id, login_date FROM user_logins
),
numbered AS (
    SELECT user_id, login_date,
           ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date) AS rn
    FROM days
),
grouped AS (
    SELECT user_id, login_date, {sub} AS grp
    FROM numbered
)""" + STREAK_TAIL


TOPIC = dict(
    title="08 · Classic interview patterns",
    intro="""
These patterns come up again and again in data engineering interviews:
duplicates, latest row, **gaps and islands** (streaks of consecutive days), retention,
pivot, median, filling missing dates, top N per group.

Most of them use window functions (topic 05). Read the question twice: *ties? NULLs? duplicates?*
""",
    questions=[
        Q("Find the duplicate rows",
          "Show the rows of `orders` that appear **more than once** (all 5 columns) and `copies` = how many times.",
          level="easy",
          sql="""
SELECT order_id, customer_id, order_date, amount, status, COUNT(*) AS copies
FROM orders
GROUP BY order_id, customer_id, order_date, amount, status
HAVING COUNT(*) > 1""",
          sdf="""
res = (orders.groupBy(orders.columns).agg(F.count("*").alias("copies"))
       .filter("copies > 1"))""",
          note="GROUP BY **all** columns finds exact duplicates. To find duplicate **keys** "
               "(same order_id, different values), group by the key only."),

        Q("Remove duplicates, keep one",
          "Return `orders` with each exact duplicate kept **once** (104 rows). Use ROW_NUMBER, "
          "because that also works when you must choose *which* copy to keep.",
          level="medium",
          sql="""
SELECT order_id, customer_id, order_date, amount, status
FROM (SELECT o.*,
             ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_date DESC) AS rn
      FROM orders o) t
WHERE rn = 1""",
          sdf="""
w = Window.partitionBy("order_id").orderBy(F.desc("order_date"))
res = orders.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")""",
          note="Simple ways: `SELECT DISTINCT *` or `dropDuplicates()`. ROW_NUMBER lets you keep the "
               "**latest** version of a key (e.g. order by `updated_at DESC`).",
          notes={"spark": "`dropDuplicates([\"order_id\"])` keeps **any one** row per key, not a chosen one. "
                          "Use the window when it matters.",
                 "bigquery": "To fix the table in place: `CREATE OR REPLACE TABLE ... AS SELECT DISTINCT * ...`.",
                 "oracle": "To delete duplicates in place, Oracle has ROWID (the row's physical address): "
                           "`DELETE FROM orders WHERE ROWID NOT IN (SELECT MIN(ROWID) FROM orders GROUP BY order_id)`."}),

        Q("Login streaks of 3+ days (gaps and islands)",
          "For each user, find runs of **consecutive** login days that are 3 days or longer. "
          "Show `user_id`, `streak_start`, `streak_end`, `days`. A user can log in twice on one day: count the day once.",
          level="hard",
          hint="Number each user's days 1, 2, 3... with ROW_NUMBER. `login_date - rn` stays the **same** "
               "inside a run of consecutive days. Group by it.",
          bq=streak("DATE_SUB(login_date, INTERVAL rn DAY)").replace("FROM user_logins", "FROM shield.user_logins"),
          ora=streak("login_date - rn"),
          ssql=streak("DATE_SUB(login_date, rn)"),
          sdf="""
w = Window.partitionBy("user_id").orderBy("login_date")
res = (user_logins.dropDuplicates()
       .withColumn("rn", F.row_number().over(w))
       .withColumn("grp", F.date_sub("login_date", F.col("rn")))
       .groupBy("user_id", "grp")
       .agg(F.min("login_date").alias("streak_start"),
            F.max("login_date").alias("streak_end"),
            F.count("*").alias("days"))
       .filter("days >= 3").drop("grp")
       .orderBy("user_id", "streak_start"))""",
          note="**Gaps and islands**: an island = a run of consecutive values. Date minus row number is "
               "constant inside an island. Remove same-day duplicates first, or the trick breaks.",
          ordered=True),

        Q("Longest streak per user",
          "Show `user_id` and `longest_streak` (days) for every user who logged in.",
          level="hard",
          bq="""
WITH days AS (SELECT DISTINCT user_id, login_date FROM shield.user_logins),
grouped AS (
    SELECT user_id,
           DATE_SUB(login_date, INTERVAL ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date) DAY) AS grp
    FROM days
),
streaks AS (SELECT user_id, COUNT(*) AS days FROM grouped GROUP BY user_id, grp)
SELECT user_id, MAX(days) AS longest_streak
FROM streaks
GROUP BY user_id
ORDER BY user_id""",
          ora="""
WITH days AS (SELECT DISTINCT user_id, login_date FROM user_logins),
grouped AS (
    SELECT user_id,
           login_date - ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date) AS grp
    FROM days
),
streaks AS (SELECT user_id, COUNT(*) AS days FROM grouped GROUP BY user_id, grp)
SELECT user_id, MAX(days) AS longest_streak
FROM streaks
GROUP BY user_id
ORDER BY user_id""",
          ssql="""
WITH days AS (SELECT DISTINCT user_id, login_date FROM user_logins),
numbered AS (
    SELECT user_id, login_date,
           ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date) AS rn
    FROM days
),
streaks AS (
    SELECT user_id, COUNT(*) AS days
    FROM numbered
    GROUP BY user_id, DATE_SUB(login_date, rn)
)
SELECT user_id, MAX(days) AS longest_streak
FROM streaks
GROUP BY user_id
ORDER BY user_id""",
          sdf="""
w = Window.partitionBy("user_id").orderBy("login_date")
res = (user_logins.dropDuplicates()
       .withColumn("grp", F.date_sub("login_date", F.row_number().over(w)))
       .groupBy("user_id", "grp").count()
       .groupBy("user_id").agg(F.max("count").alias("longest_streak"))
       .orderBy("user_id"))""",
          ordered=True),

        Q("Find the gaps",
          "For each user, show the gaps between logins: `user_id`, `last_login` (day before the gap), "
          "`next_login` (day after the gap), `days_missed`. Only gaps where at least 1 day was missed.",
          level="medium",
          bq="""
WITH days AS (SELECT DISTINCT user_id, login_date FROM shield.user_logins),
nxt AS (
    SELECT user_id, login_date AS last_login,
           LEAD(login_date) OVER (PARTITION BY user_id ORDER BY login_date) AS next_login
    FROM days
)
SELECT user_id, last_login, next_login, DATE_DIFF(next_login, last_login, DAY) - 1 AS days_missed
FROM nxt
WHERE DATE_DIFF(next_login, last_login, DAY) > 1
ORDER BY user_id, last_login""",
          ora="""
WITH days AS (SELECT DISTINCT user_id, login_date FROM user_logins),
nxt AS (
    SELECT user_id, login_date AS last_login,
           LEAD(login_date) OVER (PARTITION BY user_id ORDER BY login_date) AS next_login
    FROM days
)
SELECT user_id, last_login, next_login, next_login - last_login - 1 AS days_missed
FROM nxt
WHERE next_login - last_login > 1
ORDER BY user_id, last_login""",
          ssql="""
WITH days AS (SELECT DISTINCT user_id, login_date FROM user_logins),
nxt AS (
    SELECT user_id, login_date AS last_login,
           LEAD(login_date) OVER (PARTITION BY user_id ORDER BY login_date) AS next_login
    FROM days
)
SELECT user_id, last_login, next_login, DATEDIFF(next_login, last_login) - 1 AS days_missed
FROM nxt
WHERE DATEDIFF(next_login, last_login) > 1
ORDER BY user_id, last_login""",
          sdf="""
w = Window.partitionBy("user_id").orderBy("login_date")
res = (user_logins.dropDuplicates()
       .withColumn("next_login", F.lead("login_date").over(w))
       .withColumnRenamed("login_date", "last_login")
       .withColumn("days_missed", F.datediff("next_login", "last_login") - 1)
       .filter("days_missed >= 1")
       .orderBy("user_id", "last_login"))""",
          note="The last login of each user has `next_login = NULL`, so the WHERE drops it (NULL > 1 is unknown).",
          ordered=True),

        Q("Retention: January users back in February",
          "Of the users who logged in during **January 2025**, how many also logged in during "
          "**February 2025**? Show `jan_users`, `returned_users`, `retention_pct` (1 decimal).",
          level="medium",
          sql="""
WITH jan AS (
    SELECT DISTINCT user_id FROM user_logins
    WHERE login_date >= DATE '2025-01-01' AND login_date < DATE '2025-02-01'
),
feb AS (
    SELECT DISTINCT user_id FROM user_logins
    WHERE login_date >= DATE '2025-02-01' AND login_date < DATE '2025-03-01'
)
SELECT COUNT(*)                                       AS jan_users,
       COUNT(feb.user_id)                             AS returned_users,
       ROUND(100 * COUNT(feb.user_id) / COUNT(*), 1)  AS retention_pct
FROM jan
LEFT JOIN feb ON feb.user_id = jan.user_id""",
          sdf="""
between = lambda a, b: user_logins.filter((F.col("login_date") >= a) & (F.col("login_date") < b)) \\
                                  .select("user_id").distinct()
jan, feb = between("2025-01-01", "2025-02-01"), between("2025-02-01", "2025-03-01")
res = (jan.join(feb.withColumn("back", F.lit(1)), "user_id", "left")
       .agg(F.count("*").alias("jan_users"), F.count("back").alias("returned_users"))
       .withColumn("retention_pct", F.round(100 * F.col("returned_users") / F.col("jan_users"), 1)))""",
          note="LEFT JOIN from the **starting group**, then `COUNT(right_key)` counts only the matches."),

        Q("Pivot: items sold per category and year",
          "Show one row per `category` with columns `y2023`, `y2024`, `y2025` = quantity sold in "
          "**completed** orders (each order once). Use 0 when nothing was sold.",
          level="medium",
          sql="""
WITH""" + SOLD + """
SELECT category,
       SUM(CASE WHEN yr = 2023 THEN quantity ELSE 0 END) AS y2023,
       SUM(CASE WHEN yr = 2024 THEN quantity ELSE 0 END) AS y2024,
       SUM(CASE WHEN yr = 2025 THEN quantity ELSE 0 END) AS y2025
FROM sold
GROUP BY category
ORDER BY category""",
          sdf=SOLD_DF + """res = (sold.groupBy("category").pivot("yr", [2023, 2024, 2025]).sum("quantity")
       .na.fill(0)
       .withColumnsRenamed({"2023": "y2023", "2024": "y2024", "2025": "y2025"})
       .orderBy("category"))""",
          note="CASE inside SUM is the portable pivot. All 3 engines also have a `PIVOT` clause, "
               "but its syntax is different in each one.",
          notes={"bigquery": "BigQuery: `... PIVOT (SUM(quantity) FOR yr IN (2023 AS y2023, 2024 AS y2024))`.",
                 "oracle": "Oracle: `... PIVOT (SUM(quantity) FOR yr IN (2023 AS y2023, 2024 AS y2024))`.",
                 "spark": "DataFrame `pivot(col, [values])`: give the values, or Spark runs an extra job to find them."},
          ordered=True),

        Q("Median salary per department",
          "Show `dept_id` and `median_salary` for each department (skip NULL dept_id).",
          level="medium",
          bq="""
SELECT DISTINCT dept_id,
       PERCENTILE_CONT(salary, 0.5) OVER (PARTITION BY dept_id) AS median_salary
FROM shield.employees
WHERE dept_id IS NOT NULL
ORDER BY dept_id""",
          ora="""
SELECT dept_id, MEDIAN(salary) AS median_salary
FROM employees
WHERE dept_id IS NOT NULL
GROUP BY dept_id
ORDER BY dept_id""",
          ssql="""
SELECT dept_id, MEDIAN(salary) AS median_salary
FROM employees
WHERE dept_id IS NOT NULL
GROUP BY dept_id
ORDER BY dept_id""",
          sdf="""
res = (employees.filter(F.col("dept_id").isNotNull())
       .groupBy("dept_id").agg(F.median("salary").alias("median_salary"))
       .orderBy("dept_id"))""",
          note="Median = the middle value; with an even count, the average of the two middle values. "
               "Without a built-in: ROW_NUMBER + COUNT, then average the middle row(s).",
          notes={"bigquery": "In BigQuery, PERCENTILE_CONT is only a **window** function, so use DISTINCT. "
                             "`APPROX_QUANTILES(salary, 2)[OFFSET(1)]` is fast but approximate.",
                 "oracle": "Also: `PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary)`."},
          ordered=True),

        Q("Fill missing dates with 0",
          "For **user 1**, show every date of January 2025 (`login_date`) and `num_logins` "
          "(login rows that day). Days without logins show **0**. 31 rows.",
          level="hard",
          bq="""
SELECT d AS login_date, COUNT(l.user_id) AS num_logins
FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2025-01-01', DATE '2025-01-31')) AS d
LEFT JOIN shield.user_logins l
       ON l.login_date = d AND l.user_id = 1
GROUP BY d
ORDER BY d""",
          ora="""
WITH cal AS (
    SELECT DATE '2025-01-01' + LEVEL - 1 AS d
    FROM dual
    CONNECT BY LEVEL <= 31
)
SELECT cal.d AS login_date, COUNT(l.user_id) AS num_logins
FROM cal
LEFT JOIN user_logins l
       ON l.login_date = cal.d AND l.user_id = 1
GROUP BY cal.d
ORDER BY cal.d""",
          ssql="""
WITH cal AS (
    SELECT EXPLODE(SEQUENCE(DATE '2025-01-01', DATE '2025-01-31', INTERVAL 1 DAY)) AS d
)
SELECT cal.d AS login_date, COUNT(l.user_id) AS num_logins
FROM cal
LEFT JOIN user_logins l
       ON l.login_date = cal.d AND l.user_id = 1
GROUP BY cal.d
ORDER BY cal.d""",
          sdf="""
cal = spark.sql("SELECT EXPLODE(SEQUENCE(DATE'2025-01-01', DATE'2025-01-31', INTERVAL 1 DAY)) AS login_date")
u1 = user_logins.filter("user_id = 1")
res = (cal.join(u1, "login_date", "left")
       .groupBy("login_date").agg(F.count("user_id").alias("num_logins"))
       .orderBy("login_date"))""",
          note="Put the `user_id = 1` filter in the **ON** clause. In WHERE it would remove the empty days "
               "(their user_id is NULL), and the LEFT JOIN becomes an INNER JOIN.",
          notes={"oracle": "`CONNECT BY LEVEL <= n` makes n rows from `dual`: the classic Oracle row generator."},
          ordered=True),

        Q("New users per day and running total",
          "A user is **new** on the day of their first login. Show `login_date`, `new_users`, "
          "`total_users` (running total), only for days with at least one new user.",
          level="medium",
          sql="""
WITH firsts AS (
    SELECT user_id, MIN(login_date) AS first_day
    FROM user_logins
    GROUP BY user_id
)
SELECT first_day AS login_date,
       COUNT(*)  AS new_users,
       SUM(COUNT(*)) OVER (ORDER BY first_day) AS total_users
FROM firsts
GROUP BY first_day
ORDER BY first_day""",
          sdf="""
firsts = user_logins.groupBy("user_id").agg(F.min("login_date").alias("login_date"))
res = (firsts.groupBy("login_date").agg(F.count("*").alias("new_users"))
       .withColumn("total_users", F.sum("new_users").over(Window.orderBy("login_date")))
       .orderBy("login_date"))""",
          note="`SUM(COUNT(*)) OVER (...)`: the window runs **after** GROUP BY, so it can use an aggregate.",
          ordered=True),

        Q("Best-selling product per category (with ties)",
          "For each category, show the product(s) with the **most quantity sold** in completed orders "
          "(each order once). Show `category`, `product_name`, `qty`. Keep ties.",
          level="medium",
          sql="""
WITH""" + SOLD + """,
per_product AS (
    SELECT category, product_name, SUM(quantity) AS qty
    FROM sold
    GROUP BY category, product_name
),
ranked AS (
    SELECT category, product_name, qty,
           RANK() OVER (PARTITION BY category ORDER BY qty DESC) AS rnk
    FROM per_product
)
SELECT category, product_name, qty
FROM ranked
WHERE rnk = 1
ORDER BY category, product_name""",
          sdf=SOLD_DF + """per_product = sold.groupBy("category", "product_name").agg(F.sum("quantity").alias("qty"))
w = Window.partitionBy("category").orderBy(F.desc("qty"))
res = (per_product.withColumn("rnk", F.rank().over(w)).filter("rnk = 1").drop("rnk")
       .orderBy("category", "product_name"))""",
          note="Aggregate first, rank second. RANK keeps ties.",
          ordered=True),

        Q("Two orders in a row with the same status",
          "For each customer, sort their different orders by date (then order_id). Find orders whose "
          "status is the **same as the previous order's** status. Show `customer_id`, `order_id`, "
          "`order_date`, `status`.",
          level="medium",
          sql="""
WITH o AS (
    SELECT DISTINCT order_id, customer_id, order_date, status FROM orders
),
with_prev AS (
    SELECT o.*,
           LAG(status) OVER (PARTITION BY customer_id ORDER BY order_date, order_id) AS prev_status
    FROM o
)
SELECT customer_id, order_id, order_date, status
FROM with_prev
WHERE status = prev_status
ORDER BY customer_id, order_date, order_id""",
          sdf="""
w = Window.partitionBy("customer_id").orderBy("order_date", "order_id")
res = (orders.select("order_id", "customer_id", "order_date", "status").distinct()
       .withColumn("prev_status", F.lag("status").over(w))
       .filter(F.col("status") == F.col("prev_status"))
       .select("customer_id", "order_id", "order_date", "status")
       .orderBy("customer_id", "order_date", "order_id"))""",
          ordered=True),
    ],
)

# "How it works" text for each question, in order (shown in the solutions).
HOW = [
    "GROUP BY all columns makes one group per distinct row; `HAVING COUNT(*) > 1` keeps only rows that appear several times.",
    "Number the rows inside each `order_id` with ROW_NUMBER and keep number 1. The ORDER BY decides which copy wins.",
    "1) One row per user per day (DISTINCT). 2) ROW_NUMBER per user by date. 3) `date − row_number` is the same for consecutive days = a group id. 4) Group by it: MIN, MAX, COUNT.",
    "Same group trick as Q3; count days per group, then take the MAX per user.",
    "`LEAD(login_date)` gives the user's next login day; when next − this > 1, days were missed in between.",
    "Users of January, users of February. LEFT JOIN January → February: `COUNT(feb.user_id)` counts only those who came back.",
    "One SUM(CASE ...) per year turns year **rows** into year **columns**; ELSE 0 gives 0 for missing years.",
    "Median = 50th percentile: `MEDIAN()` (Oracle, Spark) or `PERCENTILE_CONT(salary, 0.5)` window (BigQuery).",
    "1) Generate all 31 dates. 2) LEFT JOIN the logins of user 1 **in the ON clause**. 3) COUNT the matches per date (0 when none).",
    "1) First login day per user (MIN). 2) Count users per first day. 3) Running SUM of those counts.",
    "1) Quantity per product (completed, unique orders). 2) RANK per category by quantity. 3) Keep rank 1 (ties stay).",
    "`LAG(status)` over each customer's orders gives the previous status; keep rows where status = previous status.",
]
