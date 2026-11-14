MONTHLY_BQ = """
WITH monthly AS (
    SELECT DATE_TRUNC(order_date, MONTH) AS month, SUM(amount) AS revenue
    FROM (SELECT DISTINCT order_id, order_date, amount, status FROM practice.orders) o
    WHERE status = 'completed'
    GROUP BY DATE_TRUNC(order_date, MONTH)
)"""
MONTHLY = """
WITH monthly AS (
    SELECT TRUNC(order_date, 'MM') AS month, SUM(amount) AS revenue
    FROM (SELECT DISTINCT order_id, order_date, amount, status FROM orders) o
    WHERE status = 'completed'
    GROUP BY TRUNC(order_date, 'MM')
)"""
MONTHLY_DF = """
monthly = (orders.dropDuplicates().filter(F.col("status") == "completed")
           .groupBy(F.trunc("order_date", "MM").alias("month"))
           .agg(F.sum("amount").alias("revenue")))
"""

TOPIC = dict(
    title="05 · Window functions",
    intro="""
A **window function** computes a value from a group of rows (the *window*) but **keeps every row**
(GROUP BY would squash them into one).

```
function() OVER (PARTITION BY <groups> ORDER BY <sort> ROWS BETWEEN ... )
```
- `PARTITION BY` = restart for each group (like GROUP BY, but rows stay)
- `ORDER BY` = the order inside the group (needed for ranking, running totals, LAG/LEAD)
- frame (`ROWS BETWEEN ...`) = which rows around the current row to use

You **cannot** use a window function in WHERE. Wrap it in a subquery/CTE and filter outside.
BigQuery (and Oracle 23ai) also has `QUALIFY`. Spark SQL has no QUALIFY.
""",
    questions=[
        Q("ROW_NUMBER vs RANK vs DENSE_RANK",
          "For department 2 (Sales), show `name`, `salary` and three columns ordered by salary (high first):\n"
          "`rn` (ROW_NUMBER, ties broken by name), `rnk` (RANK), `drnk` (DENSE_RANK). "
          "Look at the 90,000 tie: what does each function do?",
          level="easy",
          sql="""
SELECT name, salary,
       ROW_NUMBER() OVER (ORDER BY salary DESC, name) AS rn,
       RANK()       OVER (ORDER BY salary DESC)       AS rnk,
       DENSE_RANK() OVER (ORDER BY salary DESC)       AS drnk
FROM employees
WHERE dept_id = 2
ORDER BY rn""",
          sdf="""
w = Window.orderBy(F.desc("salary"))
res = (employees.filter("dept_id = 2")
       .select("name", "salary",
               F.row_number().over(Window.orderBy(F.desc("salary"), "name")).alias("rn"),
               F.rank().over(w).alias("rnk"),
               F.dense_rank().over(w).alias("drnk"))
       .orderBy("rn"))""",
          note="Ties 90k ×3: ROW_NUMBER → 2,3,4 (always different). RANK → 2,2,2 then **5** (skips). "
               "DENSE_RANK → 2,2,2 then **3** (no gaps). Most asked window question.",
          notes={"spark": "A window with ORDER BY but no PARTITION BY moves all rows to one partition. "
                          "OK for small data; slow for big data (Spark warns you)."},
          ordered=True),

        Q("Top 2 salary levels per department",
          "For each department (skip NULL dept_id) show employees whose salary is in the "
          "**top 2 different salary values** of that department. Show `dept_id`, `name`, `salary`.",
          level="medium",
          sql="""
SELECT dept_id, name, salary
FROM (SELECT dept_id, name, salary,
             DENSE_RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS drnk
      FROM employees
      WHERE dept_id IS NOT NULL) t
WHERE drnk <= 2
ORDER BY dept_id, salary DESC, name""",
          bq="""
SELECT dept_id, name, salary
FROM practice.employees
WHERE dept_id IS NOT NULL
QUALIFY DENSE_RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) <= 2
ORDER BY dept_id, salary DESC, name""",
          sdf="""
w = Window.partitionBy("dept_id").orderBy(F.desc("salary"))
res = (employees.filter(F.col("dept_id").isNotNull())
       .withColumn("drnk", F.dense_rank().over(w))
       .filter("drnk <= 2")
       .select("dept_id", "name", "salary")
       .orderBy("dept_id", F.desc("salary"), "name"))""",
          note="\"Top N **people**\" → ROW_NUMBER. \"Top N **salaries**, keep ties\" → DENSE_RANK. Ask which one!",
          notes={"bigquery": "`QUALIFY` filters on a window function, like HAVING does for GROUP BY. "
                             "Without QUALIFY use the subquery way (works everywhere)."},
          ordered=True),

        Q("Third highest salary",
          "Show the **3rd highest different** salary in the company as `third_highest`.",
          level="medium",
          sql="""
SELECT DISTINCT salary AS third_highest
FROM (SELECT salary, DENSE_RANK() OVER (ORDER BY salary DESC) AS drnk
      FROM employees) t
WHERE drnk = 3""",
          sdf="""
res = (employees.withColumn("drnk", F.dense_rank().over(Window.orderBy(F.desc("salary"))))
       .filter("drnk = 3").select(F.col("salary").alias("third_highest")).distinct())""",
          note="DENSE_RANK + DISTINCT, because several people can share the 3rd salary. "
               "Other ways: `OFFSET 2` on distinct salaries; or a correlated COUNT(DISTINCT)."),

        Q("Running total of revenue",
          "For **completed** orders in 2024 (each order once), show `order_date`, `daily_revenue` "
          "and `running_total` (sum of all days so far in 2024).",
          level="medium",
          sql="""
WITH daily AS (
    SELECT order_date, SUM(amount) AS daily_revenue
    FROM (SELECT DISTINCT order_id, order_date, amount, status FROM orders) o
    WHERE status = 'completed'
      AND order_date >= DATE '2024-01-01' AND order_date < DATE '2025-01-01'
    GROUP BY order_date
)
SELECT order_date, daily_revenue,
       SUM(daily_revenue) OVER (ORDER BY order_date
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM daily
ORDER BY order_date""",
          sdf="""
daily = (orders.dropDuplicates()
         .filter((F.col("status") == "completed") & (F.year("order_date") == 2024))
         .groupBy("order_date").agg(F.sum("amount").alias("daily_revenue")))
w = Window.orderBy("order_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
res = daily.withColumn("running_total", F.sum("daily_revenue").over(w)).orderBy("order_date")""",
          note="Aggregate to one row per day **first**, then run the window. "
               "A day with only NULL amounts gives NULL daily revenue; SUM over the window skips it.",
          ordered=True),

        Q("Salary vs department average",
          "Show `name`, `dept_id`, `salary`, `dept_avg` (2 decimals) and `diff` = salary − dept_avg "
          "(2 decimals) for employees with a department.",
          level="easy",
          sql="""
SELECT name, dept_id, salary,
       ROUND(AVG(salary) OVER (PARTITION BY dept_id), 2)          AS dept_avg,
       ROUND(salary - AVG(salary) OVER (PARTITION BY dept_id), 2) AS diff
FROM employees
WHERE dept_id IS NOT NULL""",
          sdf="""
w = Window.partitionBy("dept_id")
res = (employees.filter(F.col("dept_id").isNotNull())
       .withColumn("a", F.avg("salary").over(w))
       .select("name", "dept_id", "salary",
               F.round("a", 2).alias("dept_avg"),
               F.round(F.col("salary") - F.col("a"), 2).alias("diff")))""",
          note="With PARTITION BY and **no** ORDER BY, the window is the whole group: every row gets the group total/average."),

        Q("Share of department payroll",
          "Show `name`, `dept_id`, `salary` and `pct_of_dept` = salary as % of the department's total "
          "salary, rounded to 1 decimal. Skip NULL dept_id.",
          level="easy",
          sql="""
SELECT name, dept_id, salary,
       ROUND(100 * salary / SUM(salary) OVER (PARTITION BY dept_id), 1) AS pct_of_dept
FROM employees
WHERE dept_id IS NOT NULL""",
          sdf="""
res = (employees.filter(F.col("dept_id").isNotNull())
       .withColumn("pct_of_dept",
                   F.round(100 * F.col("salary") / F.sum("salary").over(Window.partitionBy("dept_id")), 1))
       .select("name", "dept_id", "salary", "pct_of_dept"))"""),

        Q("Days since the previous order (LAG)",
          "For each customer's orders (each order once), show `customer_id`, `order_id`, `order_date`, "
          "`prev_order_date` and `days_since_prev` (whole days). The first order of a customer has NULLs.",
          level="medium",
          bq="""
SELECT customer_id, order_id, order_date,
       LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date, order_id) AS prev_order_date,
       DATE_DIFF(order_date,
                 LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date, order_id),
                 DAY) AS days_since_prev
FROM (SELECT DISTINCT order_id, customer_id, order_date FROM practice.orders)""",
          ora="""
SELECT customer_id, order_id, order_date,
       LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date, order_id) AS prev_order_date,
       order_date - LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date, order_id)
           AS days_since_prev
FROM (SELECT DISTINCT order_id, customer_id, order_date FROM orders)""",
          ssql="""
SELECT customer_id, order_id, order_date,
       LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date, order_id) AS prev_order_date,
       DATEDIFF(order_date,
                LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date, order_id))
           AS days_since_prev
FROM (SELECT DISTINCT order_id, customer_id, order_date FROM orders)""",
          sdf="""
w = Window.partitionBy("customer_id").orderBy("order_date", "order_id")
res = (orders.select("order_id", "customer_id", "order_date").distinct()
       .withColumn("prev_order_date", F.lag("order_date").over(w))
       .withColumn("days_since_prev", F.datediff("order_date", "prev_order_date"))
       .select("customer_id", "order_id", "order_date", "prev_order_date", "days_since_prev"))""",
          note="`LAG(col, n, default)` = value n rows **before**. `LEAD` = n rows **after**.",
          notes={"bigquery": "Date difference: `DATE_DIFF(later, earlier, DAY)`.",
                 "oracle": "Date difference: `later - earlier` (a number of days). "
                           "Oracle DATE has a time part; here it is always 00:00.",
                 "spark": "Date difference: `DATEDIFF(later, earlier)`. "
                          "Subquery alias is optional in Spark and BigQuery; Oracle allows no alias too."}),

        Q("Month-over-month change",
          "Monthly revenue of completed orders (each order once). Show `month` (first day of the month), "
          "`revenue`, `prev_revenue`, and `pct_change` = (revenue − prev) / prev × 100, 1 decimal.",
          level="hard",
          bq=MONTHLY_BQ + """
SELECT month, revenue,
       LAG(revenue) OVER (ORDER BY month) AS prev_revenue,
       ROUND(100 * (revenue - LAG(revenue) OVER (ORDER BY month))
                 / LAG(revenue) OVER (ORDER BY month), 1) AS pct_change
FROM monthly
ORDER BY month""",
          ora=MONTHLY + """
SELECT month, revenue,
       LAG(revenue) OVER (ORDER BY month) AS prev_revenue,
       ROUND(100 * (revenue - LAG(revenue) OVER (ORDER BY month))
                 / LAG(revenue) OVER (ORDER BY month), 1) AS pct_change
FROM monthly
ORDER BY month""",
          ssql=MONTHLY + """
SELECT month, revenue,
       LAG(revenue) OVER (ORDER BY month) AS prev_revenue,
       ROUND(100 * (revenue - LAG(revenue) OVER (ORDER BY month))
                 / LAG(revenue) OVER (ORDER BY month), 1) AS pct_change
FROM monthly
ORDER BY month""",
          sdf=MONTHLY_DF + """w = Window.orderBy("month")
res = (monthly.withColumn("prev_revenue", F.lag("revenue").over(w))
       .withColumn("pct_change",
                   F.round(100 * (F.col("revenue") - F.col("prev_revenue")) / F.col("prev_revenue"), 1))
       .orderBy("month"))""",
          note="Months with no completed orders are **missing**, so LAG compares with an older month. "
               "To be exact, build a full month calendar first (topic 08). "
               "Division by zero: BigQuery has `SAFE_DIVIDE`; elsewhere use `NULLIF(prev, 0)`.",
          notes={"bigquery": "First day of month: `DATE_TRUNC(d, MONTH)`.",
                 "oracle": "First day of month: `TRUNC(d, 'MM')`.",
                 "spark": "First day of month: `TRUNC(d, 'MM')` (returns a date). "
                          "`DATE_TRUNC('MONTH', d)` returns a timestamp."},
          ordered=True),

        Q("3-month moving average",
          "Same monthly revenue. Show `month`, `revenue`, and `moving_avg_3` = average of this month and "
          "the 2 months before it (by row), 2 decimals.",
          level="medium",
          bq=MONTHLY_BQ + """
SELECT month, revenue,
       ROUND(AVG(revenue) OVER (ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS moving_avg_3
FROM monthly
ORDER BY month""",
          ora=MONTHLY + """
SELECT month, revenue,
       ROUND(AVG(revenue) OVER (ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS moving_avg_3
FROM monthly
ORDER BY month""",
          ssql=MONTHLY + """
SELECT month, revenue,
       ROUND(AVG(revenue) OVER (ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS moving_avg_3
FROM monthly
ORDER BY month""",
          sdf=MONTHLY_DF + """w = Window.orderBy("month").rowsBetween(-2, Window.currentRow)
res = monthly.withColumn("moving_avg_3", F.round(F.avg("revenue").over(w), 2)).orderBy("month")""",
          note="`ROWS BETWEEN 2 PRECEDING AND CURRENT ROW` = 3 rows. The first 2 months average fewer rows.",
          ordered=True),

        Q("Salary quartiles (NTILE)",
          "Split all employees into 4 groups by salary (highest first): `name`, `salary`, `quartile` (1–4). "
          "Break ties by `emp_id`.",
          level="easy",
          sql="""
SELECT name, salary,
       NTILE(4) OVER (ORDER BY salary DESC, emp_id) AS quartile
FROM employees""",
          sdf="""
res = employees.select("name", "salary",
                       F.ntile(4).over(Window.orderBy(F.desc("salary"), "emp_id")).alias("quartile"))""",
          note="30 rows / 4 groups: the first 2 groups get 8 rows, the last 2 get 7."),

        Q("Top earner on every row (FIRST_VALUE)",
          "Show `name`, `dept_id`, `salary` and `top_earner` = the name of the highest-paid person in "
          "the same department (ties: first by name). Skip NULL dept_id.",
          level="medium",
          sql="""
SELECT name, dept_id, salary,
       FIRST_VALUE(name) OVER (PARTITION BY dept_id ORDER BY salary DESC, name) AS top_earner
FROM employees
WHERE dept_id IS NOT NULL""",
          sdf="""
w = Window.partitionBy("dept_id").orderBy(F.desc("salary"), "name")
res = (employees.filter(F.col("dept_id").isNotNull())
       .select("name", "dept_id", "salary", F.first("name").over(w).alias("top_earner")))""",
          note="Trap: `LAST_VALUE(...) OVER (ORDER BY ...)` gives the **current** row, because the default "
               "frame ends at the current row. Add `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`."),

        Q("Latest order per customer",
          "For each customer, show their latest order: `customer_id`, `order_id`, `order_date`, `amount`. "
          "If two orders are on the same day, take the bigger `order_id`. One row per customer.",
          level="medium",
          sql="""
SELECT customer_id, order_id, order_date, amount
FROM (SELECT o.*,
             ROW_NUMBER() OVER (PARTITION BY customer_id
                                ORDER BY order_date DESC, order_id DESC) AS rn
      FROM orders o) t
WHERE rn = 1""",
          bq="""
SELECT customer_id, order_id, order_date, amount
FROM practice.orders
WHERE TRUE
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC, order_id DESC) = 1""",
          sdf="""
w = Window.partitionBy("customer_id").orderBy(F.desc("order_date"), F.desc("order_id"))
res = (orders.withColumn("rn", F.row_number().over(w)).filter("rn = 1")
       .select("customer_id", "order_id", "order_date", "amount"))""",
          note="ROW_NUMBER = exactly one row per group. It also removes the duplicate rows for free. "
               "Always add a tie-breaker to ORDER BY, so the result is the same every run.",
          notes={"bigquery": "`WHERE TRUE` is a habit: older BigQuery needed a WHERE/GROUP BY/HAVING next to QUALIFY."}),

        Q("ROWS vs RANGE (ties in a running total)",
          "Department 2 employees ordered by salary (low → high). Show `name`, `salary`, "
          "`rows_total` (frame ROWS ... CURRENT ROW, ties broken by emp_id) and `range_total` (frame RANGE ... CURRENT ROW). "
          "Why are they different for the 90,000 tie?",
          level="hard",
          sql="""
SELECT name, salary,
       SUM(salary) OVER (ORDER BY salary, emp_id ROWS  BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rows_total,
       SUM(salary) OVER (ORDER BY salary         RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_total
FROM employees
WHERE dept_id = 2
ORDER BY salary, emp_id""",
          sdf="""
rows_w = Window.orderBy("salary", "emp_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
range_w = Window.orderBy("salary").rangeBetween(Window.unboundedPreceding, Window.currentRow)
res = (employees.filter("dept_id = 2")
       .select("name", "salary", "emp_id",
               F.sum("salary").over(rows_w).alias("rows_total"),
               F.sum("salary").over(range_w).alias("range_total"))
       .orderBy("salary", "emp_id").drop("emp_id"))""",
          note="ROWS counts physical rows. RANGE includes **all rows with the same ORDER BY value** (peers). "
               "The default frame with ORDER BY is RANGE: that is why running totals jump on ties.",
          ordered=True),
    ],
)

# "How it works" text for each question, in order (shown in the solutions).
HOW = [
    "All three number rows by salary (high first) inside one window. They differ only on ties: ROW_NUMBER never repeats, RANK repeats then skips, DENSE_RANK repeats without gaps.",
    "1) DENSE_RANK salaries inside each department (`PARTITION BY dept_id`). 2) Keep rank ≤ 2 in an outer query (or with QUALIFY in BigQuery).",
    "DENSE_RANK over all salaries (no PARTITION BY); rank 3 = the third different salary. DISTINCT shows it once.",
    "1) One row per day (SUM per date). 2) `SUM(...) OVER (ORDER BY date ROWS UNBOUNDED PRECEDING → CURRENT ROW)` adds all days so far.",
    "`AVG(salary) OVER (PARTITION BY dept_id)` puts the department average on every row; subtract it from the salary.",
    "`SUM(salary) OVER (PARTITION BY dept_id)` = department total on each row; salary / total × 100 = share.",
    "`LAG(order_date)` over each customer's orders (sorted by date) gives the previous order date; subtract the two dates for the gap.",
    "1) CTE: revenue per month. 2) `LAG(revenue)` = last month's value. 3) (this − last) / last × 100.",
    "Average over a frame of 3 rows: `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW` = this month and the 2 rows before.",
    "`NTILE(4)` sorts the rows and cuts them into 4 groups of (almost) the same size.",
    "`FIRST_VALUE(name)` over each department sorted by salary high first returns the top earner's name on every row.",
    "ROW_NUMBER per customer, newest first (`order_date DESC, order_id DESC`), then keep `rn = 1` = exactly one latest row per customer.",
    "ROWS adds one physical row at a time. RANGE adds **all rows with the same salary** at once, so tied rows show the same total.",
]
