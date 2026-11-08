TOPIC = dict(
    title="02 · Aggregation: GROUP BY and HAVING",
    intro="""
**Aggregate functions** (COUNT, SUM, AVG, MIN, MAX) turn many rows into one value.
`GROUP BY` makes one row per group. `HAVING` filters groups (WHERE filters rows, before grouping).

Traps in this data: NULL amounts in `orders`, 4 exact duplicate rows in `orders`, one employee with NULL `dept_id`.
""",
    questions=[
        Q("Head count per department",
          "Show `dept_id` and the number of employees `num_employees` for each department id. "
          "Keep the group for employees with no department (NULL).",
          level="easy",
          sql="""
SELECT dept_id, COUNT(*) AS num_employees
FROM employees
GROUP BY dept_id
ORDER BY dept_id""",
          sdf="""
res = employees.groupBy("dept_id").agg(F.count("*").alias("num_employees")).orderBy("dept_id")""",
          note="GROUP BY puts all NULLs into one group. ORDER BY puts NULL first in BigQuery and Spark, "
               "but **last** in Oracle (use `NULLS FIRST` / `NULLS LAST` to choose)."),

        Q("Salary stats per department",
          "For each `dept_id` (skip NULL) show `num_employees`, `total_salary`, `avg_salary` "
          "(rounded to 2 decimals), `min_salary`, `max_salary`.",
          level="easy",
          sql="""
SELECT dept_id,
       COUNT(*)               AS num_employees,
       SUM(salary)            AS total_salary,
       ROUND(AVG(salary), 2)  AS avg_salary,
       MIN(salary)            AS min_salary,
       MAX(salary)            AS max_salary
FROM employees
WHERE dept_id IS NOT NULL
GROUP BY dept_id
ORDER BY dept_id""",
          sdf="""
res = (employees.filter(F.col("dept_id").isNotNull())
       .groupBy("dept_id")
       .agg(F.count("*").alias("num_employees"),
            F.sum("salary").alias("total_salary"),
            F.round(F.avg("salary"), 2).alias("avg_salary"),
            F.min("salary").alias("min_salary"),
            F.max("salary").alias("max_salary"))
       .orderBy("dept_id"))"""),

        Q("Three kinds of COUNT",
          "On the `orders` table, in one row, show:\n"
          "- `all_rows` = number of rows\n- `rows_with_amount` = rows where amount is not NULL\n"
          "- `unique_orders` = number of different order ids",
          level="medium",
          sql="""
SELECT COUNT(*)                 AS all_rows,
       COUNT(amount)            AS rows_with_amount,
       COUNT(DISTINCT order_id) AS unique_orders
FROM orders""",
          sdf="""
res = orders.agg(F.count("*").alias("all_rows"),
                 F.count("amount").alias("rows_with_amount"),
                 F.countDistinct("order_id").alias("unique_orders"))""",
          note="`COUNT(*)` counts rows. `COUNT(col)` skips NULLs. `COUNT(DISTINCT col)` counts different values. "
               "Here 108 rows but only 104 orders, because of 4 duplicate rows."),

        Q("Orders per status",
          "Show each `status` and how many orders `num_orders` have it. Most common first.",
          level="easy",
          sql="""
SELECT status, COUNT(*) AS num_orders
FROM orders
GROUP BY status
ORDER BY num_orders DESC, status""",
          sdf="""
res = orders.groupBy("status").count().withColumnRenamed("count", "num_orders") \\
            .orderBy(F.desc("num_orders"), "status")""",
          ordered=True),

        Q("Big departments (HAVING)",
          "Show `dept_id` and `num_employees` for departments with **more than 4** employees.",
          level="easy",
          sql="""
SELECT dept_id, COUNT(*) AS num_employees
FROM employees
WHERE dept_id IS NOT NULL
GROUP BY dept_id
HAVING COUNT(*) > 4""",
          sdf="""
res = (employees.filter(F.col("dept_id").isNotNull())
       .groupBy("dept_id").agg(F.count("*").alias("num_employees"))
       .filter(F.col("num_employees") > 4))""",
          note="You cannot use an aggregate in WHERE: WHERE runs **before** GROUP BY. "
               "Order of running: FROM → WHERE → GROUP BY → HAVING → SELECT → ORDER BY."),

        Q("Completed vs cancelled per customer",
          "For each `customer_id` that has orders, show `completed_orders` and `cancelled_orders` "
          "(count of rows with that status) in **one** query.",
          level="medium",
          sql="""
SELECT customer_id,
       SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed_orders,
       SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_orders
FROM orders
GROUP BY customer_id""",
          sdf="""
res = (orders.groupBy("customer_id")
       .agg(F.sum(F.when(F.col("status") == "completed", 1).otherwise(0)).alias("completed_orders"),
            F.sum(F.when(F.col("status") == "cancelled", 1).otherwise(0)).alias("cancelled_orders")))""",
          note="This is called **conditional aggregation**. It works everywhere. "
               "Shorter versions: BigQuery `COUNTIF(status = 'completed')`, Spark `count_if(...)`. "
               "In Spark you cannot `SUM` a true/false value."),

        Q("AVG and NULL",
          "In one row show `avg_ignoring_null` = AVG(amount) and `avg_null_as_zero` = average when "
          "a missing amount counts as 0. Round both to 2 decimals. Why are they different?",
          level="medium",
          sql="""
SELECT ROUND(AVG(amount), 2)              AS avg_ignoring_null,
       ROUND(AVG(COALESCE(amount, 0)), 2) AS avg_null_as_zero
FROM orders""",
          sdf="""
res = orders.agg(F.round(F.avg("amount"), 2).alias("avg_ignoring_null"),
                 F.round(F.avg(F.coalesce("amount", F.lit(0))), 2).alias("avg_null_as_zero"))""",
          note="AVG (like SUM, MIN, MAX) **skips NULL** rows: it divides by 102, not 108. "
               "Ask the interviewer which one the business wants."),

        Q("Completed revenue per year (duplicates!)",
          "Show `order_year` and `revenue` = total amount of **completed** orders per year. "
          "The orders table has exact duplicate rows: count each order only once.",
          level="medium",
          sql="""
SELECT EXTRACT(YEAR FROM order_date) AS order_year,
       SUM(amount)                    AS revenue
FROM (SELECT DISTINCT order_id, customer_id, order_date, amount, status
      FROM orders) o
WHERE status = 'completed'
GROUP BY EXTRACT(YEAR FROM order_date)
ORDER BY order_year""",
          sdf="""
res = (orders.dropDuplicates()
       .filter(F.col("status") == "completed")
       .groupBy(F.year("order_date").alias("order_year"))
       .agg(F.sum("amount").alias("revenue"))
       .orderBy("order_year"))""",
          note="Always check for duplicates before you SUM. Without the DISTINCT the revenue is too high. "
               "Oracle 19c cannot `GROUP BY order_year` (the alias), so repeat the expression; "
               "BigQuery and Spark allow the alias.",
          ordered=True),

        Q("Loyal customers",
          "Show `customer_id` and `num_orders` for customers with **at least 5 different orders**. "
          "Most orders first, then customer_id.",
          level="medium",
          sql="""
SELECT customer_id, COUNT(DISTINCT order_id) AS num_orders
FROM orders
GROUP BY customer_id
HAVING COUNT(DISTINCT order_id) >= 5
ORDER BY num_orders DESC, customer_id""",
          sdf="""
res = (orders.groupBy("customer_id")
       .agg(F.countDistinct("order_id").alias("num_orders"))
       .filter("num_orders >= 5")
       .orderBy(F.desc("num_orders"), "customer_id"))""",
          ordered=True),

        Q("Hires per department per year",
          "Show `dept_id`, `hire_year`, `num_hired` (skip NULL dept_id). Sort by dept_id, hire_year.",
          level="easy",
          sql="""
SELECT dept_id, EXTRACT(YEAR FROM hire_date) AS hire_year, COUNT(*) AS num_hired
FROM employees
WHERE dept_id IS NOT NULL
GROUP BY dept_id, EXTRACT(YEAR FROM hire_date)
ORDER BY dept_id, hire_year""",
          sdf="""
res = (employees.filter(F.col("dept_id").isNotNull())
       .groupBy("dept_id", F.year("hire_date").alias("hire_year"))
       .agg(F.count("*").alias("num_hired"))
       .orderBy("dept_id", "hire_year"))""",
          note="Every column in SELECT that is not inside an aggregate must be in GROUP BY.",
          ordered=True),

        Q("Categories with no cheap products",
          "Show `category` and `cheapest` (the lowest price) for categories where "
          "**every** product costs at least 15.",
          level="medium",
          sql="""
SELECT category, MIN(price) AS cheapest
FROM products
GROUP BY category
HAVING MIN(price) >= 15""",
          sdf="""
res = (products.groupBy("category").agg(F.min("price").alias("cheapest"))
       .filter(F.col("cheapest") >= 15))""",
          note="\"Every product ≥ 15\" is the same as \"the cheapest product ≥ 15\". "
               "This MIN/MAX trick answers many \"all / none\" questions."),

        Q("Subtotals with ROLLUP",
          "Show `category`, `num_products` and `avg_price` (2 decimals) per category, **plus one "
          "extra row** for all products together (category = NULL).",
          level="hard",
          sql="""
SELECT category,
       COUNT(*)              AS num_products,
       ROUND(AVG(price), 2)  AS avg_price
FROM products
GROUP BY ROLLUP (category)
ORDER BY category""",
          sdf="""
res = (products.rollup("category")
       .agg(F.count("*").alias("num_products"), F.round(F.avg("price"), 2).alias("avg_price"))
       .orderBy("category"))""",
          note="`ROLLUP(a, b)` adds subtotals for (a) and a grand total. `CUBE` adds every combination. "
               "`GROUPING SETS` lets you list the groups yourself. All 3 engines support them."),
    ],
)

# "How it works" text for each question, in order (shown in the solutions).
HOW = [
    "`GROUP BY dept_id` makes one group per department id (NULL is its own group); `COUNT(*)` counts the rows in each group.",
    "Remove the NULL department, group by `dept_id`, and compute all five aggregates in the same SELECT.",
    "One scan, three counts: `COUNT(*)` counts rows, `COUNT(amount)` skips NULLs, `COUNT(DISTINCT order_id)` counts different ids.",
    "Group by `status`, count each group, then sort by the count (biggest first) with `status` as tie-breaker.",
    "Group and count first; `HAVING COUNT(*) > 4` then filters the **groups**. (WHERE can't do it: it runs before grouping.)",
    "`CASE WHEN status = 'completed' THEN 1 ELSE 0 END` gives 1 for matching rows; `SUM` adds the 1s = a count per condition, all in one GROUP BY.",
    "`AVG(amount)` divides by rows **with** an amount. `AVG(COALESCE(amount, 0))` turns NULL into 0 first, so it divides by all rows and the result is lower.",
    "1) Remove exact duplicate rows (`SELECT DISTINCT *` in a subquery). 2) Keep completed orders. 3) Group by the year and SUM the amount.",
    "Count **different** order ids per customer (duplicates must not count twice), keep groups with `>= 5`, sort.",
    "Group by two things at once (department and hire year): one output row per pair.",
    "\"All products cost ≥ 15\" means the cheapest one does. So group by category, compute `MIN(price)`, and keep groups with `HAVING MIN(price) >= 15`.",
    "`ROLLUP(category)` makes the normal per-category groups **plus** one grand-total group where category is NULL.",
]
