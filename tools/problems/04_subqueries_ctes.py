TOPIC = dict(
    title="04 · Subqueries and CTEs",
    intro="""
A **subquery** is a query inside another query.
- **scalar** subquery: returns one value, e.g. `(SELECT AVG(salary) FROM employees)`
- **correlated** subquery: uses a column from the outer query, so it runs "per row"
- `IN` / `EXISTS` / `NOT EXISTS`: check if matching rows exist

A **CTE** (`WITH name AS (...)`) gives a subquery a name. It makes long queries easy to read, step by step.
""",
    questions=[
        Q("Above the company average",
          "Show `name` and `salary` of employees who earn more than the **average salary of all employees**.",
          level="easy",
          sql="""
SELECT name, salary
FROM employees
WHERE salary > (SELECT AVG(salary) FROM employees)""",
          sdf="""
avg_sal = employees.agg(F.avg("salary")).first()[0]
res = employees.filter(F.col("salary") > avg_sal).select("name", "salary")""",
          notes={"spark": "`.first()[0]` brings one value to the driver (your Python program). "
                          "Fine for one number; never do it for big data."}),

        Q("Above their department average (correlated)",
          "Show `name`, `dept_id`, `salary` of employees who earn more than the average "
          "salary **of their own department**.",
          level="medium",
          sql="""
SELECT e.name, e.dept_id, e.salary
FROM employees e
WHERE e.salary > (SELECT AVG(x.salary)
                  FROM employees x
                  WHERE x.dept_id = e.dept_id)""",
          sdf="""
dept_avg = employees.groupBy("dept_id").agg(F.avg("salary").alias("dept_avg"))
res = (employees.join(dept_avg, "dept_id")
       .filter(F.col("salary") > F.col("dept_avg"))
       .select("name", "dept_id", "salary"))""",
          note="The inner query uses `e.dept_id` from the outer query: that makes it **correlated**. "
               "The engine usually rewrites it as a join with a GROUP BY (like the DataFrame answer). "
               "The employee with NULL dept_id drops out: `x.dept_id = NULL` never matches."),

        Q("Not a manager: the NOT IN trap",
          "Show `emp_id` and `name` of employees who are **nobody's manager**.\n\n"
          "First try `WHERE emp_id NOT IN (SELECT manager_id FROM employees)`. How many rows? Why?",
          level="medium",
          sql="""
SELECT e.emp_id, e.name
FROM employees e
WHERE NOT EXISTS (SELECT 1 FROM employees x WHERE x.manager_id = e.emp_id)""",
          sdf="""
mgrs = employees.select(F.col("manager_id").alias("emp_id")).where("emp_id IS NOT NULL")
res = employees.join(mgrs, "emp_id", "left_anti").select("emp_id", "name")""",
          note="`NOT IN` with a NULL in the list returns **0 rows**: `5 NOT IN (1, NULL)` means "
               "`5 <> 1 AND 5 <> NULL`, which is unknown. Use `NOT EXISTS`, or add "
               "`WHERE manager_id IS NOT NULL` inside the subquery."),

        Q("Products in big orders (IN)",
          "Show `product_id` and `product_name` of products that appear in at least one **completed** "
          "order with amount **over 200**. Each product once.",
          level="easy",
          sql="""
SELECT product_id, product_name
FROM products
WHERE product_id IN (
    SELECT i.product_id
    FROM order_items i
    JOIN orders o ON o.order_id = i.order_id
    WHERE o.status = 'completed' AND o.amount > 200)""",
          sdf="""
big = (order_items.join(orders, "order_id")
       .filter((F.col("status") == "completed") & (F.col("amount") > 200)))
res = products.join(big, "product_id", "left_semi").select("product_id", "product_name")"""),

        Q("Departments that pay above average",
          "Show `dept_id` and `avg_salary` (2 decimals) of departments whose average salary is higher "
          "than the company-wide average salary.",
          level="medium",
          sql="""
SELECT dept_id, ROUND(AVG(salary), 2) AS avg_salary
FROM employees
WHERE dept_id IS NOT NULL
GROUP BY dept_id
HAVING AVG(salary) > (SELECT AVG(salary) FROM employees)""",
          sdf="""
company = employees.agg(F.avg("salary")).first()[0]
res = (employees.filter(F.col("dept_id").isNotNull())
       .groupBy("dept_id").agg(F.avg("salary").alias("a"))
       .filter(F.col("a") > company)
       .select("dept_id", F.round("a", 2).alias("avg_salary")))"""),

        Q("Customers who spend above average (CTE)",
          "Use a CTE. Step 1: total completed amount per customer (count each order once). "
          "Step 2: keep customers whose total is above the **average of those totals**. "
          "Show `customer_id`, `total_spent`. Highest first.",
          level="medium",
          sql="""
WITH clean_orders AS (
    SELECT DISTINCT order_id, customer_id, amount, status
    FROM orders
),
spend AS (
    SELECT customer_id, SUM(amount) AS total_spent
    FROM clean_orders
    WHERE status = 'completed'
    GROUP BY customer_id
)
SELECT customer_id, total_spent
FROM spend
WHERE total_spent > (SELECT AVG(total_spent) FROM spend)
ORDER BY total_spent DESC, customer_id""",
          sdf="""
spend = (orders.dropDuplicates().filter(F.col("status") == "completed")
         .groupBy("customer_id").agg(F.sum("amount").alias("total_spent")))
avg_spend = spend.agg(F.avg("total_spent")).first()[0]
res = (spend.filter(F.col("total_spent") > avg_spend)
       .orderBy(F.desc("total_spent"), "customer_id"))""",
          note="A CTE can be used more than once (`spend` is used twice here). "
               "Each CTE is one clear step: easy to test alone.",
          ordered=True),

        Q("Second highest salary (no window functions)",
          "Show the **second highest** different salary as `second_highest`. "
          "Do it without window functions.",
          level="medium",
          sql="""
SELECT MAX(salary) AS second_highest
FROM employees
WHERE salary < (SELECT MAX(salary) FROM employees)""",
          sdf="""
top = employees.agg(F.max("salary")).first()[0]
res = employees.filter(F.col("salary") < top).agg(F.max("salary").alias("second_highest"))""",
          note="Classic interview question. If there is no second salary, MAX returns NULL "
               "(one row with NULL), which is usually what they want. For the Nth salary see topic 05."),

        Q("Employee count per department (scalar subquery in SELECT)",
          "Show every `dept_name` with `num_employees`, using a **subquery in the SELECT list**. "
          "Legal must show 0.",
          level="medium",
          sql="""
SELECT d.dept_name,
       (SELECT COUNT(*) FROM employees e WHERE e.dept_id = d.dept_id) AS num_employees
FROM departments d
ORDER BY num_employees DESC, d.dept_name""",
          sdf="""
counts = employees.groupBy("dept_id").agg(F.count("*").alias("n"))
res = (departments.join(counts, "dept_id", "left")
       .select("dept_name", F.coalesce("n", F.lit(0)).alias("num_employees"))
       .orderBy(F.desc("num_employees"), "dept_name"))""",
          note="COUNT over zero rows gives 0 (not NULL). SUM over zero rows gives NULL.",
          ordered=True),

        Q("Departments with a recent hire (EXISTS)",
          "Show `dept_name` of departments with **at least one** employee hired on or after 2023-01-01.",
          level="easy",
          sql="""
SELECT d.dept_name
FROM departments d
WHERE EXISTS (SELECT 1
              FROM employees e
              WHERE e.dept_id = d.dept_id
                AND e.hire_date >= DATE '2023-01-01')""",
          sdf="""
recent = employees.filter(F.col("hire_date") >= F.lit("2023-01-01").cast("date"))
res = departments.join(recent, "dept_id", "left_semi").select("dept_name")""",
          note="`DATE '2023-01-01'` is a date literal (a fixed date written in SQL). "
               "It works in all 3 engines."),

        Q("The most recent order(s)",
          "Show `order_id`, `customer_id`, `order_date` of the order(s) on the **latest order date**. "
          "If two orders share that date, show both. No duplicate rows.",
          level="easy",
          sql="""
SELECT DISTINCT order_id, customer_id, order_date
FROM orders
WHERE order_date = (SELECT MAX(order_date) FROM orders)"""
          ,
          sdf="""
last_day = orders.agg(F.max("order_date")).first()[0]
res = orders.filter(F.col("order_date") == last_day).select("order_id", "customer_id", "order_date").distinct()"""),

        Q("Busy months of 2024 (CTE chain)",
          "For year 2024: count different orders per month (`order_month` 1–12, `num_orders`). "
          "Keep only months with **more** orders than the average month of 2024.",
          level="hard",
          sql="""
WITH per_month AS (
    SELECT EXTRACT(MONTH FROM order_date) AS order_month,
           COUNT(DISTINCT order_id)        AS num_orders
    FROM orders
    WHERE order_date >= DATE '2024-01-01' AND order_date < DATE '2025-01-01'
    GROUP BY EXTRACT(MONTH FROM order_date)
)
SELECT order_month, num_orders
FROM per_month
WHERE num_orders > (SELECT AVG(num_orders) FROM per_month)
ORDER BY order_month""",
          sdf="""
per_month = (orders.filter(F.year("order_date") == 2024)
             .groupBy(F.month("order_date").alias("order_month"))
             .agg(F.countDistinct("order_id").alias("num_orders")))
avg_n = per_month.agg(F.avg("num_orders")).first()[0]
res = per_month.filter(F.col("num_orders") > avg_n).orderBy("order_month")""",
          note="Filter dates with a **range** (`>= start AND < end`) instead of `EXTRACT(YEAR ...) = 2024`. "
               "On partitioned tables the range lets the engine skip data (partition pruning).",
          ordered=True),
    ],
)

# "How it works" text for each question, in order (shown in the solutions).
HOW = [
    "The subquery `(SELECT AVG(salary) FROM employees)` returns one number; the outer WHERE compares every salary with it.",
    "For each employee row, the subquery averages salaries of **the same** department (`x.dept_id = e.dept_id`), and WHERE keeps people above it.",
    "`NOT EXISTS` asks, for each employee: is there anyone whose `manager_id` is my `emp_id`? If not, I manage nobody. No NULL problem.",
    "The subquery lists product ids from completed orders over 200; `IN` keeps products in that list. Duplicates in the list don't matter.",
    "Group employees by department and compute the average; `HAVING` compares each group's average with the company average from a subquery.",
    "CTE 1 removes duplicates; CTE 2 sums spend per customer; the final query keeps customers above `AVG(total_spent)` of CTE 2.",
    "Take the highest salary that is **lower** than the overall maximum = the second highest different salary.",
    "For each department, the subquery in SELECT counts its employees. It returns 0 for Legal (COUNT of no rows is 0).",
    "`EXISTS` with a correlated subquery: keep a department if at least one of its employees has `hire_date >= 2023-01-01`.",
    "The subquery finds the latest date; the outer query keeps all orders on that date. DISTINCT removes duplicate rows.",
    "CTE `per_month` counts orders per month of 2024; the final query compares each month with the average of all months in the CTE.",
]
