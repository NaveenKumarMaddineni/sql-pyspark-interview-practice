TOPIC = dict(
    title="03 · Joins",
    intro="""
A **join** puts columns from two tables side by side, by matching a key.

- `INNER JOIN`: only rows that match on both sides
- `LEFT JOIN`: all rows from the left table, NULLs where the right side has no match
- `FULL OUTER JOIN`: all rows from both sides
- **anti join**: rows with **no** match (LEFT JOIN + `IS NULL`, or `NOT EXISTS`)
- **semi join**: rows that have **at least one** match, without repeating them (`EXISTS` / `IN`)

**Fan-out** = a join makes more rows than you expect (one order × many items). Then SUM gives a too-big number.
""",
    questions=[
        Q("Employee with department name",
          "Show `name` and `dept_name` for every employee who **has** a department.",
          level="easy",
          sql="""
SELECT e.name, d.dept_name
FROM employees e
JOIN departments d ON d.dept_id = e.dept_id""",
          sdf="""
res = employees.join(departments, "dept_id").select("name", "dept_name")""",
          notes={"oracle": "Oracle does not allow `AS` before a table alias: write `employees e`, not `employees AS e`."}),

        Q("Keep employees with no department",
          "Show `name` and `dept_name` for **all** employees. The one with no department must show NULL.",
          level="easy",
          sql="""
SELECT e.name, d.dept_name
FROM employees e
LEFT JOIN departments d ON d.dept_id = e.dept_id""",
          sdf="""
res = employees.join(departments, "dept_id", "left").select("name", "dept_name")"""),

        Q("Department with no employees",
          "Show the `dept_name` of departments that have **no** employees.",
          level="easy",
          sql="""
SELECT d.dept_name
FROM departments d
LEFT JOIN employees e ON e.dept_id = d.dept_id
WHERE e.emp_id IS NULL""",
          sdf="""
res = departments.join(employees, "dept_id", "left_anti").select("dept_name")""",
          note="This is an **anti join**. Test the right table's key (`e.emp_id IS NULL`), "
               "not a column that can be NULL on its own.",
          notes={"spark": "The DataFrame API has a real anti join: `how=\"left_anti\"`. "
                          "Spark SQL also has `LEFT ANTI JOIN`."}),

        Q("Customers who never ordered",
          "Show `customer_id` and `name` of customers with **no** orders. Use `NOT EXISTS`.",
          level="easy",
          sql="""
SELECT c.customer_id, c.name
FROM customers c
WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id)
ORDER BY c.customer_id""",
          sdf="""
res = customers.join(orders, "customer_id", "left_anti").select("customer_id", "name").orderBy("customer_id")""",
          note="`NOT EXISTS` is safe with NULLs. `NOT IN (subquery)` returns **nothing** if the subquery "
               "has even one NULL (see topic 04).",
          ordered=True),

        Q("Employee and manager names (self join)",
          "Show `employee` (name) and `manager` (manager's name) for **all** employees. "
          "Top bosses have no manager: show NULL.",
          level="easy",
          sql="""
SELECT e.name AS employee, m.name AS manager
FROM employees e
LEFT JOIN employees m ON m.emp_id = e.manager_id""",
          sdf="""
e, m = employees.alias("e"), employees.alias("m")
res = (e.join(m, F.col("m.emp_id") == F.col("e.manager_id"), "left")
        .select(F.col("e.name").alias("employee"), F.col("m.name").alias("manager")))""",
          note="A **self join** joins a table to itself. Give each copy its own alias.",
          notes={"spark": "Use `.alias()` on both DataFrames, then `F.col(\"e.name\")`, so the columns "
                          "do not get mixed up."}),

        Q("Close to the manager's pay",
          "Show `employee`, `salary`, `manager`, `manager_salary` for employees who earn "
          "**at least 90%** of their manager's salary.\n\n"
          "(Try first: who earns **more** than their manager? In this data: nobody, 0 rows.)",
          level="medium",
          sql="""
SELECT e.name AS employee, e.salary,
       m.name AS manager, m.salary AS manager_salary
FROM employees e
JOIN employees m ON m.emp_id = e.manager_id
WHERE e.salary >= 0.9 * m.salary""",
          sdf="""
e, m = employees.alias("e"), employees.alias("m")
res = (e.join(m, F.col("m.emp_id") == F.col("e.manager_id"))
        .filter(F.col("e.salary") >= 0.9 * F.col("m.salary"))
        .select(F.col("e.name").alias("employee"), F.col("e.salary"),
                F.col("m.name").alias("manager"), F.col("m.salary").alias("manager_salary")))"""),

        Q("Everyone and every department (FULL OUTER JOIN)",
          "Show `dept_name` and `name` (employee). Keep departments with no employees **and** "
          "employees with no department.",
          level="medium",
          sql="""
SELECT d.dept_name, e.name
FROM departments d
FULL OUTER JOIN employees e ON e.dept_id = d.dept_id""",
          sdf="""
res = departments.join(employees, "dept_id", "full").select("dept_name", "name")""",
          note="29 matches + 1 department alone (Legal) + 1 employee alone (Daniel) = 31 rows."),

        Q("Products never sold",
          "Show `product_id` and `product_name` of products that are not in `order_items`.",
          level="easy",
          sql="""
SELECT p.product_id, p.product_name
FROM products p
LEFT JOIN order_items i ON i.product_id = p.product_id
WHERE i.product_id IS NULL""",
          sdf="""
res = products.join(order_items, "product_id", "left_anti").select("product_id", "product_name")"""),

        Q("Customers who bought a book (semi join)",
          "Show `customer_id` and `name` of customers who bought **at least one** product in "
          "category `Books` (any order status). Each customer **once**.",
          level="medium",
          sql="""
SELECT c.customer_id, c.name
FROM customers c
WHERE EXISTS (
    SELECT 1
    FROM orders o
    JOIN order_items i ON i.order_id = o.order_id
    JOIN products p    ON p.product_id = i.product_id
    WHERE o.customer_id = c.customer_id
      AND p.category = 'Books')""",
          sdf="""
books = (orders.join(order_items, "order_id")
               .join(products, "product_id")
               .filter(F.col("category") == "Books"))
res = customers.join(books, "customer_id", "left_semi").select("customer_id", "name")""",
          note="If you JOIN instead of EXISTS, a customer with 3 book rows shows 3 times "
               "and you need DISTINCT. EXISTS (a **semi join**) never repeats rows."),

        Q("Revenue per category (fan-out from duplicates)",
          "Show `category` and `revenue` = SUM(quantity × price) for **completed** orders. "
          "Careful: `orders` has duplicate rows. If you join to them, items get counted twice.",
          level="hard",
          sql="""
SELECT p.category, SUM(i.quantity * p.price) AS revenue
FROM (SELECT DISTINCT order_id, status FROM orders) o
JOIN order_items i ON i.order_id = o.order_id
JOIN products p    ON p.product_id = i.product_id
WHERE o.status = 'completed'
GROUP BY p.category
ORDER BY revenue DESC""",
          sdf="""
done = orders.filter(F.col("status") == "completed").select("order_id").distinct()
res = (done.join(order_items, "order_id").join(products, "product_id")
       .groupBy("category")
       .agg(F.sum(F.col("quantity") * F.col("price")).alias("revenue"))
       .orderBy(F.desc("revenue")))""",
          note="Before a join, ask: *is the key unique on this side?* Here `order_id` is **not** unique in "
               "`orders`, so make it unique first (DISTINCT or GROUP BY).",
          ordered=True),

        Q("Amount and items per customer (fan-out)",
          "For each customer with orders, show `customer_id`, `total_amount` (sum of order amounts, "
          "each order once) and `total_items` (sum of quantity). "
          "Joining orders to items first and then summing amount gives a wrong, too-big number. Avoid it.",
          level="hard",
          sql="""
WITH o AS (
    SELECT customer_id, order_id, SUM(amount) AS amount
    FROM (SELECT DISTINCT order_id, customer_id, amount FROM orders) d
    GROUP BY customer_id, order_id
),
items AS (
    SELECT order_id, SUM(quantity) AS qty
    FROM order_items
    GROUP BY order_id
)
SELECT o.customer_id,
       SUM(o.amount) AS total_amount,
       SUM(items.qty) AS total_items
FROM o
LEFT JOIN items ON items.order_id = o.order_id
GROUP BY o.customer_id""",
          sdf="""
o = (orders.select("order_id", "customer_id", "amount").dropDuplicates()
     .groupBy("customer_id", "order_id").agg(F.sum("amount").alias("amount")))
items = order_items.groupBy("order_id").agg(F.sum("quantity").alias("qty"))
res = (o.join(items, "order_id", "left")
        .groupBy("customer_id")
        .agg(F.sum("amount").alias("total_amount"), F.sum("qty").alias("total_items")))""",
          note="Fix for fan-out: **aggregate each side to the join key first**, then join 1-to-1."),

        Q("Every category × every year (CROSS JOIN)",
          "Show `category`, `order_year` (2023, 2024, 2025) and `items_sold` = SUM(quantity) for "
          "completed orders. Show **every** category-year pair, with **0** when nothing was sold.",
          level="hard",
          sql="""
WITH years AS (
    SELECT DISTINCT EXTRACT(YEAR FROM order_date) AS order_year FROM orders
),
cats AS (
    SELECT DISTINCT category FROM products
),
sold AS (
    SELECT p.category, EXTRACT(YEAR FROM o.order_date) AS order_year, SUM(i.quantity) AS qty
    FROM (SELECT DISTINCT order_id, order_date, status FROM orders) o
    JOIN order_items i ON i.order_id = o.order_id
    JOIN products p    ON p.product_id = i.product_id
    WHERE o.status = 'completed'
    GROUP BY p.category, EXTRACT(YEAR FROM o.order_date)
)
SELECT c.category, y.order_year, COALESCE(s.qty, 0) AS items_sold
FROM cats c
CROSS JOIN years y
LEFT JOIN sold s ON s.category = c.category AND s.order_year = y.order_year
ORDER BY c.category, y.order_year""",
          sdf="""
years = orders.select(F.year("order_date").alias("order_year")).distinct()
cats = products.select("category").distinct()
sold = (orders.filter(F.col("status") == "completed")
        .select("order_id", "order_date").distinct()
        .join(order_items, "order_id").join(products, "product_id")
        .groupBy("category", F.year("order_date").alias("order_year"))
        .agg(F.sum("quantity").alias("qty")))
res = (cats.crossJoin(years)
       .join(sold, ["category", "order_year"], "left")
       .select("category", "order_year", F.coalesce("qty", F.lit(0)).alias("items_sold"))
       .orderBy("category", "order_year"))""",
          note="CROSS JOIN = every row × every row (5 categories × 3 years = 15 rows). "
               "Build the full grid first, then LEFT JOIN the facts. Same trick fills missing dates.",
          ordered=True),

        Q("Same department, same salary (pairs)",
          "Find pairs of employees in the **same department** with the **same salary**. "
          "Show `dept_id`, `emp1`, `emp2`, `salary`. Each pair once (not A-B and B-A).",
          level="medium",
          sql="""
SELECT a.dept_id, a.name AS emp1, b.name AS emp2, a.salary
FROM employees a
JOIN employees b
  ON b.dept_id = a.dept_id
 AND b.salary  = a.salary
 AND b.emp_id  > a.emp_id
ORDER BY a.dept_id, emp1, emp2""",
          sdf="""
a, b = employees.alias("a"), employees.alias("b")
res = (a.join(b, (F.col("b.dept_id") == F.col("a.dept_id"))
                 & (F.col("b.salary") == F.col("a.salary"))
                 & (F.col("b.emp_id") > F.col("a.emp_id")))
        .select("a.dept_id", F.col("a.name").alias("emp1"), F.col("b.name").alias("emp2"), "a.salary")
        .orderBy("dept_id", "emp1", "emp2"))""",
          note="`b.emp_id > a.emp_id` removes self-pairs (A-A) and mirror pairs (B-A). "
               "A 3-way tie (Sales, 90,000) makes 3 pairs.",
          ordered=True),
    ],
)

# "How it works" text for each question, in order (shown in the solutions).
HOW = [
    "Inner join on `dept_id`: each employee gets the matching department row. The employee with NULL `dept_id` has no match and drops out.",
    "LEFT JOIN keeps every employee. When there is no matching department, the department columns are NULL.",
    "LEFT JOIN departments → employees, then keep rows where the employee side is NULL = departments with no match (anti join).",
    "`NOT EXISTS` checks, for each customer, that no order row has their id. Customers 33–40 pass.",
    "Join the table to itself: `e` = the employee, `m` = the manager, matched by `m.emp_id = e.manager_id`. LEFT JOIN keeps the top bosses.",
    "Self join employee → manager, then compare the two salaries in WHERE (`e.salary >= 0.9 * m.salary`).",
    "FULL OUTER JOIN keeps matches + unmatched rows from **both** sides: Legal (no employees) and Daniel (no department).",
    "LEFT JOIN products → order_items and keep rows with no item = never sold.",
    "For each customer, `EXISTS` looks for at least one order line with a Books product (orders → order_items → products). Each customer appears once.",
    "1) Make orders unique per order_id first (DISTINCT). 2) Join to order_items and products. 3) Keep completed. 4) SUM(quantity × price) per category.",
    "Aggregate **each side to the order level first** (amount per order, quantity per order), then join 1-to-1 and sum per customer. No row is counted twice.",
    "1) Build all categories and all years. 2) CROSS JOIN = every pair. 3) LEFT JOIN the real sales per pair. 4) COALESCE missing sales to 0.",
    "Self join on same department **and** same salary; `b.emp_id > a.emp_id` keeps each pair once and removes pairing a person with themselves.",
]
