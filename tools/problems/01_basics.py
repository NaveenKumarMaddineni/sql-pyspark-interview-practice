TOPIC = dict(
    title="01 · SQL basics",
    intro="""
SELECT, WHERE, ORDER BY, DISTINCT, CASE, LIMIT, and **NULL** (a missing value).

Remember: `NULL = NULL` is not true. Use `IS NULL` / `IS NOT NULL`.
""",
    questions=[
        Q("High earners in Engineering",
          "Show `name` and `salary` of employees in department 1 (Engineering) "
          "who earn **more than** 100,000. Highest salary first.",
          level="easy",
          sql="""
SELECT name, salary
FROM employees
WHERE dept_id = 1
  AND salary > 100000
ORDER BY salary DESC, name""",
          sdf="""
res = (employees
       .filter((F.col("dept_id") == 1) & (F.col("salary") > 100000))
       .select("name", "salary")
       .orderBy(F.desc("salary"), "name"))""",
          notes={"spark": "In the DataFrame API use `&` and `|` (not `and`/`or`) and put each condition in brackets."},
          ordered=True),

        Q("Cities we sell to",
          "List each customer `city` **once**, A to Z. Leave out the missing (NULL) city.",
          level="easy",
          sql="""
SELECT DISTINCT city
FROM customers
WHERE city IS NOT NULL
ORDER BY city""",
          sdf="""
res = (customers
       .filter(F.col("city").isNotNull())
       .select("city").distinct()
       .orderBy("city"))""",
          note="`DISTINCT` keeps NULL as one value, so filter it out yourself.",
          ordered=True),

        Q("Top bosses",
          "Show the `emp_id` and `name` of employees who have **no manager**.",
          level="easy",
          sql="""
SELECT emp_id, name
FROM employees
WHERE manager_id IS NULL""",
          sdf="""
res = employees.filter(F.col("manager_id").isNull()).select("emp_id", "name")""",
          note="`WHERE manager_id = NULL` returns **0 rows**: any comparison with NULL is unknown, not true."),

        Q("Mid-price electronics and books",
          "Show `product_name`, `category`, `price` for products in category "
          "Electronics or Books with price **from 20 to 50** (both included). Cheapest first, then by name.",
          level="easy",
          sql="""
SELECT product_name, category, price
FROM products
WHERE category IN ('Electronics', 'Books')
  AND price BETWEEN 20 AND 50
ORDER BY price, product_name""",
          sdf="""
res = (products
       .filter(F.col("category").isin("Electronics", "Books")
               & F.col("price").between(20, 50))
       .select("product_name", "category", "price")
       .orderBy("price", "product_name"))""",
          note="`BETWEEN` includes both ends.",
          ordered=True),

        Q("Names that start with A",
          "Show `customer_id` and `name` of customers whose name starts with the letter **A**.",
          level="easy",
          sql="""
SELECT customer_id, name
FROM customers
WHERE name LIKE 'A%'""",
          sdf="""
res = customers.filter(F.col("name").startswith("A")).select("customer_id", "name")""",
          note="`%` = any number of characters, `_` = exactly one character. "
               "LIKE is case-sensitive in BigQuery, Oracle and Spark."),

        Q("Salary bands",
          "For every employee show `name`, `salary` and `band`:\n"
          "- `high` when salary is 150,000 or more\n- `mid` when 90,000 or more\n- otherwise `low`",
          level="easy",
          sql="""
SELECT name, salary,
       CASE WHEN salary >= 150000 THEN 'high'
            WHEN salary >= 90000  THEN 'mid'
            ELSE 'low'
       END AS band
FROM employees""",
          sdf="""
res = employees.select(
    "name", "salary",
    F.when(F.col("salary") >= 150000, "high")
     .when(F.col("salary") >= 90000, "mid")
     .otherwise("low").alias("band"))""",
          note="CASE checks the conditions from top to bottom and stops at the first true one, "
               "so the order of the WHEN lines matters."),

        Q("Missing amounts",
          "Show `order_id`, `status` and `amount` for orders whose amount is missing. "
          "Show the amount as **0** instead of NULL.",
          level="easy",
          sql="""
SELECT order_id, status, COALESCE(amount, 0) AS amount
FROM orders
WHERE amount IS NULL""",
          sdf="""
res = (orders.filter(F.col("amount").isNull())
       .select("order_id", "status", F.coalesce("amount", F.lit(0)).alias("amount")))""",
          note="`COALESCE(a, b)` returns the first value that is not NULL. It works in all 3 engines. "
               "Engine versions: BigQuery `IFNULL`, Oracle `NVL`, Spark `nvl`."),

        Q("Top 5 salaries",
          "Show the 5 highest-paid employees: `name`, `salary`.",
          level="easy",
          sql="""
SELECT name, salary
FROM employees
ORDER BY salary DESC
LIMIT 5""",
          ora="""
SELECT name, salary
FROM employees
ORDER BY salary DESC
FETCH FIRST 5 ROWS ONLY""",
          sdf="""
res = employees.select("name", "salary").orderBy(F.desc("salary")).limit(5)""",
          notes={"oracle": "Oracle has no `LIMIT`. Use `FETCH FIRST n ROWS ONLY` (12c and later). "
                           "Old way: `WHERE ROWNUM <= 5` on a sorted subquery."},
          note="If the 5th and 6th salaries were equal, LIMIT would cut one of them at random. "
               "For ties use a window function (topic 05).",
          ordered=True),

        Q("Page 2 of the product list",
          "Products sorted by `price` (highest first), then `product_id`. "
          "Show **page 2** with 5 products per page (rows 6 to 10): `product_id`, `product_name`, `price`.",
          level="medium",
          sql="""
SELECT product_id, product_name, price
FROM products
ORDER BY price DESC, product_id
LIMIT 5 OFFSET 5""",
          ora="""
SELECT product_id, product_name, price
FROM products
ORDER BY price DESC, product_id
OFFSET 5 ROWS FETCH NEXT 5 ROWS ONLY""",
          sdf="""
res = (products.select("product_id", "product_name", "price")
       .orderBy(F.desc("price"), "product_id")
       .offset(5).limit(5))""",
          note="Always add a unique column (here `product_id`) to ORDER BY when paging. "
               "Without it, rows with the same price can move between pages.",
          ordered=True),

        Q("Everyone outside Engineering (NULL trap)",
          "Show `emp_id`, `name`, `dept_id` of employees who are **not** in department 1. "
          "The employee with no department (NULL) must be in the result too.",
          level="medium",
          sql="""
SELECT emp_id, name, dept_id
FROM employees
WHERE dept_id <> 1
   OR dept_id IS NULL""",
          sdf="""
res = (employees
       .filter((F.col("dept_id") != 1) | F.col("dept_id").isNull())
       .select("emp_id", "name", "dept_id"))""",
          note="`dept_id <> 1` alone skips the NULL row: `NULL <> 1` is unknown, and WHERE keeps only true. "
               "Interviewers love this trap. Spark also has a null-safe `<=>` / `eqNullSafe`."),

        Q("Label with a default",
          "For customers 1 to 5 show `customer_id` and one text column `label` like "
          "`Aarav Nguyen (Chicago)`. If the city is NULL write `unknown` instead.",
          level="easy",
          sql="""
SELECT customer_id,
       name || ' (' || COALESCE(city, 'unknown') || ')' AS label
FROM customers
WHERE customer_id BETWEEN 1 AND 5
ORDER BY customer_id""",
          sdf="""
res = (customers.filter(F.col("customer_id").between(1, 5))
       .select("customer_id",
               F.concat(F.col("name"), F.lit(" ("),
                        F.coalesce("city", F.lit("unknown")), F.lit(")")).alias("label"))
       .orderBy("customer_id"))""",
          note="`||` joins text in all 3 engines. Careful: in BigQuery and Spark `'a' || NULL` is NULL, "
               "but Oracle treats NULL as an empty string here. That is why we use COALESCE.",
          ordered=True),
    ],
)

# "How it works" text for each question, in order (shown in the solutions).
HOW = [
    "1) `WHERE` keeps rows where **both** conditions are true (`AND`). 2) `ORDER BY salary DESC` puts the highest first; `name` breaks ties so the order is always the same.",
    "1) Filter out the NULL city first. 2) `DISTINCT` keeps one copy of each city. 3) Sort A→Z.",
    "Rows with no manager have `manager_id` = NULL. `IS NULL` is the only test that finds them.",
    "`IN (...)` = equal to any value in the list. `BETWEEN 20 AND 50` = `>= 20 AND <= 50`. Both conditions must be true.",
    "`LIKE 'A%'` = starts with A, then anything. In the DataFrame API `startswith` does the same.",
    "`CASE` tests each `WHEN` from top to bottom and returns the first match; `ELSE` catches the rest. Because 150k is tested first, the 90k rule only sees salaries below 150k.",
    "1) `WHERE amount IS NULL` finds the missing amounts. 2) `COALESCE(amount, 0)` shows 0 in place of NULL (it returns the first non-NULL value).",
    "Sort by salary high→low, then keep the first 5 rows (`LIMIT 5`; Oracle `FETCH FIRST 5 ROWS ONLY`).",
    "Sort with a unique tie-breaker, skip the first 5 rows (`OFFSET 5`), then take the next 5. Page n = `OFFSET (n-1)*size`.",
    "`dept_id <> 1` is *unknown* for NULL, so that row would be lost. Adding `OR dept_id IS NULL` brings it back.",
    "Join text pieces with `||`. `COALESCE(city, 'unknown')` replaces a NULL city before joining, so the label is never NULL.",
]
