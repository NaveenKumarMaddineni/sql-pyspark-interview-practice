def yr(y, bq=False):
    t = "shield.orders" if bq else "orders"
    return f"SELECT customer_id FROM {t} WHERE EXTRACT(YEAR FROM order_date) = {y}"


TOPIC = dict(
    title="07 · Set operations: UNION, INTERSECT, EXCEPT",
    intro="""
Set operations stack two query results **on top of each other** (joins put them side by side).
Both queries need the same number of columns, in the same order, with matching types.

| | BigQuery | Oracle | Spark |
|---|---|---|---|
| all rows | `UNION ALL` | `UNION ALL` | `UNION ALL` |
| no duplicates | `UNION DISTINCT` | `UNION` | `UNION` |
| in both | `INTERSECT DISTINCT` | `INTERSECT` | `INTERSECT` |
| in first, not second | `EXCEPT DISTINCT` | `MINUS` (23ai also `EXCEPT`) | `EXCEPT` |
""",
    questions=[
        Q("One list of people",
          "Make one list of **all** employees and customers: `person_type` (`employee` / `customer`), "
          "`person_id`, `name`.",
          level="easy",
          sql="""
SELECT 'employee' AS person_type, emp_id AS person_id, name FROM employees
UNION ALL
SELECT 'customer', customer_id, name FROM customers""",
          sdf="""
res = (employees.select(F.lit("employee").alias("person_type"), F.col("emp_id").alias("person_id"), "name")
       .unionByName(customers.select(F.lit("customer").alias("person_type"),
                                     F.col("customer_id").alias("person_id"), "name")))""",
          note="Column names come from the **first** query.",
          notes={"spark": "`union()` matches columns by **position** (like SQL). `unionByName()` matches by name: "
                          "safer when column order may change."}),

        Q("Customers active in 2023 or 2025",
          "Show each `customer_id` that ordered in 2023 **or** in 2025, once.",
          level="easy",
          bq=f"{yr(2023, True)}\nUNION DISTINCT\n{yr(2025, True)}",
          ora=f"{yr(2023)}\nUNION\n{yr(2025)}",
          ssql=f"{yr(2023)}\nUNION\n{yr(2025)}",
          sdf="""
y = lambda n: orders.filter(F.year("order_date") == n).select("customer_id")
res = y(2023).union(y(2025)).distinct()""",
          note="`UNION` removes duplicates (extra work: a sort or hash). Use `UNION ALL` when you know "
               "there are none, or when you want them.",
          notes={"spark": "DataFrame `.union()` is UNION **ALL**. Add `.distinct()` for SQL UNION."}),

        Q("Customers active in both 2023 and 2025",
          "Show each `customer_id` that ordered in 2023 **and** in 2025.",
          level="easy",
          bq=f"{yr(2023, True)}\nINTERSECT DISTINCT\n{yr(2025, True)}",
          ora=f"{yr(2023)}\nINTERSECT\n{yr(2025)}",
          ssql=f"{yr(2023)}\nINTERSECT\n{yr(2025)}",
          sdf="""
y = lambda n: orders.filter(F.year("order_date") == n).select("customer_id")
res = y(2023).intersect(y(2025))"""),

        Q("Lost customers",
          "Show each `customer_id` that ordered in 2024 but **not** in 2025.",
          level="easy",
          bq=f"{yr(2024, True)}\nEXCEPT DISTINCT\n{yr(2025, True)}",
          ora=f"{yr(2024)}\nMINUS\n{yr(2025)}",
          ssql=f"{yr(2024)}\nEXCEPT\n{yr(2025)}",
          sdf="""
y = lambda n: orders.filter(F.year("order_date") == n).select("customer_id")
res = y(2024).subtract(y(2025))""",
          note="Same as an anti join. EXCEPT also removes duplicates. Interview follow-up: "
               "\"write it with NOT EXISTS\".",
          notes={"spark": "`subtract()` = EXCEPT DISTINCT. `exceptAll()` keeps duplicates."}),

        Q("January users who did not come back in February",
          "Show each `user_id` that logged in during January 2025 but **not** during February 2025.",
          level="medium",
          bq="""
SELECT user_id FROM shield.user_logins
WHERE login_date >= DATE '2025-01-01' AND login_date < DATE '2025-02-01'
EXCEPT DISTINCT
SELECT user_id FROM shield.user_logins
WHERE login_date >= DATE '2025-02-01' AND login_date < DATE '2025-03-01'""",
          ora="""
SELECT user_id FROM user_logins
WHERE login_date >= DATE '2025-01-01' AND login_date < DATE '2025-02-01'
MINUS
SELECT user_id FROM user_logins
WHERE login_date >= DATE '2025-02-01' AND login_date < DATE '2025-03-01'""",
          ssql="""
SELECT user_id FROM user_logins
WHERE login_date >= DATE '2025-01-01' AND login_date < DATE '2025-02-01'
EXCEPT
SELECT user_id FROM user_logins
WHERE login_date >= DATE '2025-02-01' AND login_date < DATE '2025-03-01'""",
          sdf="""
jan = user_logins.filter((F.col("login_date") >= "2025-01-01") & (F.col("login_date") < "2025-02-01"))
feb = user_logins.filter((F.col("login_date") >= "2025-02-01") & (F.col("login_date") < "2025-03-01"))
res = jan.select("user_id").subtract(feb.select("user_id"))""",
          note="This is **churn** (users who stopped coming). Retention questions in topic 08 build on it."),

        Q("Status summary with a TOTAL row",
          "Show `status` and `num_orders` (different orders) per status, then **one more row** "
          "`TOTAL` with the count of all different orders. Use UNION ALL.",
          level="medium",
          bq="""
SELECT status, COUNT(DISTINCT order_id) AS num_orders
FROM shield.orders
GROUP BY status
UNION ALL
SELECT 'TOTAL', COUNT(DISTINCT order_id)
FROM shield.orders""",
          ora="""
SELECT status, COUNT(DISTINCT order_id) AS num_orders
FROM orders
GROUP BY status
UNION ALL
SELECT 'TOTAL', COUNT(DISTINCT order_id)
FROM orders""",
          ssql="""
SELECT status, COUNT(DISTINCT order_id) AS num_orders
FROM orders
GROUP BY status
UNION ALL
SELECT 'TOTAL', COUNT(DISTINCT order_id)
FROM orders""",
          sdf="""
by_status = orders.groupBy("status").agg(F.countDistinct("order_id").alias("num_orders"))
total = orders.agg(F.lit("TOTAL").alias("status"), F.countDistinct("order_id").alias("num_orders"))
res = by_status.unionByName(total)""",
          note="Why not use ROLLUP? With COUNT(DISTINCT) both work; UNION ALL is easier to read. "
               "Here the total (104) equals the sum because each order has one status."),

        Q("Do two tables hold the same rows?",
          "Is `orders` the same as `orders` **without duplicates**? Count rows that are in one but "
          "not the other, **in both directions**, keeping duplicates. Show `only_in_raw`, `only_in_clean`.\n\n"
          "Hint: one row that appears twice in raw and once in clean counts once.",
          level="hard",
          bq="""
WITH raw AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, customer_id, order_date, amount, status) AS copy_no
             FROM shield.orders),
clean AS (SELECT *, 1 AS copy_no FROM (SELECT DISTINCT * FROM shield.orders))
SELECT
  (SELECT COUNT(*) FROM (SELECT * FROM raw   EXCEPT DISTINCT SELECT * FROM clean)) AS only_in_raw,
  (SELECT COUNT(*) FROM (SELECT * FROM clean EXCEPT DISTINCT SELECT * FROM raw))   AS only_in_clean""",
          ora="""
WITH clean AS (SELECT DISTINCT * FROM orders)
SELECT
  (SELECT COUNT(*) FROM (SELECT * FROM orders EXCEPT ALL SELECT * FROM clean))  AS only_in_raw,
  (SELECT COUNT(*) FROM (SELECT * FROM clean  EXCEPT ALL SELECT * FROM orders)) AS only_in_clean
FROM dual""",
          ssql="""
WITH clean AS (SELECT DISTINCT * FROM orders)
SELECT
  (SELECT COUNT(*) FROM (SELECT * FROM orders EXCEPT ALL SELECT * FROM clean))  AS only_in_raw,
  (SELECT COUNT(*) FROM (SELECT * FROM clean  EXCEPT ALL SELECT * FROM orders)) AS only_in_clean""",
          sdf="""
clean = orders.distinct()
res = spark.createDataFrame([(orders.exceptAll(clean).count(), clean.exceptAll(orders).count())],
                            "only_in_raw long, only_in_clean long")""",
          note="`EXCEPT` (DISTINCT) would say \"0 differences\" and hide the 4 extra copies. "
               "Comparing tables both ways is how you test a pipeline: source vs target.",
          notes={"bigquery": "BigQuery has no `EXCEPT ALL`. The trick: number each copy with ROW_NUMBER, "
                             "then plain EXCEPT DISTINCT sees copy 1 and copy 2 as different rows. "
                             "A window with only PARTITION BY (no ORDER BY) is allowed for ROW_NUMBER here.",
                 "oracle": "`EXCEPT ALL` is new in Oracle 21c/23ai. In 19c use the ROW_NUMBER trick (see the BigQuery version)."}),

        Q("Products: sold, never sold, all",
          "Show `status` (`sold` / `never sold`) and `num_products`, using a UNION ALL of two queries.",
          level="medium",
          bq="""
SELECT 'sold' AS status, COUNT(*) AS num_products
FROM shield.products WHERE product_id IN (SELECT product_id FROM shield.order_items)
UNION ALL
SELECT 'never sold', COUNT(*)
FROM shield.products WHERE product_id NOT IN (SELECT product_id FROM shield.order_items)""",
          ora="""
SELECT 'sold' AS status, COUNT(*) AS num_products
FROM products WHERE product_id IN (SELECT product_id FROM order_items)
UNION ALL
SELECT 'never sold', COUNT(*)
FROM products WHERE product_id NOT IN (SELECT product_id FROM order_items)""",
          ssql="""
SELECT 'sold' AS status, COUNT(*) AS num_products
FROM products WHERE product_id IN (SELECT product_id FROM order_items)
UNION ALL
SELECT 'never sold', COUNT(*)
FROM products WHERE product_id NOT IN (SELECT product_id FROM order_items)""",
          sdf="""
sold = products.join(order_items, "product_id", "left_semi").count()
res = spark.createDataFrame([("sold", sold), ("never sold", products.count() - sold)],
                            "status string, num_products long")""",
          note="`NOT IN` is safe **here** only because `order_items.product_id` has no NULLs. "
               "In real code prefer NOT EXISTS."),
    ],
)

# "How it works" text for each question, in order (shown in the solutions).
HOW = [
    "Two SELECTs with the same column shape, stacked with `UNION ALL`. A fixed text column (`'employee'`) tells where each row came from.",
    "Customers of 2023 `UNION` customers of 2025: duplicates are removed, so each id appears once.",
    "`INTERSECT` keeps only ids that appear in **both** results.",
    "`EXCEPT` / `MINUS` keeps ids from the first result that are **not** in the second.",
    "Same as Q4 with login dates: January users minus February users = users who did not come back.",
    "First SELECT = counts per status. Second SELECT = one row with the total. `UNION ALL` stacks them.",
    "`EXCEPT ALL` subtracts row by row (keeps duplicates). raw − clean leaves the 4 extra copies; clean − raw leaves nothing.",
    "Count sold products and never-sold products in two queries, label each, stack with UNION ALL.",
]
