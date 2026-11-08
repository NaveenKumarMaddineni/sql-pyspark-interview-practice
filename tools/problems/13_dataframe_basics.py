TOPIC = dict(
    title="13 · PySpark DataFrame basics",
    engines=["spark"],
    nocache=True,
    intro="""
A **DataFrame** is a table spread over many machines (here: your laptop's CPU cores).
- **transformations** (`select`, `filter`, `withColumn`, `join`, ...) only **describe** the work: nothing runs yet
- **actions** (`show`, `count`, `collect`, `write`, ...) **run** it. This is **lazy evaluation**.

Import style used everywhere: `from pyspark.sql import functions as F, Window, types as T`.
""",
    questions=[
        Q("Look at a DataFrame",
          "For `employees`: print the schema, the list of column names, the column types, and the row count.",
          level="easy", compare=False, ssql="SKIP",
          sdf="""
employees.printSchema()
print(employees.columns)
print(employees.dtypes)
print(employees.count())""",
          note="`printSchema()` shows types and **nullable** (can it hold NULL). "
               "`count()` is an **action**: it runs a job."),

        Q("Select, compute, rename",
          "Show `name`, `salary` and `new_salary` = salary × 1.10 rounded to 2 decimals. "
          "Then rename `name` to `employee_name`.",
          level="easy",
          sdf="""
res = (employees
       .select("name", "salary", F.round(F.col("salary") * 1.10, 2).alias("new_salary"))
       .withColumnRenamed("name", "employee_name"))""",
          ssql="""
SELECT name AS employee_name, salary, ROUND(salary * 1.10, 2) AS new_salary
FROM employees""",
          note="Columns can be strings (`\"salary\"`) or Column objects (`F.col(\"salary\")`). "
               "You need Column objects for math and conditions."),

        Q("Add and cast columns",
          "Add `hire_year` (int), `salary_double` (salary cast to double) and `hire_date_text` "
          "(hire_date as string). Use **one** call: `withColumns`.",
          level="easy", ssql="SKIP",
          sdf="""
res = employees.withColumns({
    "hire_year": F.year("hire_date"),
    "salary_double": F.col("salary").cast("double"),
    "hire_date_text": F.col("hire_date").cast("string"),
}).select("name", "hire_year", "salary_double", "hire_date_text")""",
          note="Many `withColumn` calls in a loop make the plan big and slow. "
               "`withColumns({...})` or one `select` is better."),

        Q("Filter with several conditions",
          "Employees in departments 1 or 2, hired in **2018 or later**, whose name contains `a` "
          "(lower-case). Show `name`, `dept_id`, `hire_date`.",
          level="easy",
          sdf="""
res = (employees
       .filter(F.col("dept_id").isin(1, 2)
               & (F.col("hire_date") >= "2018-01-01")
               & F.col("name").contains("a"))
       .select("name", "dept_id", "hire_date"))""",
          ssql="""
SELECT name, dept_id, hire_date
FROM employees
WHERE dept_id IN (1, 2) AND hire_date >= DATE '2018-01-01' AND name LIKE '%a%'""",
          note="`filter` and `where` are the same. A filter can also be a SQL string: `.filter(\"dept_id IN (1, 2)\")`."),

        Q("Handle NULLs: fillna, dropna",
          "From `orders`: (1) `res` = orders with NULL amount replaced by 0 and NULL status by `unknown`. "
          "(2) print how many rows remain after dropping rows with NULL amount.",
          level="easy", ssql="SKIP",
          sdf="""
res = orders.fillna({"amount": 0, "status": "unknown"})
print("rows without NULL amount:", orders.dropna(subset=["amount"]).count())""",
          note="`fillna` with a dict fills each column with its own value. `dropna()` with no subset drops a row "
               "if **any** column is NULL: usually too much."),

        Q("Create a DataFrame with an explicit schema",
          "Build a DataFrame of 3 bonuses: `(1, 5000.0, '2025-01-31')`, `(8, 3000.0, '2025-01-31')`, "
          "`(99, 100.0, '2025-02-28')` with a **StructType** schema (emp_id int, bonus double, paid_on string), "
          "then cast paid_on to date.",
          level="medium", ssql="SKIP",
          sdf="""
schema = T.StructType([
    T.StructField("emp_id", T.IntegerType(), nullable=False),
    T.StructField("bonus", T.DoubleType()),
    T.StructField("paid_on", T.StringType()),
])
bonuses = spark.createDataFrame([(1, 5000.0, "2025-01-31"), (8, 3000.0, "2025-01-31"),
                                 (99, 100.0, "2025-02-28")], schema)
res = bonuses.withColumn("paid_on", F.to_date("paid_on"))""",
          note="An explicit schema is faster (no guessing) and safer (no wrong types). A short form also works: "
               "`\"emp_id int, bonus double, paid_on string\"`."),

        Q("Sort with NULLs last",
          "Sort employees by `dept_id` ascending with the NULL dept_id **last**, then salary high → low. "
          "Show `name`, `dept_id`, `salary`.",
          level="easy",
          sdf="""
res = (employees.select("name", "dept_id", "salary")
       .orderBy(F.col("dept_id").asc_nulls_last(), F.desc("salary"), "name"))""",
          ssql="""
SELECT name, dept_id, salary
FROM employees
ORDER BY dept_id ASC NULLS LAST, salary DESC, name""",
          note="Spark (and BigQuery) put NULLs **first** for ASC by default; Oracle puts them **last**.",
          ordered=True),

        Q("distinct vs dropDuplicates",
          "(1) Different `(customer_id, status)` pairs in orders: count them. "
          "(2) Keep **one row per customer** with `dropDuplicates([\"customer_id\"])` and count.",
          level="easy", ssql="SKIP",
          sdf="""
print("pairs:", orders.select("customer_id", "status").distinct().count())
one_per_customer = orders.dropDuplicates(["customer_id"])
print("customers:", one_per_customer.count())""",
          note="`dropDuplicates(subset)` keeps **some** row per key: which one is not defined. "
               "Use a window with `row_number` to choose (topic 08)."),

        Q("SQL inside the DataFrame API",
          "Use `selectExpr` and `F.expr` to show `name`, `salary` and `band` = "
          "`CASE WHEN salary >= 100000 THEN 'senior' ELSE 'junior' END`.",
          level="easy", ssql="SKIP",
          sdf="""
res = employees.selectExpr(
    "name", "salary",
    "CASE WHEN salary >= 100000 THEN 'senior' ELSE 'junior' END AS band")
# same thing with F.expr inside a normal select:
same = employees.select("name", "salary",
                        F.expr("CASE WHEN salary >= 100000 THEN 'senior' ELSE 'junior' END").alias("band"))""",
          note="`expr` lets you mix SQL strings and Python. Handy for functions that are easier to write in SQL."),

        Q("Bring results to Python carefully",
          "Get the **top 3** salaries as Python values with `take`, the first row with `first()`, and the "
          "employees table as **pandas** with only 5 rows.",
          level="easy", compare=False, ssql="SKIP",
          sdf="""
top3 = employees.orderBy(F.desc("salary")).select("name", "salary").take(3)
print(top3)
print(top3[0]["name"], top3[0].salary)
print(employees.first())
print(employees.limit(5).toPandas())""",
          note="`collect()`, `take()`, `toPandas()` bring data to the **driver** (one machine). "
               "On big data this causes out-of-memory errors. Always `limit` or aggregate first."),

        Q("Quick statistics",
          "Show count, mean, stddev, min, max of `salary` with `describe`, and the 25%/50%/75% "
          "values with `summary`.",
          level="easy", compare=False, ssql="SKIP",
          sdf="""
employees.describe("salary").show()
employees.select("salary").summary("25%", "50%", "75%").show()""",
          note="Percentiles in `summary` are **approximate** (fast on big data)."),

        Q("Rows per group, then filter (like HAVING)",
          "Show `dept_id` and `n` for departments with **at least 5** employees, biggest first.",
          level="easy",
          sdf="""
res = (employees.groupBy("dept_id").agg(F.count("*").alias("n"))
       .filter("n >= 5").orderBy(F.desc("n"), "dept_id"))""",
          ssql="""
SELECT dept_id, COUNT(*) AS n
FROM employees
GROUP BY dept_id
HAVING COUNT(*) >= 5
ORDER BY n DESC, dept_id""",
          ordered=True),
    ],
)

# "How it works" text for each question, in order (shown in the solutions).
HOW = [
    "`printSchema`, `columns` and `dtypes` only read metadata; `count()` is an action that runs a job.",
    "`select` with an expression creates the new column; `withColumnRenamed` renames an existing one.",
    "`withColumns` adds several columns in one step; `cast` changes the type.",
    "Combine conditions with `&` inside `filter`; `isin` = SQL IN, `contains` = LIKE '%a%'.",
    "`fillna({col: value})` fills per column; `dropna(subset=[...])` drops rows with NULL in those columns only.",
    "Define the schema with StructType/StructField, build the DataFrame from Python tuples, then convert the text date with `to_date`.",
    "`asc_nulls_last()` on the column moves NULLs to the end; then the other sort keys.",
    "`distinct` compares whole rows; `dropDuplicates([cols])` compares only the listed columns and keeps one row each.",
    "`selectExpr` takes SQL text for each column; `F.expr` puts SQL text inside a normal select.",
    "`take(n)` and `first()` return Row objects to Python; `limit(n).toPandas()` returns a small pandas DataFrame.",
    "`describe()` gives count/mean/stddev/min/max; `summary()` can also give approximate percentiles.",
    "groupBy + count, then `filter` on the count column works like SQL HAVING.",
]
