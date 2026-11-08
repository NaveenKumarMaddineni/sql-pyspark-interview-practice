TOPIC = dict(
    title="18 · UDFs: when (not) to use them",
    engines=["spark"],
    nocache=True,
    intro="""
A **UDF** (user-defined function) runs your own Python code on each value.

- A plain **Python UDF** sends every row from the JVM (where Spark runs) to a Python process and back:
  slow, and Spark cannot optimize inside it.
- A **pandas UDF** (vectorized) sends whole batches with Apache Arrow: much faster.
- **Built-in functions** (`F.*`) are fastest: always try them first.

Interview answer: "I avoid Python UDFs; I use built-ins, then pandas UDFs if I really need Python."
""",
    questions=[
        Q("Python UDF vs built-in",
          "Make a column `name_upper` two ways: a Python UDF and `F.upper`. Show both on 3 rows and print "
          "both plans. Find `BatchEvalPython` in the UDF plan.",
          level="easy", compare=False, ssql="SKIP",
          sdf="""
@F.udf(returnType="string")
def to_upper(s):
    return s.upper() if s is not None else None

employees.select("name", to_upper("name").alias("name_upper")).show(3)
employees.select("name", to_upper("name").alias("u")).explain()
employees.select("name", F.upper("name").alias("u")).explain()""",
          note="`BatchEvalPython` = rows leave the JVM. The built-in stays in one optimized `Project` step "
               "(and gets whole-stage code generation)."),

        Q("Handle NULL inside a UDF",
          "Write a UDF `city_code` that returns the first 3 letters of a city in capitals. Run it on "
          "`customers` **without** a NULL check first (it fails on the NULL city), then fix it.",
          level="easy", ssql="SKIP",
          sdf="""
bad = F.udf(lambda c: c[:3].upper(), "string")
try:
    customers.select(bad("city")).collect()
except Exception as e:
    print("fails:", type(e).__name__)

city_code = F.udf(lambda c: c[:3].upper() if c else None, "string")
res = customers.filter("customer_id IN (1, 13)").select("customer_id", "city", city_code("city").alias("code"))""",
          note="Python gets NULL as `None`. `None[:3]` raises an error and the **whole job** fails. "
               "Built-ins handle NULL for you."),

        Q("Wrong return type gives silent NULLs",
          "Declare a UDF that returns `int` but really returns a float (salary / 1000). What do you get? "
          "Fix the declared type.",
          level="medium", ssql="SKIP",
          sdf="""
wrong = F.udf(lambda s: float(s) / 1000, "int")
employees.select("salary", wrong("salary").alias("k")).show(2)

right = F.udf(lambda s: float(s) / 1000, "double")
res = employees.filter("emp_id <= 2").select("salary", right("salary").alias("k"))""",
          note="A Python UDF whose value does not match the declared type returns **NULL**, with no error. "
               "Easy to miss in a pipeline: add a NULL-count check after UDF columns."),

        Q("Pandas UDF (vectorized)",
          "Write a pandas UDF `with_tax` that multiplies a price Series by 1.18 and rounds to 2 decimals. "
          "Apply it to `products`.",
          level="medium", ssql="SKIP",
          sdf="""
import pandas as pd

@F.pandas_udf("double")
def with_tax(price: pd.Series) -> pd.Series:
    return (price.astype(float) * 1.18).round(2)

res = products.select("product_name", "price", with_tax("price").alias("price_with_tax")).limit(5)""",
          note="The function gets a whole **pandas Series** (a batch of rows), not one value. Needs `pyarrow`. "
               "The built-in `F.round(F.col(\"price\") * 1.18, 2)` is still faster."),

        Q("Use a UDF in Spark SQL",
          "Register a Python function as `initials` for SQL with `spark.udf.register`, then call it in `%%sql`.",
          level="easy",
          sdf="""
def initials(name):
    return "".join(p[0] for p in name.split()) if name else None

spark.udf.register("initials", initials, "string")
res = spark.sql("SELECT name, initials(name) AS ini FROM employees WHERE emp_id <= 3")""",
          ssql="""
SELECT name, initials(name) AS ini FROM employees WHERE emp_id <= 3""",
          df_first=True,
          note="Run the Python cell first: it registers the function for this Spark session."),

        Q("Replace a UDF with built-ins",
          "This UDF labels orders: amount NULL → `missing`, > 200 → `big`, else `normal`. "
          "Write the same with `F.when` and check that both give the same counts per label.",
          level="medium",
          sdf="""
label_udf = F.udf(lambda a: "missing" if a is None else ("big" if a > 200 else "normal"), "string")
by_udf = orders.groupBy(label_udf("amount").alias("label")).count()

label = (F.when(F.col("amount").isNull(), "missing")
          .when(F.col("amount") > 200, "big")
          .otherwise("normal"))
res = orders.groupBy(label.alias("label")).count()
print("same result:", sorted(by_udf.collect()) == sorted(res.collect()))""",
          ssql="""
SELECT CASE WHEN amount IS NULL THEN 'missing'
            WHEN amount > 200 THEN 'big'
            ELSE 'normal' END AS label,
       COUNT(*) AS count
FROM orders
GROUP BY 1""",
          note="Most UDFs in real code can be replaced by `when`, `regexp_extract`, `split`, `transform`, "
               "or a join to a lookup table."),

        Q("Per-group pandas logic: applyInPandas",
          "For each department, use pandas to compute each employee's salary **z-score** "
          "((salary − mean) / std) with `groupBy(...).applyInPandas`. Skip NULL dept_id.",
          level="hard", ssql="SKIP",
          sdf="""
import pandas as pd

def zscore(pdf: pd.DataFrame) -> pd.DataFrame:
    s = pdf["salary"].astype(float)
    std = s.std(ddof=0)
    pdf["z"] = ((s - s.mean()) / std).round(2) if std else 0.0
    return pdf[["dept_id", "name", "z"]]

res = (employees.filter(F.col("dept_id").isNotNull())
       .groupBy("dept_id").applyInPandas(zscore, schema="dept_id int, name string, z double")
       .orderBy("dept_id", F.desc("z")))""",
          note="Each group goes to Python as one pandas DataFrame, so **one group must fit in memory**. "
               "A skewed key can break it. The same z-score with window functions: "
               "`(salary - avg(salary) over w) / stddev_pop(salary) over w`."),
    ],
)

# "How it works" text for each question, in order (shown in the solutions).
HOW = [
    "The UDF runs Python per row (BatchEvalPython in the plan); `F.upper` runs inside the JVM. Same output.",
    "The first UDF crashes on `None`; the fixed one returns None when the input is None.",
    "Spark can't convert the float into the declared int, so it returns NULL. Declaring `double` fixes it.",
    "A pandas UDF gets a batch of values as a pandas Series and returns a Series of the same length.",
    "`spark.udf.register(name, function, type)` makes the Python function callable from SQL.",
    "`when` / `otherwise` builds the same labels without Python; both give the same counts.",
    "`applyInPandas` sends each department's rows to Python as one pandas DataFrame; the function returns a DataFrame with the given schema.",
]
