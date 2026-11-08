TOPIC = dict(
    title="16 · Spark joins in depth",
    engines=["spark"],
    nocache=True,
    intro="""
How Spark runs a join:
- **Broadcast hash join**: the small side is copied to every executor. No shuffle. Fastest.
- **Sort-merge join**: both sides are **shuffled** (moved over the network) by the key, sorted, then merged.
  The default for two big tables.
- **Skew**: one key has far more rows than the others, so one task does most of the work and the job waits for it.

`how=` values: `inner`, `left`, `right`, `full`, `left_semi`, `left_anti`, `cross`.
""",
    questions=[
        Q("All join types in one table",
          "Join `customers` to the **distinct customer_ids** of `orders`. Print the row count for each `how`: "
          "inner, left, right, full, left_semi, left_anti.",
          level="easy", compare=False, ssql="SKIP",
          sdf="""
buyers = orders.select("customer_id").distinct()
for how in ["inner", "left", "right", "full", "left_semi", "left_anti"]:
    print(f"{how:10}", customers.join(buyers, "customer_id", how).count())""",
          note="40 customers, 32 buyers. semi = inner without the right columns (and no duplicates); "
               "anti = left rows **without** a match (8 never ordered)."),

        Q("Ambiguous column names",
          "Join employees to departments with an **expression** (`e.dept_id == d.dept_id`) and select `dept_id`. "
          "It fails: why? Fix it two ways: (1) join on the column **name**, (2) use aliases.",
          level="medium", compare=False, ssql="SKIP",
          sdf="""
e, d = employees.alias("e"), departments.alias("d")
joined = e.join(d, F.col("e.dept_id") == F.col("d.dept_id"))
try:
    joined.select("dept_id").show(1)
except Exception as err:
    print("error:", type(err).__name__)

employees.join(departments, "dept_id").select("dept_id", "name", "dept_name").show(2)   # fix 1
joined.select(F.col("e.dept_id"), "e.name", "d.dept_name").show(2)                     # fix 2""",
          note="With an expression both `dept_id` columns stay, so the name is **ambiguous**. "
               "`on=\"dept_id\"` (or a list of names) keeps only one copy."),

        Q("NULL keys never match",
          "Make two small DataFrames whose key column has a NULL. Join them with `==` and with `eqNullSafe`. "
          "Compare the results.",
          level="medium", ssql="SKIP",
          sdf="""
a = spark.createDataFrame([(1, "a1"), (None, "a-null")], "k int, va string")
b = spark.createDataFrame([(1, "b1"), (None, "b-null")], "k int, vb string")
print("== join rows:", a.join(b, a.k == b.k).count())
res = a.join(b, a.k.eqNullSafe(b.k)).select("va", "vb")""",
          note="`NULL = NULL` is unknown, so NULL keys never join. Usually that is right. If NULL is a real "
               "value for you (\"no department\"), use `eqNullSafe` (SQL `<=>`) or COALESCE the key."),

        Q("Join on two columns with different names",
          "Join `order_items` (order_id, product_id) to a small price list "
          "`prices(oid, pid, unit_price)` on both keys. Show matching rows.",
          level="easy",
          sdf="""
prices = spark.createDataFrame([(1001, 7, 30.0), (1001, 10, 30.0), (1002, 99, 1.0)],
                               "oid int, pid int, unit_price double")
res = (order_items.join(prices, (order_items.order_id == prices.oid) & (order_items.product_id == prices.pid))
       .select("order_id", "product_id", "quantity", "unit_price"))""",
          ssql="""
SELECT i.order_id, i.product_id, i.quantity, p.unit_price
FROM order_items i
JOIN VALUES (1001, 7, 30.0), (1001, 10, 30.0), (1002, 99, 1.0) AS p(oid, pid, unit_price)
  ON i.order_id = p.oid AND i.product_id = p.pid"""),

        Q("Broadcast or not? Read the plan",
          "Join `order_items` to `products` (1) with AQE broadcast **turned off** "
          "(`spark.sql.autoBroadcastJoinThreshold = -1`) and (2) with `F.broadcast`. Print the physical plan "
          "of both and find `SortMergeJoin` vs `BroadcastHashJoin`.",
          level="medium", compare=False, ssql="SKIP",
          sdf="""
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.adaptive.enabled", "false")      # to see the plain plan
order_items.join(products, "product_id").explain()
order_items.join(F.broadcast(products), "product_id").explain()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
spark.conf.set("spark.sql.adaptive.enabled", "true")""",
          note="SortMergeJoin needs an `Exchange hashpartitioning` (shuffle) on **both** sides. "
               "Broadcast needs `BroadcastExchange` of the small side only. Only broadcast **small** tables: "
               "it must fit in each executor's memory (and the driver's)."),

        Q("Find skewed keys",
          "Make a fake big table: 1,000,000 rows where key `0` has 90% of the rows. "
          "Count rows per key and show the top 3 keys.",
          level="medium", compare=False, ssql="SKIP",
          sdf="""
big = spark.range(1_000_000).withColumn(
    "key", F.when(F.col("id") % 10 != 0, F.lit(0)).otherwise(F.col("id") % 100))
big.groupBy("key").count().orderBy(F.desc("count")).show(3)""",
          note="Always check the key distribution before a big join or groupBy. The task with key 0 would "
               "get ~900,000 rows while the others get ~1,000."),

        Q("Fix skew with salting",
          "Join the skewed `big` table to a small `dim(key, label)` with **salting**: add a random salt 0–9 "
          "to `big`, copy each `dim` row 10 times (one per salt), join on (key, salt). "
          "Check the result count is still 1,000,000.",
          level="hard", compare=False, ssql="SKIP",
          sdf="""
SALT = 10
big = spark.range(1_000_000).withColumn(
    "key", F.when(F.col("id") % 10 != 0, F.lit(0)).otherwise(F.col("id") % 100))
dim = spark.range(100).select(F.col("id").alias("key"), F.concat(F.lit("k"), "id").alias("label"))

big_salted = big.withColumn("salt", (F.rand(seed=42) * SALT).cast("int"))
dim_salted = dim.crossJoin(spark.range(SALT).withColumnRenamed("id", "salt"))
joined = big_salted.join(dim_salted, ["key", "salt"])
print(joined.count())
joined.groupBy("salt").count().orderBy("salt").show(3)""",
          note="The hot key 0 is now split into 10 smaller groups, so 10 tasks share it. Cost: the small side "
               "is 10× bigger. **First try** a broadcast join (no shuffle = no skew) or AQE skew join "
               "(`spark.sql.adaptive.skewJoin.enabled`, on by default in Spark 3.2+)."),

        Q("Range join (non-equi)",
          "Give each employee a salary band from a band table: `low` (0–79,999), `mid` (80,000–129,999), "
          "`high` (130,000+). Join on `salary BETWEEN min AND max`.",
          level="medium",
          sdf="""
bands = spark.createDataFrame([("low", 0, 79999), ("mid", 80000, 129999), ("high", 130000, 10**9)],
                              "band string, min_sal int, max_sal int")
res = (employees.join(F.broadcast(bands),
                      employees.salary.between(bands.min_sal, bands.max_sal))
       .select("name", "salary", "band"))""",
          ssql="""
SELECT e.name, e.salary, b.band
FROM employees e
JOIN VALUES ('low', 0, 79999), ('mid', 80000, 129999), ('high', 130000, 1000000000) AS b(band, min_sal, max_sal)
  ON e.salary BETWEEN b.min_sal AND b.max_sal""",
          note="A join without `=` cannot be hash/sort-merged: Spark uses a **nested loop**. "
               "Broadcast the small table, or it gets very slow."),

        Q("Cross join explosion",
          "How many rows do `customers × products` make? And `orders × order_items` (without a condition)? "
          "Print both counts.",
          level="easy", compare=False, ssql="SKIP",
          sdf="""
print(customers.crossJoin(products).count())
print(orders.crossJoin(order_items).count())""",
          note="Rows multiply: 40 × 20 = 800, 108 × 201 = 21,708. A missing join condition on big tables can "
               "create billions of rows. Spark makes you write `crossJoin` on purpose."),
    ],
)

# "How it works" text for each question, in order (shown in the solutions).
HOW = [
    "Run the same join with each `how` and count: shows which rows each join type keeps.",
    "After an expression join both tables' `dept_id` exist, so a plain name is ambiguous. Joining on the name, or selecting `e.dept_id`, fixes it.",
    "`==` never matches NULL keys; `eqNullSafe` treats NULL = NULL as true.",
    "Build the join condition with `&` between two equalities when the column names differ.",
    "Turn off auto-broadcast to see the default SortMergeJoin with shuffles; `F.broadcast` changes it to BroadcastHashJoin.",
    "groupBy(key).count() sorted by count shows the hot key at the top.",
    "Add a random salt (0–9) on the big side, copy the small side once per salt, and join on (key, salt): the hot key is spread over 10 tasks.",
    "A join condition with BETWEEN (no equality) matches each employee to the band whose range contains the salary.",
    "`crossJoin` pairs every row with every row: count = rows × rows.",
]
