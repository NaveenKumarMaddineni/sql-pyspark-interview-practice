CLEAN_FN = '''
def clean_orders(df):
    """Raw orders -> clean orders: trim/lower status, drop exact duplicates, keep rows with a key."""
    return (df.withColumn("status", F.lower(F.trim("status")))
              .filter(F.col("order_id").isNotNull())
              .dropDuplicates())
'''

TOPIC = dict(
    title="19 · Pipeline patterns (like a real job)",
    engines=["spark"],
    nocache=True,
    intro="""
A batch pipeline is more than one query. It is a chain of small, testable steps:

**read → add audit columns → clean → check quality → transform → reconcile → write (idempotent) → log**

Words used here:
- **run_id** = a unique id for one run of the job, saved on every row it writes (so a bad run can be found and removed)
- **quality gate** = checks that **stop** the job when the data is wrong
- **idempotent** = a rerun gives the same result (no duplicates)
- **lookback** = also re-process the last few days, to catch late-arriving rows
""",
    questions=[
        Q("Audit columns on read",
          "Read `data/orders.tsv` with a schema and add `run_id` (`'run_20251231_01'`), `load_ts` "
          "(current timestamp) and `source_file` (the file name, from the hidden `_metadata` column).",
          level="easy", compare=False, ssql="SKIP",
          sdf="""
RUN_ID = "run_20251231_01"
raw = (spark.read.option("header", True).option("sep", "\\t").option("nullValue", "NULL")
       .schema("order_id INT, customer_id INT, order_date DATE, amount DECIMAL(10,2), status STRING")
       .csv(str(ROOT / "data" / "orders.tsv")))
res = raw.select("*",
                 F.lit(RUN_ID).alias("run_id"),
                 F.current_timestamp().alias("load_ts"),
                 F.col("_metadata.file_name").alias("source_file"))
res.show(3, truncate=False)""",
          note="With audit columns you can answer \"which run/file loaded this row?\" and delete one bad run "
               "(`WHERE run_id = ...`). `_metadata` replaces the old `input_file_name()` function."),

        Q("A small, testable clean step",
          "Write a function `clean_orders(df)` that lower-cases and trims `status`, drops rows with NULL "
          "`order_id` and drops exact duplicates. Print the row count before and after.",
          level="easy", ssql="SKIP",
          sdf=CLEAN_FN + '''
print("before:", orders.count())
res = clean_orders(orders)
print("after: ", res.count())''',
          note="Each step = one function that takes a DataFrame and returns a DataFrame. "
               "Easy to read, reuse and unit test (next question)."),

        Q("Unit test a transformation",
          "Test `clean_orders` with a tiny hand-made input: one duplicate, one `' Completed '` status, one NULL "
          "order_id. Compare to the expected output with `assertDataFrameEqual`.",
          level="medium", compare=False, ssql="SKIP",
          sdf=CLEAN_FN + '''
from pyspark.testing import assertDataFrameEqual

schema = "order_id int, status string"
given = spark.createDataFrame([(1, " Completed "), (1, " Completed "), (2, "pending"), (None, "x")], schema)
expected = spark.createDataFrame([(1, "completed"), (2, "pending")], schema)

assertDataFrameEqual(clean_orders(given), expected)    # raises an error if different
print("test passed")''',
          note="Tests with 3–5 rows run in seconds and catch most logic bugs. In a project put them in "
               "`tests/test_*.py` and run `pytest`. Row order is ignored by default."),

        Q("Quality gate",
          "Write `quality_gate(df)` that checks: no NULL order_id, order_id unique, no negative amount. "
          "It returns the check results and **raises an error** if any check fails. "
          "Run it on clean orders (pass) and on raw orders (fail: catch and print).",
          level="medium", compare=False, ssql="SKIP",
          sdf=CLEAN_FN + '''
def quality_gate(df):
    r = df.agg(F.count("*").alias("rows"),
               F.countDistinct("order_id").alias("keys"),
               F.sum(F.col("order_id").isNull().cast("int")).alias("null_keys"),
               F.sum((F.col("amount") < 0).cast("int")).alias("negative")).first()
    checks = {"no_null_keys": r.null_keys == 0,
              "unique_keys": r.rows == r.keys,
              "no_negative_amount": (r.negative or 0) == 0}
    failed = [name for name, ok in checks.items() if not ok]
    if failed:
        raise ValueError(f"quality gate failed: {failed}")
    return checks

print(quality_gate(clean_orders(orders)))
try:
    quality_gate(orders)
except ValueError as e:
    print(e)''',
          note="One `agg` computes all checks in **one** pass. Failing loudly is better than loading bad data: "
               "the scheduler (Airflow/Composer) marks the task red and can alert or retry."),

        Q("Reconcile and write an audit row",
          "After a transform (daily revenue of completed clean orders), reconcile source vs target "
          "(count of orders and sum of amount). Append one audit row (run_id, counts, sums, status) to "
          "`tmp/audit` (Parquet) and show the audit table.",
          level="medium", compare=False, ssql="SKIP",
          sdf=CLEAN_FN + '''
RUN_ID = "run_20251231_01"
src = clean_orders(orders).filter("status = 'completed'")
tgt = src.groupBy("order_date").agg(F.count("*").alias("n"), F.sum("amount").alias("amount"))

s = src.agg(F.count("*").alias("c"), F.sum("amount").alias("a")).first()
t = tgt.agg(F.sum("n").alias("c"), F.sum("amount").alias("a")).first()
status = "MATCH" if (s.c, s.a) == (t.c, t.a) else "MISMATCH"

audit = spark.createDataFrame(
    [(RUN_ID, s.c, t.c, float(s.a), float(t.a), status)],
    "run_id string, src_rows long, tgt_rows long, src_amount double, tgt_amount double, status string"
).withColumn("checked_at", F.current_timestamp())
audit.write.mode("append").parquet(str(ROOT / "tmp" / "audit"))
spark.read.parquet(str(ROOT / "tmp" / "audit")).orderBy("checked_at").show(truncate=False)''',
          note="The audit table is your proof for every run (regulators and auditors ask for it). "
               "Reconcile **before** publishing, and stop on MISMATCH."),

        Q("Idempotent daily job",
          "Write `run_job(run_date)` that takes the clean orders of **that day**, adds `run_id`, and writes "
          "them to `tmp/daily_orders` partitioned by `order_date` with **dynamic** partition overwrite. "
          "Run it **twice** for `2025-10-14` and once for `2025-11-03`. Show the row count per date: no duplicates.",
          level="hard", compare=False, ssql="SKIP",
          sdf=CLEAN_FN + '''
OUT = str(ROOT / "tmp" / "daily_orders")

def run_job(run_date, attempt=1):
    run_id = f"run_{run_date.replace('-', '')}_{attempt:02d}"
    day = (clean_orders(orders)
           .filter((F.col("order_date") == F.lit(run_date).cast("date")))
           .withColumn("run_id", F.lit(run_id)))
    (day.write.mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")      # only this day's folder is replaced
        .partitionBy("order_date").parquet(OUT))
    return day.count()

print(run_job("2025-10-14"), run_job("2025-10-14", attempt=2), run_job("2025-11-03"))
spark.read.parquet(OUT).groupBy("order_date", "run_id").count().orderBy("order_date").show()''',
          note="The rerun (attempt 2) **replaced** the 2025-10-14 folder instead of adding rows. "
               "Same idea as delete-then-insert by slice in SQL (topic 10). The write option "
               "`partitionOverwriteMode` works per write, so you don't change the session setting."),

        Q("Latest version per key (snapshot + delta)",
          "`snapshot` = customers with `updated_at = 2024-12-31`. A `delta` arrives with 3 rows: customer 1 "
          "changed twice (2025-01-05 and 2025-01-09) and new customer 41. Build the new snapshot: "
          "the **latest** version of every customer. Show customers 1, 2, 41.",
          level="medium", ssql="SKIP",
          sdf='''
snapshot = customers.withColumn("updated_at", F.lit("2024-12-31").cast("date"))
delta = spark.createDataFrame(
    [(1, "Aarav Nguyen", "Denver", "2023-01-26", "2025-01-05"),
     (1, "Aarav Nguyen", "Austin", "2023-01-26", "2025-01-09"),
     (41, "Nina Patel", "Boston", "2025-01-07", "2025-01-07")],
    "customer_id int, name string, city string, signup_date string, updated_at string"
).withColumns({"signup_date": F.to_date("signup_date"), "updated_at": F.to_date("updated_at")})

w = Window.partitionBy("customer_id").orderBy(F.desc("updated_at"))
new_snapshot = (snapshot.unionByName(delta)
                .withColumn("rn", F.row_number().over(w))
                .filter("rn = 1").drop("rn"))
res = new_snapshot.filter(F.col("customer_id").isin(1, 2, 41)).orderBy("customer_id")''',
          note="Union old + new, then keep the newest row per key. Works even when the delta has **several** "
               "versions of one key (a MERGE would fail on that). Ties on `updated_at` need a tie-breaker."),

        Q("Lookback window for late data",
          "For run date `2025-12-28` with a **3-day lookback**, select the slice of orders from 2025-12-26 to "
          "2025-12-28 (inclusive), computed from the run date. Show the dates and counts in the slice.",
          level="easy", ssql="SKIP",
          sdf='''
def slice_for(run_date, lookback_days):
    end = F.lit(run_date).cast("date")
    start = F.date_sub(end, lookback_days - 1)
    return orders.filter((F.col("order_date") >= start) & (F.col("order_date") <= end))

res = (slice_for("2025-12-28", 3).dropDuplicates()
       .groupBy("order_date").count().orderBy("order_date"))''',
          note="Re-processing the last few days (with an idempotent overwrite) picks up rows that arrived late. "
               "Choose N from how late data really arrives."),
    ],
)

# "How it works" text for each question, in order (shown in the solutions).
HOW = [
    "Add constant and computed columns while reading: `lit(run_id)`, `current_timestamp()`, and the file name from `_metadata`.",
    "A function wraps the clean rules; calling it on raw orders returns the cleaned DataFrame (108 → 104 rows).",
    "Build a tiny input and the expected output by hand; `assertDataFrameEqual` fails if the function's output differs.",
    "One aggregation computes all metrics; the function compares them with the rules and raises an error listing failed checks.",
    "Compute count and sum on both sides, decide MATCH/MISMATCH, and append one audit row per run to a Parquet table.",
    "The job writes only its day's partition with dynamic overwrite; the second run of the same day replaces that folder.",
    "Union snapshot + delta, number rows per key newest first, keep number 1.",
    "Compute the start date from the run date (run_date − (N−1) days) and filter the range.",
]
