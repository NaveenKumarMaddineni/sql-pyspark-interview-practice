TOPIC = dict(
    title="17 · Partitions, shuffles, caching, lazy evaluation",
    engines=["spark"],
    nocache=True,
    intro="""
- A **partition** = one chunk of a DataFrame. One **task** processes one partition on one CPU core.
- **Narrow** transformation (`filter`, `select`, `withColumn`): each output partition needs only one input
  partition. No data moves.
- **Wide** transformation (`groupBy`, `join`, `distinct`, `orderBy`, `repartition`): rows must move between
  partitions = a **shuffle** (`Exchange` in the plan). Shuffles split a job into **stages**.
- **AQE** (Adaptive Query Execution) changes the plan while running: merges small shuffle partitions,
  switches to broadcast joins, splits skewed partitions.

This practice Spark uses `local[*]` (all your cores) and `spark.sql.shuffle.partitions = 4`.
""",
    questions=[
        Q("How many partitions?",
          "Print the number of partitions of: `orders` (read from a file), `orders.repartition(8)`, "
          "and `spark.range(0, 1000, numPartitions=6)`.",
          level="easy", compare=False, ssql="SKIP",
          sdf="""
print(orders.rdd.getNumPartitions())
print(orders.repartition(8).rdd.getNumPartitions())
print(spark.range(0, 1000, numPartitions=6).rdd.getNumPartitions())""",
          note="For files, Spark makes about one partition per 128 MB (`spark.sql.files.maxPartitionBytes`). "
               "Our tiny file = 1 partition."),

        Q("repartition vs coalesce",
          "Start from `spark.range(1000, numPartitions=8)`. Make 2 partitions with `coalesce(2)` and with "
          "`repartition(2)`. Print both plans: which one has an `Exchange` (shuffle)?",
          level="medium", compare=False, ssql="SKIP",
          sdf="""
df = spark.range(1000, numPartitions=8)
df.coalesce(2).explain()
df.repartition(2).explain()""",
          note="`coalesce(n)` only **merges** partitions (no shuffle, can only go down; partitions may be uneven). "
               "`repartition(n)` does a full shuffle (can go up or down, even sizes). "
               "`repartition(\"col\")` puts equal keys together."),

        Q("Rows per partition",
          "Repartition `orders` by `customer_id` into 4 partitions and count rows per partition with "
          "`F.spark_partition_id()`.",
          level="medium", compare=False, ssql="SKIP",
          sdf="""
res = (orders.repartition(4, "customer_id")
       .groupBy(F.spark_partition_id().alias("partition")).count()
       .orderBy("partition"))""",
          note="Uneven counts = **skew**. Hash partitioning sends every row of one customer to the same partition."),

        Q("Narrow vs wide: find the shuffle",
          "Print the plan of: orders → filter completed → withColumn year → groupBy year → count. "
          "Which step makes the `Exchange`?",
          level="easy", compare=False, ssql="SKIP",
          sdf="""
(orders.filter("status = 'completed'")
       .withColumn("yr", F.year("order_date"))
       .groupBy("yr").count()
       .explain())""",
          note="filter and withColumn are narrow (same stage). groupBy is wide: `HashAggregate (partial)` → "
               "`Exchange hashpartitioning(yr)` → `HashAggregate (final)`. The partial step runs **before** "
               "the shuffle, so less data moves (like a combiner in MapReduce)."),

        Q("Lazy evaluation",
          "Build a DataFrame with a filter and a new column, and time it. Then call `count()` and time that. "
          "Which one actually did the work?",
          level="easy", compare=False, ssql="SKIP",
          sdf="""
import time
t0 = time.time()
df = spark.range(5_000_000).filter("id % 3 = 0").withColumn("sq", F.col("id") * F.col("id"))
print(f"transformations: {time.time() - t0:.3f}s")
t0 = time.time()
print(df.count())
print(f"action (count):  {time.time() - t0:.3f}s")""",
          note="Transformations only build a plan (a **DAG**: a graph of steps). Spark runs nothing until an "
               "**action**. That lets the optimizer (Catalyst) rearrange the whole plan, e.g. push filters down."),

        Q("cache and persist",
          "Make an expensive DataFrame (`spark.range(3_000_000)` with a groupBy), `cache()` it, run `count()` "
          "twice and time both. Check `is_cached`, then `unpersist()`.",
          level="medium", compare=False, ssql="SKIP",
          sdf="""
import time
df = spark.range(3_000_000).withColumn("k", F.col("id") % 1000).groupBy("k").agg(F.sum("id").alias("s"))
df.cache()
for i in (1, 2):
    t0 = time.time(); df.count(); print(f"run {i}: {time.time() - t0:.3f}s")
print("is_cached:", df.is_cached, "|", df.storageLevel)
df.unpersist()""",
          note="`cache()` is lazy too: the first action fills it, later actions read from memory. "
               "Cache only data you reuse **several** times; it takes executor memory. "
               "`persist(StorageLevel.MEMORY_AND_DISK)` lets you choose the storage level."),

        Q("Shuffle partitions and AQE",
          "Print `spark.sql.shuffle.partitions` and `spark.sql.adaptive.enabled`. Then run a groupBy on `orders` "
          "and print the output partition count with AQE **on** and **off**.",
          level="medium", compare=False, ssql="SKIP",
          sdf="""
print(spark.conf.get("spark.sql.shuffle.partitions"), spark.conf.get("spark.sql.adaptive.enabled"))
for aqe in ("true", "false"):
    spark.conf.set("spark.sql.adaptive.enabled", aqe)
    n = orders.groupBy("status").count().rdd.getNumPartitions()
    print(f"AQE {aqe}: {n} partition(s) after groupBy")
spark.conf.set("spark.sql.adaptive.enabled", "true")""",
          note="Without AQE you always get `shuffle.partitions` (default **200**: far too many for small data, "
               "too few for huge data). AQE **coalesces** small shuffle partitions after seeing the real sizes. "
               "A common rule of thumb: shuffle partitions ≈ 2–3 × total cores."),

        Q("Cut a long lineage with checkpoint",
          "Run a loop that adds a column 30 times. Print the length of the plan text before and after "
          "`localCheckpoint()`.",
          level="hard", compare=False, ssql="SKIP",
          sdf="""
df = spark.range(100)
for i in range(30):
    df = df.withColumn(f"c{i}", F.col("id") + i)
plan_len = lambda d: len(d._jdf.queryExecution().logical().toString())
print("plan length before:", plan_len(df))
df = df.localCheckpoint()
print("plan length after: ", plan_len(df))""",
          note="**Lineage** = the full list of steps to rebuild a DataFrame. Loops (like the recursion in topic 09) "
               "make it huge and slow to plan. `checkpoint()` (to reliable storage) or `localCheckpoint()` "
               "(executor storage) saves the data and starts a fresh, short plan."),

        Q("Spark SQL hints for partitions",
          "In Spark SQL, use the `REPARTITION(3)` and `COALESCE(1)` hints on `orders` and check the partition count.",
          level="easy", compare=False,
          ssql="""
SELECT /*+ REPARTITION(3) */ order_id, status FROM orders""",
          sdf="""
for hint in ("REPARTITION(3)", "COALESCE(1)"):
    df = spark.sql(f"SELECT /*+ {hint} */ * FROM orders")
    print(hint, df.rdd.getNumPartitions())""",
          note="SQL users get the same controls as the DataFrame API: `REPARTITION`, `COALESCE`, "
               "`REPARTITION_BY_RANGE`, `BROADCAST`, `MERGE`, `SHUFFLE_HASH`."),
    ],
)

# "How it works" text for each question, in order (shown in the solutions).
HOW = [
    "`df.rdd.getNumPartitions()` shows how many chunks (= tasks) the DataFrame has.",
    "The plans show the difference: `coalesce` is a simple `Coalesce` step; `repartition` adds an `Exchange` (shuffle).",
    "`spark_partition_id()` tells which partition each row is in; groupBy it to see the row count per partition.",
    "`explain()` shows the plan; the `Exchange` line appears at the groupBy.",
    "Building the DataFrame takes almost no time (just a plan); `count()` is when the work runs.",
    "`cache()` marks the DataFrame; the first count computes and stores it, the second reads from memory (faster).",
    "Read the settings with `spark.conf.get`; with AQE on, the groupBy output shrinks to fewer partitions.",
    "A loop of withColumn makes a long plan; `localCheckpoint()` saves the data and replaces the plan with a short one.",
    "SQL hints in `/*+ ... */` ask Spark to repartition or coalesce the query result.",
]
