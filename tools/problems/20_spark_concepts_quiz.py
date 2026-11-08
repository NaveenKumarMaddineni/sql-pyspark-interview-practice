TOPIC = dict(
    title="20 · Spark concepts quiz (interview theory)",
    engines=["spark"],
    intro="""
Interviewers always ask theory too. Answer each question **out loud** in 3–5 sentences,
then write it down. Compare with the solutions notebook.
""",
    questions=[
        Q("Driver, executors, cluster manager",
          "What do the **driver** and the **executors** do? What is a cluster manager?",
          level="easy",
          answer="""
- **Driver**: the main program. It runs your Python code, builds the plan, splits it into tasks and sends
  them out. `collect()` results come back to it.
- **Executors**: worker processes (JVMs) on the cluster machines. They run the tasks and keep cached data.
  Each executor has some cores (tasks at the same time) and memory.
- **Cluster manager**: gives machines/containers to the app: YARN (Dataproc), Kubernetes, or Spark standalone.
  `local[*]` = everything in one process on your laptop."""),

        Q("Job, stage, task",
          "What are a **job**, a **stage** and a **task**? What creates a new stage?",
          level="easy",
          answer="""
- **Job**: started by one **action** (`count`, `write`, `collect`...).
- **Stage**: a part of the job with no shuffle inside. A **shuffle** (wide transformation) ends a stage.
- **Task**: one stage on **one partition**. 200 partitions = 200 tasks, run in parallel by the executor cores.

So: action → job → stages (cut at shuffles) → tasks (one per partition). You see all of this in the Spark UI."""),

        Q("Narrow vs wide transformations",
          "Explain narrow and wide transformations with 3 examples each. Why does it matter?",
          level="easy",
          answer="""
- **Narrow**: each output partition needs data from **one** input partition. No network.
  `select`, `filter`, `withColumn`, `map`, `union`, `coalesce`.
- **Wide**: an output partition needs data from **many** input partitions, so rows are **shuffled**.
  `groupBy`, `join` (non-broadcast), `distinct`, `orderBy`, `repartition`, window functions.

It matters because shuffles are the slow part: disk writes, network, sorting. Fewer/smaller shuffles = faster job."""),

        Q("Lazy evaluation and the DAG",
          "What is lazy evaluation? What are the benefits?",
          level="easy",
          answer="""
Transformations do not run when you call them: Spark only records them in a plan (a **DAG**, directed acyclic
graph of steps). Work starts at an **action**.

Benefits: the optimizer sees the **whole** plan, so it can push filters down to the source, read only needed
columns, combine steps into one stage, and pick join types. It also avoids computing things you never use."""),

        Q("Catalyst and Tungsten",
          "What do the **Catalyst** optimizer and **Tungsten** do?",
          level="medium",
          answer="""
- **Catalyst** (query optimizer): turns your DataFrame/SQL into a logical plan, applies rules (predicate pushdown,
  column pruning, constant folding), then chooses a physical plan (e.g. broadcast vs sort-merge join) using
  statistics. Same engine for SQL and DataFrames, so both are equally fast.
- **Tungsten** (execution engine): compact binary memory format (off-heap), cache-friendly, and
  **whole-stage code generation** (compiles a stage into one Java function). Python UDFs cannot use it."""),

        Q("repartition vs coalesce",
          "Difference between `repartition(n)` and `coalesce(n)`? When do you use each?",
          level="easy",
          answer="""
- `repartition(n)` / `repartition(col)`: **full shuffle**, even partition sizes, can increase or decrease,
  can group by a key.
- `coalesce(n)`: **merges** existing partitions without a shuffle, can only **decrease**, sizes may be uneven.

Use `coalesce` to reduce the number of output files cheaply (e.g. before writing). Use `repartition` to fix skew,
increase parallelism, or partition by a column before a write. Careful: `coalesce(1)` makes **one task** do the
whole last stage."""),

        Q("cache vs persist",
          "What is the difference between `cache()` and `persist()`? When is caching a bad idea?",
          level="easy",
          answer="""
`cache()` = `persist()` with the default level (DataFrame: `MEMORY_AND_DISK`). `persist(level)` lets you choose:
`MEMORY_ONLY`, `MEMORY_AND_DISK`, `DISK_ONLY`, serialized, replicated...
Both are lazy: filled by the first action. Free with `unpersist()`.

Bad idea when: the data is used only **once**, it is too big for memory (spills, evicts other data), or reading
the source is already cheap. Cache after expensive steps that several actions reuse."""),

        Q("Broadcast join",
          "How does a broadcast join work, and when does Spark choose it? What can go wrong?",
          level="medium",
          answer="""
The small table is collected and **sent to every executor**; each task joins its part of the big table locally,
with no shuffle of the big table. Spark picks it when a side is smaller than
`spark.sql.autoBroadcastJoinThreshold` (10 MB default) or with a `broadcast()` hint; AQE can switch to it at run time.

Risks: broadcasting something big → driver/executor **out of memory** or slow broadcast timeouts. Statistics can be
wrong (a filtered table looks small but isn't)."""),

        Q("Data skew",
          "What is skew? How do you detect it and fix it?",
          level="medium",
          answer="""
**Skew** = a few keys have far more rows than the rest, so a few tasks run much longer (or run out of memory) while
others finish fast.

Detect: Spark UI stage page (max task time ≫ median, big shuffle read for one task), or `groupBy(key).count()`.

Fix: **broadcast** the small side (no shuffle); turn on **AQE skew join** (splits big partitions);
**salting** (add a random suffix to the hot key, replicate the other side); handle hot keys (e.g. NULL keys)
separately; filter or pre-aggregate before the join."""),

        Q("AQE",
          "What does Adaptive Query Execution do?",
          level="medium",
          answer="""
AQE re-optimizes the plan **during** the run, using real statistics from finished shuffle stages:
1. **coalesces** small shuffle partitions (fewer tiny tasks),
2. **switches join strategy** (e.g. sort-merge → broadcast when a side turns out small),
3. **splits skewed partitions** in joins.

On by default since Spark 3.2 (`spark.sql.adaptive.enabled`)."""),

        Q("How many partitions / shuffle partitions?",
          "How do you choose the number of partitions and `spark.sql.shuffle.partitions`?",
          level="medium",
          answer="""
Aim for partitions of about **100–200 MB** and at least **2–3 tasks per core** so all cores stay busy.
`spark.sql.shuffle.partitions` default is 200: too many for small data (overhead, tiny files), too few for TBs
(huge tasks, spills). Set it from data size / target size, or let AQE coalesce from a higher number.
For reading files, `spark.sql.files.maxPartitionBytes` (128 MB) decides the input split size."""),

        Q("Out of memory errors",
          "A Spark job fails with OutOfMemory. What are the usual causes and fixes?",
          level="hard",
          answer="""
**Driver OOM**: `collect()`/`toPandas()` on big data, broadcasting a big table, too many small files/partitions in
the plan. Fix: don't collect, write results instead; lower the broadcast threshold; compact files.

**Executor OOM**: skewed partitions, too few partitions (huge tasks), big `collect_list`/explode, Python UDF or
`applyInPandas` with a huge group, caching too much. Fix: more partitions, fix skew, avoid UDFs, raise
`spark.executor.memory` / `memoryOverhead` (Python and off-heap memory live in the overhead)."""),

        Q("The small files problem",
          "Why are many small files bad and how do you avoid them?",
          level="medium",
          answer="""
Each file = metadata to list + at least one task to read + open/close cost. Millions of small files make jobs slow to
**plan** and slow to **read** (and stress the storage / metastore).

Avoid: `coalesce`/`repartition` before writing; don't `partitionBy` a column with many values; use AQE coalescing;
run compaction jobs (Delta `OPTIMIZE`, Iceberg rewrite); aim for files of 128 MB–1 GB."""),

        Q("DataFrame vs RDD vs Dataset",
          "When would you use an RDD instead of a DataFrame?",
          level="easy",
          answer="""
**DataFrame**: rows with a schema, optimized by Catalyst/Tungsten. Use it almost always.
**RDD**: low-level distributed collection of Python/JVM objects; no optimizer. Use only for special needs
(custom partitioning logic, non-tabular data, old code).
**Dataset**: typed DataFrame, only in Scala/Java. In PySpark, DataFrame is the API."""),

        Q("Client vs cluster deploy mode",
          "What is the difference between client and cluster mode in `spark-submit`?",
          level="medium",
          answer="""
- **client** mode: the driver runs on the machine where you run spark-submit (your laptop, an edge node, a notebook).
  Good for interactive work; the job dies if that machine disconnects.
- **cluster** mode: the driver runs **inside** the cluster (e.g. in a YARN container). Good for production jobs.

On Dataproc, `gcloud dataproc jobs submit pyspark` runs the driver on the master node by default."""),

        Q("Joining on a column with NULLs",
          "Two tables are joined on `customer_id`, and both have NULL keys. What happens? "
          "What if 90% of the rows have a NULL key?",
          level="medium",
          answer="""
NULL never equals NULL, so NULL-key rows never match: an inner join drops them, a left join keeps them with NULLs on
the right. If you need NULL = NULL use `eqNullSafe` / `<=>`.

With 90% NULL keys, all those rows hash to the **same** partition in a sort-merge join → huge skew. Fix: split the
NULL-key rows off (they cannot match anyway), join only the non-NULL rows, then union the NULL rows back."""),

        Q("Reading from BigQuery with Spark",
          "How do you read a big BigQuery table efficiently from Spark (e.g. on Dataproc)?",
          level="hard",
          answer="""
Use the **spark-bigquery connector** (`spark.read.format("bigquery").load("project.dataset.table")`). It reads through
the BigQuery **Storage Read API** in parallel streams (Arrow/Avro).

Make it cheap and fast: select only needed columns (**column pruning**) and filter early (**predicate pushdown**:
`.option("filter", "load_date = '2025-01-01'")` or a `.filter()` that is pushed), especially on the partition column.
Prefer one slice (date + load version) per run. Write back with the direct or indirect (GCS) write method."""),

        Q("Idempotent Spark jobs",
          "How do you make a daily Spark job safe to rerun?",
          level="medium",
          answer="""
Make the output depend only on the **inputs and the run date**, not on how many times it ran:
- write by slice with **dynamic partition overwrite** (or delete-then-insert / MERGE in a table format),
- never plain `append` without a dedupe key,
- add `run_id` / `load_ts` audit columns,
- deduplicate the input by a business key,
- reconcile count + sum and fail before publishing on mismatch.
A rerun then replaces the slice instead of adding duplicates."""),
    ],
)
