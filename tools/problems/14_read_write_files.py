TOPIC = dict(
    title="14 · Reading and writing files",
    engines=["spark"],
    nocache=True,
    intro="""
Spark reads and writes many formats: CSV, JSON, **Parquet** (columnar, compressed, keeps types: the default
choice), ORC, Avro, and tables (Delta, Iceberg, BigQuery via a connector).

```
spark.read.format("csv").option("header", True).schema(...).load(path)
df.write.mode("overwrite").partitionBy("col").parquet(path)
```
A Spark "file" is really a **folder** with one part-file per partition. Practice files go to `tmp/` (not uploaded to git).
""",
    questions=[
        Q("Read CSV: inferSchema vs explicit schema",
          "Read `data/orders.tsv` (tab separated, header, NULL written as `NULL`) twice: "
          "(1) with `inferSchema=True`, (2) with an explicit schema. Print both schemas. What differs?",
          level="easy", compare=False, ssql="SKIP",
          sdf="""
path = str(ROOT / "data" / "orders.tsv")
inferred = (spark.read.option("header", True).option("sep", "\\t")
            .option("nullValue", "NULL").option("inferSchema", True).csv(path))
inferred.printSchema()

explicit = (spark.read.option("header", True).option("sep", "\\t").option("nullValue", "NULL")
            .schema("order_id INT, customer_id INT, order_date DATE, amount DECIMAL(10,2), status STRING")
            .csv(path))
explicit.printSchema()""",
          note="`inferSchema` guessed `amount` as **double** (a float: money can get rounding errors), "
               "not decimal(10,2). It also reads the file **one extra time** to guess. "
               "In production always give a schema."),

        Q("Bad rows: PERMISSIVE, DROPMALFORMED, FAILFAST",
          "Write a small CSV with one bad row (`abc` in the amount column) to `tmp/bad.csv`. Read it with "
          "schema `id INT, amount DOUBLE` in the 3 modes and print the row count each time "
          "(FAILFAST will fail: catch the error). In PERMISSIVE mode keep the bad text in a `_corrupt` column.",
          level="medium", compare=False, ssql="SKIP",
          sdf="""
bad = ROOT / "tmp" / "bad.csv"
bad.parent.mkdir(exist_ok=True)
bad.write_text("id,amount\\n1,10.5\\n2,abc\\n3,7\\n")

schema = "id INT, amount DOUBLE, _corrupt STRING"
permissive = (spark.read.option("header", True).option("mode", "PERMISSIVE")
              .option("columnNameOfCorruptRecord", "_corrupt").schema(schema).csv(str(bad)))
permissive.show()

dropped = spark.read.option("header", True).option("mode", "DROPMALFORMED") \\
               .schema("id INT, amount DOUBLE").csv(str(bad))
print("DROPMALFORMED count():", dropped.count())          # 3 ?!
print("DROPMALFORMED real rows:", len(dropped.collect()))  # 2

try:
    spark.read.option("header", True).option("mode", "FAILFAST") \\
         .schema("id INT, amount DOUBLE").csv(str(bad)).collect()
except Exception as e:
    print("FAILFAST error:", type(e).__name__)""",
          note="Trap: `count()` does not parse the columns, so DROPMALFORMED does not see the bad row and "
               "says 3. `collect()` parses everything: 2. "
               "PERMISSIVE (default) = bad values become NULL, the raw line goes to the corrupt column: "
               "best for pipelines (count and route bad rows). DROPMALFORMED silently loses data. "
               "FAILFAST stops the job."),

        Q("Write and read Parquet",
          "Write `orders` to `tmp/orders_pq` as Parquet (overwrite). Read it back, print the schema and the "
          "count. Then list the files in the folder.",
          level="easy", compare=False, ssql="SKIP",
          sdf="""
out = ROOT / "tmp" / "orders_pq"
orders.write.mode("overwrite").parquet(str(out))
back = spark.read.parquet(str(out))
back.printSchema()
print(back.count())
print(sorted(p.name for p in out.iterdir() if not p.name.startswith(".")))""",
          note="Parquet keeps the schema inside the file, so no options are needed to read it. "
               "`_SUCCESS` is an empty marker file: the write finished."),

        Q("Save modes",
          "Write `departments` to `tmp/dept_pq` with mode `overwrite`, then twice with `append`. Print the "
          "count after each. Then try mode `errorifexists` and catch the error, and `ignore`.",
          level="easy", compare=False, ssql="SKIP",
          sdf="""
out = str(ROOT / "tmp" / "dept_pq")
departments.write.mode("overwrite").parquet(out)
print("overwrite:", spark.read.parquet(out).count())
for i in range(2):
    departments.write.mode("append").parquet(out)
    print("append:", spark.read.parquet(out).count())
try:
    departments.write.mode("errorifexists").parquet(out)
except Exception as e:
    print("errorifexists ->", type(e).__name__)
departments.write.mode("ignore").parquet(out)     # does nothing, folder exists
print("after ignore:", spark.read.parquet(out).count())""",
          note="`append` on a rerun = duplicates. Pipelines that may rerun should **overwrite a slice** "
               "(next question) or MERGE into a table format."),

        Q("Dynamic partition overwrite",
          "Write orders partitioned by `order_month` (yyyy-MM text) to `tmp/orders_by_month`. "
          "Then rewrite **only** month `2025-10` with deduplicated rows, using "
          "`partitionOverwriteMode=dynamic`. Show that other months are untouched.",
          level="hard", compare=False, ssql="SKIP",
          sdf="""
out = str(ROOT / "tmp" / "orders_by_month")
with_month = orders.withColumn("order_month", F.date_format("order_date", "yyyy-MM"))
with_month.write.mode("overwrite").partitionBy("order_month").parquet(out)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
oct_clean = with_month.filter("order_month = '2025-10'").dropDuplicates()
oct_clean.write.mode("overwrite").partitionBy("order_month").parquet(out)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")    # back to default

back = spark.read.parquet(out)
back.groupBy("order_month").count().orderBy("order_month").filter("order_month >= '2025-08'").show()
print("total rows:", back.count())""",
          note="With the default (**static**) mode, `overwrite` deletes **all** partitions first: you would lose "
               "every other month. **Dynamic** mode replaces only the partitions present in the new data. "
               "This makes a daily job idempotent."),

        Q("Export a pipe-delimited CSV + control file",
          "Like a real hand-off to another system: write **completed** orders (deduplicated) as **one** CSV file "
          "with a header, `|` as separator, dates as `yyyy-MM-dd`, to `tmp/export`. Then write a control file "
          "`tmp/export.ctl` with the row count and the sum of amount. Print both files' first lines.",
          level="hard", compare=False, ssql="SKIP",
          sdf="""
out = ROOT / "tmp" / "export"
data = orders.filter("status = 'completed'").dropDuplicates()
(data.coalesce(1)                       # one output file (fine for small data)
     .write.mode("overwrite")
     .option("header", True).option("sep", "|")
     .option("dateFormat", "yyyy-MM-dd").option("nullValue", "")
     .csv(str(out)))

stats = data.agg(F.count("*").alias("n"), F.sum("amount").alias("total")).first()
(ROOT / "tmp" / "export.ctl").write_text(f"row_count|{stats.n}\\namount_sum|{stats.total}\\n")

part = next(out.glob("part-*.csv"))
print(part.read_text().splitlines()[:3])
print((ROOT / "tmp" / "export.ctl").read_text())""",
          note="`coalesce(1)` = one file, but also **one task** writes everything: OK for small exports, slow "
               "for big ones. The control file lets the receiver (e.g. an Oracle loader) **reconcile** "
               "count and sum before loading."),

        Q("JSON: write, read, multiline",
          "Write `products` as JSON lines to `tmp/products_json` and read it back. Then read a "
          "**multi-line** JSON array file `tmp/one.json` (write it first) with `multiLine=True`.",
          level="medium", compare=False, ssql="SKIP",
          sdf="""
out = str(ROOT / "tmp" / "products_json")
products.write.mode("overwrite").json(out)
spark.read.json(out).show(3)

(ROOT / "tmp" / "one.json").write_text('[\\n {"id": 1, "tags": ["a", "b"]},\\n {"id": 2, "tags": []}\\n]')
spark.read.option("multiLine", True).json(str(ROOT / "tmp" / "one.json")).show()""",
          note="Spark expects **one JSON object per line** by default (JSON Lines). A pretty-printed file "
               "or a JSON array needs `multiLine=True`. Reading JSON without a schema scans the data to infer it."),

        Q("How many output files?",
          "Write `order_items` with `repartition(3)` to `tmp/items_3` and with `coalesce(1)` to `tmp/items_1`. "
          "Count the part-files in each folder.",
          level="easy", compare=False, ssql="SKIP",
          sdf="""
for name, df in [("items_3", order_items.repartition(3)), ("items_1", order_items.coalesce(1))]:
    out = ROOT / "tmp" / name
    df.write.mode("overwrite").parquet(str(out))
    print(name, len(list(out.glob("part-*"))), "part-files")""",
          note="Number of files = number of partitions when writing (× partition folders with partitionBy). "
               "Too many small files is a classic performance problem; aim for files of ~128 MB–1 GB."),

        Q("Schema evolution: mergeSchema",
          "Write 2 Parquet folders under `tmp/evolve`: `v=1` with columns (id, name), `v=2` with (id, name, city). "
          "Read the parent folder with and without `mergeSchema` and compare the columns.",
          level="medium", compare=False, ssql="SKIP",
          sdf="""
base = ROOT / "tmp" / "evolve"
customers.select("customer_id", "name").limit(3).write.mode("overwrite").parquet(str(base / "v=1"))
customers.select("customer_id", "name", "city").limit(3).write.mode("overwrite").parquet(str(base / "v=2"))
print(spark.read.parquet(str(base)).columns)
print(spark.read.option("mergeSchema", True).parquet(str(base)).columns)""",
          note="Without mergeSchema Spark takes the schema from **one** file, so `city` can be missing. "
               "`v=1` / `v=2` folder names also become a column `v` (partition discovery)."),
    ],
)

# "How it works" text for each question, in order (shown in the solutions).
HOW = [
    "Read the same file twice; `printSchema` shows the guessed types vs the types you gave.",
    "PERMISSIVE keeps the row with NULLs and puts the raw line in `_corrupt`; DROPMALFORMED drops it; FAILFAST raises an error on read.",
    "`write.parquet` creates a folder of part-files; `read.parquet` gets the schema from the files.",
    "Each `append` adds the 7 rows again; `errorifexists` refuses an existing folder; `ignore` silently does nothing.",
    "With dynamic mode, overwrite deletes and rewrites only the partition folders that appear in the new data.",
    "`coalesce(1)` → one CSV part-file with `|` separator; then compute count and sum and write them to a small control file.",
    "JSON Lines is read by default; a JSON array or pretty file needs `multiLine=True`.",
    "Each partition writes one part-file, so the partition count before writing = number of files.",
    "`mergeSchema` reads the schemas of all files and combines them; without it Spark trusts one file.",
]
