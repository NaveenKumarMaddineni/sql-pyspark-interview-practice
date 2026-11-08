BASKETS = """
baskets = (order_items.join(products, "product_id")
           .groupBy("order_id")
           .agg(F.sort_array(F.collect_list("product_name")).alias("items")))
"""

TOPIC = dict(
    title="15 · Arrays, structs, maps and JSON",
    engines=["spark"],
    nocache=True,
    intro="""
Real data (events, APIs, BigQuery exports) is often **nested**:
- **array**: a list of values in one cell, e.g. `["Mug", "Cap"]`
- **struct**: a small record in one cell, e.g. `{city: "Austin", zip: "73301"}`, read with `col("address.city")`
- **map**: key → value pairs, e.g. `{"color": "red"}`

Key tools: `collect_list` (rows → array), `explode` (array → rows), `from_json` / `to_json`.
BigQuery has the same ideas: `ARRAY_AGG`, `UNNEST`, `STRUCT`.
""",
    questions=[
        Q("Rows to an array (collect_list)",
          "Make `baskets`: one row per `order_id` with `items` = the sorted list of product names in that order. "
          "Show orders 1001–1003.",
          level="easy",
          sdf=BASKETS + """res = baskets.filter(F.col("order_id") <= 1003).orderBy("order_id")""",
          ssql="""
SELECT i.order_id, SORT_ARRAY(COLLECT_LIST(p.product_name)) AS items
FROM order_items i
JOIN products p ON p.product_id = i.product_id
WHERE i.order_id <= 1003
GROUP BY i.order_id
ORDER BY i.order_id""",
          note="`collect_list` keeps duplicates, `collect_set` removes them. The order inside the list is "
               "**not guaranteed**: sort it if it matters.",
          ordered=True),

        Q("Array to rows (explode)",
          "From `baskets`, make one row per (order_id, item) again, with the **position** of each item. "
          "Show orders 1001–1002.",
          level="easy",
          sdf=BASKETS + """res = (baskets.filter("order_id <= 1002")
       .select("order_id", F.posexplode("items").alias("pos", "item"))
       .orderBy("order_id", "pos"))""",
          ssql="""
WITH baskets AS (
    SELECT i.order_id, SORT_ARRAY(COLLECT_LIST(p.product_name)) AS items
    FROM order_items i JOIN products p ON p.product_id = i.product_id
    GROUP BY i.order_id
)
SELECT order_id, pos, item
FROM baskets
LATERAL VIEW POSEXPLODE(items) x AS pos, item
WHERE order_id <= 1002
ORDER BY order_id, pos""",
          note="`explode` drops rows whose array is empty or NULL. `explode_outer` keeps them (with NULL).",
          ordered=True),

        Q("Array functions",
          "For each basket show `order_id`, `n_items` (size), `has_mug` (contains `Coffee Mug`), and "
          "`upper_items` (every name in capitals, with `transform`). Only baskets with 3+ items.",
          level="medium", compare=False, ssql="SKIP",
          sdf=BASKETS + """res = (baskets
       .select("order_id",
               F.size("items").alias("n_items"),
               F.array_contains("items", "Coffee Mug").alias("has_mug"),
               F.transform("items", lambda x: F.upper(x)).alias("upper_items"))
       .filter("n_items >= 3").orderBy("order_id"))""",
          note="**Higher-order functions** (`transform`, `filter`, `exists`, `aggregate`) work on each element "
               "**inside** the array, with no explode and no UDF. Fast."),

        Q("Build and read a struct",
          "Make `customers_nested` with columns `customer_id` and `info` = struct(name, city, signup_date). "
          "Print the schema, then select `info.city` back out for customers 1–3.",
          level="easy", ssql="SKIP",
          sdf="""
customers_nested = customers.select("customer_id", F.struct("name", "city", "signup_date").alias("info"))
customers_nested.printSchema()
res = customers_nested.filter("customer_id <= 3").select("customer_id", F.col("info.city").alias("city"))""",
          note="`info.*` expands all fields of a struct into columns: `select(\"customer_id\", \"info.*\")`."),

        Q("Maps",
          "For products 1–3 build `attrs` = map(`category` → category, `price` → price as string). "
          "Then show `product_id`, the map, and `attrs['category']`.",
          level="medium", ssql="SKIP",
          sdf="""
res = (products.filter("product_id <= 3")
       .select("product_id",
               F.create_map(F.lit("category"), F.col("category"),
                            F.lit("price"), F.col("price").cast("string")).alias("attrs"))
       .withColumn("cat", F.col("attrs")["category"]))""",
          note="All map values must have **one type**, so the price is cast to string. "
               "Use a struct when fields have different types."),

        Q("Parse JSON text (from_json)",
          "A raw table has JSON text in a column. Parse it with a schema and pull out the fields: "
          "`event`, `user.id`, and the number of `tags`.",
          level="medium", ssql="SKIP",
          sdf="""
raw = spark.createDataFrame([
    ('{"event": "login",  "user": {"id": 1, "country": "IN"}, "tags": ["web", "new"]}',),
    ('{"event": "logout", "user": {"id": 2, "country": "UK"}, "tags": []}',),
    ('not json at all',),
], "value string")

schema = "event STRING, user STRUCT<id: INT, country: STRING>, tags ARRAY<STRING>"
parsed = raw.withColumn("j", F.from_json("value", schema))
res = parsed.select(F.col("j.event").alias("event"),
                    F.col("j.user.id").alias("user_id"),
                    F.size("j.tags").alias("n_tags"))""",
          note="Bad JSON gives NULL fields (no error). `get_json_object(value, '$.user.id')` reads one path "
               "without a schema. `schema_of_json` can suggest a schema from a sample."),

        Q("Back to JSON (to_json)",
          "For departments 1–2 make one JSON text per department: `{\"dept\": ..., \"people\": [names...]}`.",
          level="medium", compare=False, ssql="SKIP",
          sdf="""
res = (employees.filter("dept_id <= 2").join(departments, "dept_id")
       .groupBy("dept_name").agg(F.sort_array(F.collect_list("name")).alias("people"))
       .select(F.to_json(F.struct(F.col("dept_name").alias("dept"), "people")).alias("json")))
for row in res.collect():
    print(row.json)""",
          note="Common when you send data to an API or a message queue (e.g. Pub/Sub)."),

        Q("Flatten nested data",
          "Turn this nested order into flat rows (one per line item): order_id, customer.name, item.sku, item.qty.",
          level="hard",
          sdf="""
nested = spark.createDataFrame(
    [(1, ("Aarav", "Chicago"), [("SKU-1", 2), ("SKU-7", 1)]),
     (2, ("Priya", "Seattle"), [("SKU-3", 5)])],
    "order_id int, customer struct<name:string, city:string>, lines array<struct<sku:string, qty:int>>")
res = (nested
       .select("order_id", F.col("customer.name").alias("customer_name"), F.explode("lines").alias("line"))
       .select("order_id", "customer_name", "line.sku", "line.qty"))""",
          ssql="""
WITH nested AS (
    SELECT 1 AS order_id, NAMED_STRUCT('name', 'Aarav', 'city', 'Chicago') AS customer,
           ARRAY(NAMED_STRUCT('sku', 'SKU-1', 'qty', 2), NAMED_STRUCT('sku', 'SKU-7', 'qty', 1)) AS lines
    UNION ALL
    SELECT 2, NAMED_STRUCT('name', 'Priya', 'city', 'Seattle'),
           ARRAY(NAMED_STRUCT('sku', 'SKU-3', 'qty', 5))
)
SELECT order_id, customer.name AS customer_name, line.sku, line.qty
FROM nested
LATERAL VIEW EXPLODE(lines) t AS line""",
          note="Pattern: **explode** the arrays, then **select struct fields** with dots. "
               "In BigQuery: `FROM t, UNNEST(t.lines) AS line`."),

        Q("Zip two arrays",
          "Each row has a list of `skus` and a list of `qtys` of the same length. Pair them up and explode to "
          "rows (sku, qty).",
          level="medium",
          sdf="""
df = spark.createDataFrame([(1, ["A", "B"], [2, 5]), (2, ["C"], [1])],
                           "order_id int, skus array<string>, qtys array<int>")
res = (df.select("order_id", F.explode(F.arrays_zip("skus", "qtys")).alias("z"))
         .select("order_id", F.col("z.skus").alias("sku"), F.col("z.qtys").alias("qty")))""",
          ssql="""
SELECT order_id, z.skus AS sku, z.qtys AS qty
FROM VALUES (1, ARRAY('A', 'B'), ARRAY(2, 5)), (2, ARRAY('C'), ARRAY(1)) AS t(order_id, skus, qtys)
LATERAL VIEW EXPLODE(ARRAYS_ZIP(skus, qtys)) x AS z"""),
    ],
)

# "How it works" text for each question, in order (shown in the solutions).
HOW = [
    "groupBy order, `collect_list(product_name)` gathers names into an array, `sort_array` sorts it.",
    "`posexplode` turns each array element into its own row, with its position.",
    "Array functions work on the whole list: `size`, `array_contains`, and `transform` applies a function to each element.",
    "`F.struct(...)` packs columns into one struct column; `col(\"info.city\")` reads one field back.",
    "`create_map(key1, value1, key2, value2)` builds a map; `col(\"attrs\")[\"category\"]` reads one key.",
    "`from_json(text, schema)` turns JSON text into a struct; then read fields with dots. Bad JSON → NULL.",
    "Build a struct (dept + list of people), then `to_json` turns it into a JSON string.",
    "`explode` the lines array into rows, then select fields of the struct with dots.",
    "`arrays_zip` pairs elements by position into structs; `explode` makes one row per pair.",
]
