"""
spark_setup.py - starts Spark on your Mac and loads the practice tables.

Use it at the top of every practice file:

    import sys; sys.path.insert(0, "spark")      # run from the repo root
    from spark_setup import get_spark, load_tables
    spark = get_spark()
    t = load_tables(spark)          # t["employees"], t["orders"], ...

After load_tables() you can ALSO use plain SQL on the same tables:

    spark.sql("SELECT * FROM employees").show()
"""
from pathlib import Path
from pyspark.sql import SparkSession

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from settings import DATA_DIR  # noqa: E402   (repo/data)

# Same columns and types in all 3 engines.
SCHEMAS = {
    "departments": "dept_id INT, dept_name STRING",
    "employees":   "emp_id INT, name STRING, dept_id INT, manager_id INT, "
                   "salary DECIMAL(10,2), hire_date DATE",
    "customers":   "customer_id INT, name STRING, city STRING, signup_date DATE",
    "orders":      "order_id INT, customer_id INT, order_date DATE, "
                   "amount DECIMAL(10,2), status STRING",
    "products":    "product_id INT, product_name STRING, category STRING, "
                   "price DECIMAL(10,2)",
    "order_items": "order_id INT, product_id INT, quantity INT",
    "user_logins": "user_id INT, login_date DATE",
}


def get_spark(app_name="practice"):
    """Start (or reuse) a local Spark session that uses all CPU cores."""
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")   # small data -> fast
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")              # hide noisy logs
    return spark


def load_tables(spark, show_counts=False):
    """Read every data/*.tsv file. Returns a dict of DataFrames and also
    registers each one as a SQL view with the same name."""
    tables = {}
    for name, schema in SCHEMAS.items():
        df = (
            spark.read
            .option("header", True)
            .option("sep", "\t")
            .option("nullValue", "NULL")    # NULL is written as the text NULL
            .schema(schema)
            .csv(str(DATA_DIR / f"{name}.tsv"))
        )
        df.createOrReplaceTempView(name)
        tables[name] = df
        if show_counts:
            print(f"{name:12} {df.count():>5} rows")
    return tables


def load_ipython_extension(ipython):
    """In a notebook, `%load_ext spark_setup` lets you write %%sql at the top
    of a cell. The cell runs with spark.sql() and shows a nice table."""
    from IPython.core.magic import register_cell_magic

    @register_cell_magic
    def sql(line, cell):
        spark = get_spark()
        return spark.sql(cell).limit(1000).toPandas()
