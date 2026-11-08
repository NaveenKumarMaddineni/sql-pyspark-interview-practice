# ranking_example.py - one problem solved two ways: Spark SQL and the DataFrame API.
# In VS Code, click "Run Cell" above each "# %%" line to run one part at a time.
# Or run the whole file in the terminal:  python3 spark/examples/ranking_example.py

# %% Start Spark and load the tables
import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from spark_setup import get_spark, load_tables
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = get_spark()
t = load_tables(spark, show_counts=True)
employees, departments = t["employees"], t["departments"]

# %% Problem: rank employees by salary inside each department
# Way 1: Spark SQL (plain SQL)
spark.sql("""
    SELECT dept_id, name, salary,
           ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC, name ASC) AS rn,
           DENSE_RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC)           AS drnk
    FROM employees
    WHERE dept_id IN (1, 2)
    ORDER BY dept_id, rn
""").show()

# %% Way 2: DataFrame API
w = Window.partitionBy("dept_id").orderBy(F.col("salary").desc(), F.col("name").asc())
w_rank = Window.partitionBy("dept_id").orderBy(F.col("salary").desc())
(employees
    .where(F.col("dept_id").isin(1, 2))
    .withColumn("rn", F.row_number().over(w))
    .withColumn("drnk", F.dense_rank().over(w_rank))
    .select("dept_id", "name", "salary", "rn", "drnk")
    .orderBy("dept_id", "rn")
    .show())

# %% Bonus: departments with no employees (LEFT ANTI JOIN)
departments.join(employees, "dept_id", "left_anti").show()
