TOPIC = dict(
    title="06 · Dates and strings",
    intro="""
Dates and strings are where the 3 engines differ the most. Learn the **idea** once, then the names:

| idea | BigQuery | Oracle | Spark |
|---|---|---|---|
| add days | `DATE_ADD(d, INTERVAL 7 DAY)` | `d + 7` | `DATE_ADD(d, 7)` |
| days between | `DATE_DIFF(a, b, DAY)` | `a - b` | `DATEDIFF(a, b)` |
| first day of month | `DATE_TRUNC(d, MONTH)` | `TRUNC(d, 'MM')` | `TRUNC(d, 'MM')` |
| date → text | `FORMAT_DATE('%d/%m/%Y', d)` | `TO_CHAR(d, 'DD/MM/YYYY')` | `DATE_FORMAT(d, 'dd/MM/yyyy')` |
| text → date | `PARSE_DATE('%d/%m/%Y', s)` | `TO_DATE(s, 'DD/MM/YYYY')` | `TO_DATE(s, 'dd/MM/yyyy')` |
""",
    questions=[
        Q("Parts of a date",
          "For each employee show `name`, `hire_year`, `hire_month` (number) and `hire_weekday` "
          "(3-letter English day name in CAPITALS, like `MON`).",
          level="easy",
          bq="""
SELECT name,
       EXTRACT(YEAR FROM hire_date)          AS hire_year,
       EXTRACT(MONTH FROM hire_date)         AS hire_month,
       UPPER(FORMAT_DATE('%a', hire_date))   AS hire_weekday
FROM practice.employees""",
          ora="""
SELECT name,
       EXTRACT(YEAR FROM hire_date)  AS hire_year,
       EXTRACT(MONTH FROM hire_date) AS hire_month,
       TO_CHAR(hire_date, 'DY', 'NLS_DATE_LANGUAGE=ENGLISH') AS hire_weekday
FROM employees""",
          ssql="""
SELECT name,
       EXTRACT(YEAR FROM hire_date)  AS hire_year,
       EXTRACT(MONTH FROM hire_date) AS hire_month,
       UPPER(DATE_FORMAT(hire_date, 'EEE')) AS hire_weekday
FROM employees""",
          sdf="""
res = employees.select("name",
                       F.year("hire_date").alias("hire_year"),
                       F.month("hire_date").alias("hire_month"),
                       F.upper(F.date_format("hire_date", "EEE")).alias("hire_weekday"))""",
          notes={"oracle": "Day names depend on the session language (NLS). Pass "
                           "`'NLS_DATE_LANGUAGE=ENGLISH'` to be safe."}),

        Q("Full years of service",
          "Show `name`, `hire_date` and `years_of_service` = **complete** years from hire_date to "
          "2025-06-30. Someone hired 2016-11-05 has **8** full years on that day, not 9.",
          level="hard",
          bq="""
SELECT name, hire_date,
       DIV(DATE_DIFF(DATE '2025-06-30', hire_date, MONTH)
           - IF(EXTRACT(DAY FROM DATE '2025-06-30') < EXTRACT(DAY FROM hire_date), 1, 0), 12)
         AS years_of_service
FROM practice.employees""",
          ora="""
SELECT name, hire_date,
       FLOOR(MONTHS_BETWEEN(DATE '2025-06-30', hire_date) / 12) AS years_of_service
FROM employees""",
          ssql="""
SELECT name, hire_date,
       FLOOR(MONTHS_BETWEEN(DATE '2025-06-30', hire_date) / 12) AS years_of_service
FROM employees""",
          sdf="""
res = employees.select("name", "hire_date",
                       F.floor(F.months_between(F.lit("2025-06-30").cast("date"), "hire_date") / 12)
                        .alias("years_of_service"))""",
          note="Henry Clark: hired 2016-11-05, so on 2025-06-30 he has 8 years and ~8 months: **8** full years. "
               "`2025 - 2016 = 9` is wrong.",
          notes={"bigquery": "Trap: `DATE_DIFF(end, start, YEAR)` counts **year boundaries crossed**, "
                             "not full years (2024-12-31 → 2025-01-01 = 1). Count months, fix the day, divide by 12."}),

        Q("Delivery date and month end",
          "For orders 1001–1005 (each once) show `order_id`, `order_date`, `due_date` = order_date + 7 days, "
          "and `month_end` = last day of the order's month.",
          level="easy",
          bq="""
SELECT DISTINCT order_id, order_date,
       DATE_ADD(order_date, INTERVAL 7 DAY) AS due_date,
       LAST_DAY(order_date)                 AS month_end
FROM practice.orders
WHERE order_id BETWEEN 1001 AND 1005
ORDER BY order_id""",
          ora="""
SELECT DISTINCT order_id, order_date,
       order_date + 7       AS due_date,
       LAST_DAY(order_date) AS month_end
FROM orders
WHERE order_id BETWEEN 1001 AND 1005
ORDER BY order_id""",
          ssql="""
SELECT DISTINCT order_id, order_date,
       DATE_ADD(order_date, 7) AS due_date,
       LAST_DAY(order_date)    AS month_end
FROM orders
WHERE order_id BETWEEN 1001 AND 1005
ORDER BY order_id""",
          sdf="""
res = (orders.filter(F.col("order_id").between(1001, 1005))
       .select("order_id", "order_date",
               F.date_add("order_date", 7).alias("due_date"),
               F.last_day("order_date").alias("month_end"))
       .distinct().orderBy("order_id"))""",
          notes={"oracle": "Months: `ADD_MONTHS(d, 1)`. Adding a number to a DATE adds days."},
          ordered=True),

        Q("Orders per weekday",
          "Count different orders per weekday: `weekday` (`MON` … `SUN`) and `num_orders`. "
          "Most orders first, then weekday name.",
          level="medium",
          bq="""
SELECT UPPER(FORMAT_DATE('%a', order_date)) AS weekday,
       COUNT(DISTINCT order_id)             AS num_orders
FROM practice.orders
GROUP BY weekday
ORDER BY num_orders DESC, weekday""",
          ora="""
SELECT TO_CHAR(order_date, 'DY', 'NLS_DATE_LANGUAGE=ENGLISH') AS weekday,
       COUNT(DISTINCT order_id)                               AS num_orders
FROM orders
GROUP BY TO_CHAR(order_date, 'DY', 'NLS_DATE_LANGUAGE=ENGLISH')
ORDER BY num_orders DESC, weekday""",
          ssql="""
SELECT UPPER(DATE_FORMAT(order_date, 'EEE')) AS weekday,
       COUNT(DISTINCT order_id)              AS num_orders
FROM orders
GROUP BY weekday
ORDER BY num_orders DESC, weekday""",
          sdf="""
res = (orders.groupBy(F.upper(F.date_format("order_date", "EEE")).alias("weekday"))
       .agg(F.countDistinct("order_id").alias("num_orders"))
       .orderBy(F.desc("num_orders"), "weekday"))""",
          note="Day **numbers** differ: BigQuery `EXTRACT(DAYOFWEEK ...)` is 1 = Sunday; Oracle `TO_CHAR(d, 'D')` "
               "depends on the country setting; Spark `DAYOFWEEK` is 1 = Sunday, `WEEKDAY` is 0 = Monday.",
          ordered=True),

        Q("Recent sign-ups",
          "Show `customer_id`, `name`, `signup_date` of customers who signed up in the **90 days up to and "
          "including** 2024-11-30.",
          level="easy",
          bq="""
SELECT customer_id, name, signup_date
FROM practice.customers
WHERE signup_date >  DATE_SUB(DATE '2024-11-30', INTERVAL 90 DAY)
  AND signup_date <= DATE '2024-11-30'""",
          ora="""
SELECT customer_id, name, signup_date
FROM customers
WHERE signup_date >  DATE '2024-11-30' - 90
  AND signup_date <= DATE '2024-11-30'""",
          ssql="""
SELECT customer_id, name, signup_date
FROM customers
WHERE signup_date >  DATE_SUB(DATE '2024-11-30', 90)
  AND signup_date <= DATE '2024-11-30'""",
          sdf="""
end = F.lit("2024-11-30").cast("date")
res = (customers.filter((F.col("signup_date") > F.date_sub(end, 90)) & (F.col("signup_date") <= end))
       .select("customer_id", "name", "signup_date"))""",
          note="In real jobs use `CURRENT_DATE` (or `SYSDATE` / `TRUNC(SYSDATE)` in Oracle). "
               "A fixed date keeps practice answers stable. Be clear about > vs >= at both ends."),

        Q("First and last name",
          "Split employee `name` at the space: show `name`, `first_name`, `last_name`.",
          level="easy",
          bq="""
SELECT name,
       SPLIT(name, ' ')[OFFSET(0)] AS first_name,
       SPLIT(name, ' ')[OFFSET(1)] AS last_name
FROM practice.employees""",
          ora="""
SELECT name,
       SUBSTR(name, 1, INSTR(name, ' ') - 1) AS first_name,
       SUBSTR(name, INSTR(name, ' ') + 1)    AS last_name
FROM employees""",
          ssql="""
SELECT name,
       SPLIT(name, ' ')[0] AS first_name,
       SPLIT(name, ' ')[1] AS last_name
FROM employees""",
          sdf="""
parts = F.split("name", " ")
res = employees.select("name", parts[0].alias("first_name"), parts[1].alias("last_name"))""",
          note="`INSTR(s, ' ')` = position of the space. `SUBSTR(s, start, length)`. "
               "Positions start at **1** in SQL. BigQuery `OFFSET(0)` and Spark `[0]` start at 0.",
          notes={"spark": "Spark also has `SUBSTRING_INDEX(name, ' ', 1)` (text before the 1st space)."}),

        Q("Initials and lengths",
          "For customers 1–5 show `name`, `initials` like `A.N.`, `name_upper` and `name_length`.",
          level="easy",
          bq="""
SELECT name,
       SUBSTR(name, 1, 1) || '.' || SUBSTR(SPLIT(name, ' ')[OFFSET(1)], 1, 1) || '.' AS initials,
       UPPER(name)  AS name_upper,
       LENGTH(name) AS name_length
FROM practice.customers
WHERE customer_id <= 5
ORDER BY customer_id""",
          ora="""
SELECT name,
       SUBSTR(name, 1, 1) || '.' || SUBSTR(name, INSTR(name, ' ') + 1, 1) || '.' AS initials,
       UPPER(name)  AS name_upper,
       LENGTH(name) AS name_length
FROM customers
WHERE customer_id <= 5
ORDER BY customer_id""",
          ssql="""
SELECT name,
       SUBSTR(name, 1, 1) || '.' || SUBSTR(name, INSTR(name, ' ') + 1, 1) || '.' AS initials,
       UPPER(name)  AS name_upper,
       LENGTH(name) AS name_length
FROM customers
WHERE customer_id <= 5
ORDER BY customer_id""",
          sdf="""
res = (customers.filter("customer_id <= 5").orderBy("customer_id")
       .select("name",
               F.concat(F.substring("name", 1, 1), F.lit("."),
                        F.substring(F.split("name", " ")[1], 1, 1), F.lit(".")).alias("initials"),
               F.upper("name").alias("name_upper"),
               F.length("name").alias("name_length")))""",
          ordered=True),

        Q("Padded employee codes",
          "Show `emp_id`, `name` and `emp_code` like `E0001`, `E0030` (E + emp_id padded to 4 digits).",
          level="easy",
          bq="""
SELECT emp_id, name, 'E' || LPAD(CAST(emp_id AS STRING), 4, '0') AS emp_code
FROM practice.employees""",
          ora="""
SELECT emp_id, name, 'E' || LPAD(emp_id, 4, '0') AS emp_code
FROM employees""",
          ssql="""
SELECT emp_id, name, 'E' || LPAD(CAST(emp_id AS STRING), 4, '0') AS emp_code
FROM employees""",
          sdf="""
res = employees.select("emp_id", "name",
                       F.concat(F.lit("E"), F.lpad(F.col("emp_id").cast("string"), 4, "0")).alias("emp_code"))""",
          note="Fixed-width codes are common in files sent to other systems.",
          notes={"oracle": "Oracle turns the number into text for you (implicit conversion). "
                           "`TO_CHAR(emp_id, 'FM0000')` is the explicit way."}),

        Q("Pattern match with a regular expression",
          "Show `product_name` of products whose name contains a **digit** or the word **Pack**.",
          level="medium",
          bq="""
SELECT product_name
FROM practice.products
WHERE REGEXP_CONTAINS(product_name, r'[0-9]|Pack')""",
          ora="""
SELECT product_name
FROM products
WHERE REGEXP_LIKE(product_name, '[0-9]|Pack')""",
          ssql="""
SELECT product_name
FROM products
WHERE product_name RLIKE '[0-9]|Pack'""",
          sdf="""
res = products.filter(F.col("product_name").rlike("[0-9]|Pack")).select("product_name")""",
          note="Also useful: `REGEXP_REPLACE` (all 3 engines), `REGEXP_EXTRACT` (BigQuery, Spark) / "
               "`REGEXP_SUBSTR` (Oracle)."),

        Q("Names per department in one line",
          "Show `dept_id` and `members` = all employee names of the department in one text, "
          "A to Z, separated by `, `. Skip NULL dept_id.",
          level="medium",
          bq="""
SELECT dept_id, STRING_AGG(name, ', ' ORDER BY name) AS members
FROM practice.employees
WHERE dept_id IS NOT NULL
GROUP BY dept_id
ORDER BY dept_id""",
          ora="""
SELECT dept_id, LISTAGG(name, ', ') WITHIN GROUP (ORDER BY name) AS members
FROM employees
WHERE dept_id IS NOT NULL
GROUP BY dept_id
ORDER BY dept_id""",
          ssql="""
SELECT dept_id, LISTAGG(name, ', ') WITHIN GROUP (ORDER BY name) AS members
FROM employees
WHERE dept_id IS NOT NULL
GROUP BY dept_id
ORDER BY dept_id""",
          sdf="""
res = (employees.filter(F.col("dept_id").isNotNull())
       .groupBy("dept_id")
       .agg(F.array_join(F.array_sort(F.collect_list("name")), ", ").alias("members"))
       .orderBy("dept_id"))""",
          notes={"spark": "`LISTAGG` is new in Spark 4. On older Spark use "
                          "`array_join(array_sort(collect_list(name)), ', ')`."},
          ordered=True),

        Q("Date to text for a file export",
          "For orders 1001–1003 (each once) show `order_id` and `order_date_text` in the format "
          "`DD/MM/YYYY` (like `15/03/2023`). Files sent to other systems often need this.",
          level="easy",
          bq="""
SELECT DISTINCT order_id, FORMAT_DATE('%d/%m/%Y', order_date) AS order_date_text
FROM practice.orders
WHERE order_id <= 1003
ORDER BY order_id""",
          ora="""
SELECT DISTINCT order_id, TO_CHAR(order_date, 'DD/MM/YYYY') AS order_date_text
FROM orders
WHERE order_id <= 1003
ORDER BY order_id""",
          ssql="""
SELECT DISTINCT order_id, DATE_FORMAT(order_date, 'dd/MM/yyyy') AS order_date_text
FROM orders
WHERE order_id <= 1003
ORDER BY order_id""",
          sdf="""
res = (orders.filter("order_id <= 1003")
       .select("order_id", F.date_format("order_date", "dd/MM/yyyy").alias("order_date_text"))
       .distinct().orderBy("order_id"))""",
          note="Pattern letters differ! Spark (Java style): `MM` = month, `mm` = **minutes**. "
               "Oracle: `MM` = month, `MI` = minutes. BigQuery: `%m` month, `%M` minutes.",
          ordered=True),

        Q("Text to timestamp",
          "Turn the text `'2024-03-15 14:30:00'` into a timestamp and show `ts` and `ts_hour` (14). "
          "No table needed.",
          level="easy",
          bq="""
SELECT PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', '2024-03-15 14:30:00') AS ts,
       EXTRACT(HOUR FROM PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', '2024-03-15 14:30:00')) AS ts_hour""",
          ora="""
SELECT TO_TIMESTAMP('2024-03-15 14:30:00', 'YYYY-MM-DD HH24:MI:SS') AS ts,
       EXTRACT(HOUR FROM TO_TIMESTAMP('2024-03-15 14:30:00', 'YYYY-MM-DD HH24:MI:SS')) AS ts_hour
FROM dual""",
          ssql="""
SELECT TO_TIMESTAMP('2024-03-15 14:30:00', 'yyyy-MM-dd HH:mm:ss') AS ts,
       HOUR(TO_TIMESTAMP('2024-03-15 14:30:00', 'yyyy-MM-dd HH:mm:ss')) AS ts_hour""",
          sdf="""
res = spark.range(1).select(F.to_timestamp(F.lit("2024-03-15 14:30:00"), "yyyy-MM-dd HH:mm:ss").alias("ts")) \\
               .withColumn("ts_hour", F.hour("ts"))""",
          notes={"bigquery": "BigQuery TIMESTAMP is always UTC. `DATETIME` has no time zone. "
                             "The notebook shows the TIMESTAMP with `+00:00`.",
                 "oracle": "Oracle 19c needs `FROM dual` (a 1-row dummy table). 23ai/26ai lets you leave it out. "
                           "`HH24` = 0–23 hours; `HH` = 1–12."}),

        Q("Logins per ISO week",
          "Count login **rows** per week. Show `week_start` (the Monday) and `num_logins`, by week.",
          level="medium",
          bq="""
SELECT DATE_TRUNC(login_date, ISOWEEK) AS week_start, COUNT(*) AS num_logins
FROM practice.user_logins
GROUP BY week_start
ORDER BY week_start""",
          ora="""
SELECT TRUNC(login_date, 'IW') AS week_start, COUNT(*) AS num_logins
FROM user_logins
GROUP BY TRUNC(login_date, 'IW')
ORDER BY week_start""",
          ssql="""
SELECT CAST(DATE_TRUNC('WEEK', login_date) AS DATE) AS week_start, COUNT(*) AS num_logins
FROM user_logins
GROUP BY 1
ORDER BY week_start""",
          sdf="""
res = (user_logins.groupBy(F.date_trunc("week", "login_date").cast("date").alias("week_start"))
       .agg(F.count("*").alias("num_logins"))
       .orderBy("week_start"))""",
          note="ISO weeks start on **Monday**. BigQuery `WEEK` (not ISOWEEK) starts on Sunday.",
          notes={"spark": "`DATE_TRUNC` returns a timestamp; CAST it to DATE. `GROUP BY 1` = by the 1st column."},
          ordered=True),
    ],
)

# "How it works" text for each question, in order (shown in the solutions).
HOW = [
    "`EXTRACT(YEAR/MONTH FROM date)` gives numbers; a format function (`FORMAT_DATE`, `TO_CHAR`, `DATE_FORMAT`) gives the day name as text.",
    "Count complete months between the dates (minus 1 if the end day is before the start day), then divide by 12 and round **down**.",
    "Add 7 days with each engine's date-add; `LAST_DAY(date)` returns the last day of that month.",
    "Turn each order date into a weekday name, then COUNT DISTINCT order ids per name and sort.",
    "Compute the start of the window (end date − 90 days) and keep sign-ups after the start and up to the end date.",
    "Split the name at the space: array index (BigQuery/Spark) or `SUBSTR` + `INSTR` (position of the space) in Oracle.",
    "`SUBSTR(text, 1, 1)` = first letter; take the first letter of the first and last name, add dots. `UPPER` and `LENGTH` are the same everywhere.",
    "Turn the number into text and pad it on the left with zeros to 4 characters (`LPAD`), then add `E` in front.",
    "A regular expression `[0-9]|Pack` matches any digit **or** the word Pack. Each engine has its own regex function name.",
    "Group by department and join all names into one text, sorted A→Z (`STRING_AGG` / `LISTAGG`).",
    "Format the date with a pattern: day/month/year. Pattern letters are different in each engine.",
    "Parse the text with the matching pattern into a TIMESTAMP, then `EXTRACT(HOUR ...)` / `HOUR(...)`.",
    "Truncate each date to the Monday of its ISO week, then count rows per week.",
]
