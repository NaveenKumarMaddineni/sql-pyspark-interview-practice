# SQL + PySpark interview practice — BigQuery · Oracle · Spark

**225 practice questions** for data engineering interviews. Every SQL question is solved in **BigQuery**,
**Oracle** and **Spark SQL**, plus the **PySpark DataFrame API**, on the same 7 small tables.
Each solution has been run, and a build script checks that **all engines return the same answer**.

- Practice notebooks: the question, an empty cell, and the *expected result* (row count + columns) so you can check yourself.
- Solution notebooks (in a separate folder): the answer, its output, **how it works**, and the **traps** (ties, NULLs, duplicates, fan-out).
- Cheatsheets for a quick review before an interview.

The data is tiny on purpose: you learn the logic and the traps, not wait for big queries. On BigQuery it costs about nothing.

---

## What's inside

| # | Topic | BigQuery | Oracle | Spark (SQL + DataFrame) |
|---|---|:-:|:-:|:-:|
| 01 | SQL basics: WHERE, CASE, NULL, LIMIT/paging | [practice](bigquery/practice/01_basics.ipynb) | [practice](oracle/practice/01_basics.ipynb) | [practice](spark/practice/01_basics.ipynb) |
| 02 | Aggregation: GROUP BY, HAVING, conditional counts, ROLLUP | [practice](bigquery/practice/02_aggregation.ipynb) | [practice](oracle/practice/02_aggregation.ipynb) | [practice](spark/practice/02_aggregation.ipynb) |
| 03 | Joins: anti/semi/self/full/cross, fan-out traps | [practice](bigquery/practice/03_joins.ipynb) | [practice](oracle/practice/03_joins.ipynb) | [practice](spark/practice/03_joins.ipynb) |
| 04 | Subqueries and CTEs: correlated, EXISTS, the NOT IN + NULL trap | [practice](bigquery/practice/04_subqueries_ctes.ipynb) | [practice](oracle/practice/04_subqueries_ctes.ipynb) | [practice](spark/practice/04_subqueries_ctes.ipynb) |
| 05 | Window functions: ranking, running totals, LAG/LEAD, frames | [practice](bigquery/practice/05_window_functions.ipynb) | [practice](oracle/practice/05_window_functions.ipynb) | [practice](spark/practice/05_window_functions.ipynb) |
| 06 | Dates and strings | [practice](bigquery/practice/06_dates_strings.ipynb) | [practice](oracle/practice/06_dates_strings.ipynb) | [practice](spark/practice/06_dates_strings.ipynb) |
| 07 | Set operations: UNION, INTERSECT, EXCEPT/MINUS | [practice](bigquery/practice/07_set_operations.ipynb) | [practice](oracle/practice/07_set_operations.ipynb) | [practice](spark/practice/07_set_operations.ipynb) |
| 08 | Interview patterns: dedupe, gaps & islands, retention, pivot, median, calendar | [practice](bigquery/practice/08_interview_patterns.ipynb) | [practice](oracle/practice/08_interview_patterns.ipynb) | [practice](spark/practice/08_interview_patterns.ipynb) |
| 09 | Recursive queries and hierarchies | [practice](bigquery/practice/09_recursive_hierarchy.ipynb) | [practice](oracle/practice/09_recursive_hierarchy.ipynb) | [practice](spark/practice/09_recursive_hierarchy.ipynb) |
| 10 | Changing data: MERGE/upsert, SCD Type 2, incremental and idempotent loads | [practice](bigquery/practice/10_dml_merge_scd.ipynb) | [practice](oracle/practice/10_dml_merge_scd.ipynb) | [practice](spark/practice/10_dml_merge_scd.ipynb) |
| 11 | Data quality checks and reconciliation | [practice](bigquery/practice/11_data_quality.ipynb) | [practice](oracle/practice/11_data_quality.ipynb) | [practice](spark/practice/11_data_quality.ipynb) |
| 12 | Performance: dry runs, partitions, execution plans | [practice](bigquery/practice/12_performance.ipynb) | [practice](oracle/practice/12_performance.ipynb) | [practice](spark/practice/12_performance.ipynb) |
| 13 | PySpark DataFrame basics | | | [practice](spark/practice/13_dataframe_basics.ipynb) |
| 14 | Reading and writing files (CSV, Parquet, JSON, partition overwrite, CSV export + control file) | | | [practice](spark/practice/14_read_write_files.ipynb) |
| 15 | Arrays, structs, maps, JSON | | | [practice](spark/practice/15_complex_types.ipynb) |
| 16 | Joins in depth: broadcast, skew, salting | | | [practice](spark/practice/16_joins_deep_dive.ipynb) |
| 17 | Partitions, shuffles, caching, lazy evaluation, AQE | | | [practice](spark/practice/17_partitions_shuffle.ipynb) |
| 18 | UDFs and pandas UDFs | | | [practice](spark/practice/18_udfs.ipynb) |
| 19 | Pipeline patterns: audit columns, tests, quality gates, idempotent jobs | | | [practice](spark/practice/19_pipeline_patterns.ipynb) |
| 20 | Spark concepts quiz (theory) | | | [practice](spark/practice/20_spark_concepts_quiz.ipynb) |
| 21 | Data warehouse, modeling, BigQuery & Oracle concepts quiz (theory) | [practice](bigquery/practice/21_warehouse_concepts_quiz.ipynb) | [practice](oracle/practice/21_warehouse_concepts_quiz.ipynb) | |

Solutions: same file names under [`solutions/`](solutions/), e.g. `solutions/bigquery/05_window_functions.ipynb`.

**Cheatsheets** (in [`cheatsheets/`](cheatsheets/)):
- [`interview_cheatsheet.md`](cheatsheets/interview_cheatsheet.md): a short review for the 30 minutes before an interview
- [`sql_cheatsheet.md`](cheatsheets/sql_cheatsheet.md): a longer reference with tested syntax for all 3 engines
- [`tables_guide.md`](cheatsheets/tables_guide.md): the 7 tables, their columns, and their traps

## The practice tables

Same data in all 3 engines (source: [`data/`](data/), tab-separated, NULL written as `NULL`).

```
departments ──< employees            employees.manager_id → employees.emp_id (a hierarchy)
customers   ──< orders ──< order_items >── products
user_logins  (user_id, login_date)   ──< = one to many
```

The data has **traps on purpose**: salary ties, NULL `dept_id` and `manager_id`, a department with no employees,
customers who never ordered, products never sold, **4 exact duplicate rows** and 6 NULL amounts in `orders`
(no primary key), and login streaks with gaps and same-day duplicates.

## Folders

```
bigquery/   bq_helper.py (%%bigquery magic), load_tables.py, practice/
oracle/     oracle_helper.py (%%oracle magic), create_tables.sql, load_tables.py, practice/
spark/      spark_setup.py (get_spark, load_tables, %%sql magic), examples/, practice/
solutions/  bigquery/ · oracle/ · spark/      (answers, outputs, explanations)
cheatsheets/ cheatsheets and the tables guide
data/       the 7 tables as .tsv
tools/      build.py + problems/*.py: the source of all notebooks (see "Add or change a question")
settings.py reads your .env file
```

---

## Setup

You can start with **Spark only**: it runs on your computer and needs no account.
Add Oracle and BigQuery when you want them.

### 1. What you need

| | Needed for | Install |
|---|---|---|
| Python 3.10+ | everything | [python.org](https://www.python.org/downloads/) |
| Java 17 (or 21) | Spark | macOS `brew install openjdk@17` · Ubuntu `sudo apt install openjdk-17-jdk` · Windows: [Temurin 17](https://adoptium.net/) |
| VS Code + **Python** and **Jupyter** extensions | running notebooks | [code.visualstudio.com](https://code.visualstudio.com/) (or use JupyterLab) |
| Docker (Docker Desktop, or Colima on macOS) | Oracle | [docker.com](https://www.docker.com/) · `brew install colima docker` |
| Google Cloud account + `gcloud` CLI | BigQuery | [cloud.google.com/sdk](https://cloud.google.com/sdk/docs/install) |

Check Java: `java -version` should show 17 or 21. If Spark can't find Java, set `JAVA_HOME`
(macOS with Homebrew: `export JAVA_HOME=/opt/homebrew/opt/openjdk@17`).

### 2. Get the code and the Python packages

```bash
git clone https://github.com/<your-user>/<this-repo>.git
cd <this-repo>
uv venv                            # a private Python environment for this project (.venv)
source .venv/bin/activate          # Windows: .venv\Scripts\activate
uv pip install -r requirements.txt # needs uv: brew install uv (https://docs.astral.sh/uv/)
cp .env.example .env               # your personal settings; .env is never uploaded
```

In VS Code: open the folder, open any notebook, click **Select Kernel** (top right) and pick `.venv`.

### 3. Spark (local, free)

Nothing more to install. Open [`spark/practice/01_basics.ipynb`](spark/practice/01_basics.ipynb) and run the first
cell (**Shift+Enter**). It starts Spark on your computer and loads the 7 tables as DataFrames and SQL views.

Quick test from a terminal: `python3 spark/examples/ranking_example.py`

### 4. Oracle (local, free, in Docker)

Oracle Database Free runs in a container. The [`gvenzl/oracle-free`](https://hub.docker.com/r/gvenzl/oracle-free)
image works on Intel and Apple Silicon.

```bash
# macOS with Colima: start the Docker engine first (give Oracle enough memory)
colima start --cpu 4 --memory 4

# Create the database. Choose your own passwords.
docker run -d --name oracle -p 1521:1521 \
  -e ORACLE_PASSWORD='<admin-password>' \
  -e APP_USER=practice -e APP_USER_PASSWORD='<practice-password>' \
  -v oracle-data:/opt/oracle/oradata \
  gvenzl/oracle-free:23

docker logs -f oracle              # wait for "DATABASE IS READY TO USE!", then Ctrl+C
```

`APP_USER` creates the user `practice` (in Oracle a user and its schema share the name: a schema is the set of
tables a user owns). Put its password in `.env` (`ORACLE_PASSWORD=...`), or on macOS keep it in the Keychain:

```bash
security add-generic-password -s oracle-practice -a oracle-practice -w '<practice-password>'
# then in .env:  ORACLE_KEYCHAIN_ITEM=oracle-practice   (and remove ORACLE_PASSWORD)
```

Load the tables (rerun any time to reset them):

```bash
python3 oracle/load_tables.py
```

Later: `docker stop oracle` / `docker start oracle` (with Colima: `colima stop` frees the memory).
The data stays in the `oracle-data` volume. You can also connect with the **Oracle SQL Developer** extension
for VS Code: host `localhost`, port `1521`, service name `FREEPDB1`, user `practice`.

### 5. BigQuery (Google Cloud)

1. Create a project in the [Google Cloud console](https://console.cloud.google.com/) and note its **project ID**.
   New accounts get free trial credit, and BigQuery has a monthly free tier (1 TB of queries, 10 GB of storage).
   This repo uses a few MB in total. Set a **budget alert** anyway (Billing → Budgets & alerts).
2. Log in so Python can use your account:
   ```bash
   gcloud auth application-default login
   ```
   (If you use several Google accounts, make a named configuration with `gcloud config configurations create ...`
   and set `BQ_GCLOUD_CONFIG` in `.env`.)
3. Fill in `.env`: `BQ_PROJECT=<your project id>`, `BQ_DATASET=practice`, `BQ_LOCATION=US` (or `EU`, `europe-west2`...).
   The notebooks write tables as `practice.<table>`, so keep the dataset name `practice`.
4. Create the dataset and load the tables (free load jobs):
   ```bash
   python3 bigquery/load_tables.py
   ```

Without billing enabled (the BigQuery *sandbox*), some features may be limited, e.g. the INSERT/UPDATE/MERGE
questions in topic 10.

---

## How to practice

1. Pick a topic and an engine. Start with **BigQuery or Spark**, then do the same topic in the other engines:
   you learn the idea once and see the syntax differences.
2. Run the setup cell. Read the question, write your answer in the empty cell.
3. Check the line *Expected result: N rows · columns: ...*. Same count and columns? Good sign.
4. Open the solution notebook. Compare your answer, then read **How it works** and **Why / traps**.
5. Before you move on, ask yourself the 4 questions: **ties? NULLs? duplicates? join fan-out?**
6. Theory: answer the quiz questions (topics 20 and 21) out loud, like in an interview.

Tips:
- In BigQuery, each cell prints how much data it read. Use `%%bigquery dry` to check the size for free first.
- Oracle notebook cells take **one** statement each. A `;` at the end is removed for you.
- Topic 10 changes only copies named `prac_*`, never the 7 practice tables. Reload the tables any time with
  `load_tables.py`.
- Files written by Spark practice go to `tmp/` (ignored by git).

## Add or change a question

All notebooks are generated from [`tools/problems/`](tools/problems/): one Python file per topic with questions,
answers per engine, notes and "how it works" text. After you edit one, rebuild:

```bash
python3 tools/build.py            # all topics (runs every answer on every engine)
python3 tools/build.py 05         # only topic 05
python3 tools/build.py --engines spark    # only run Spark; other engines use saved results
```

The build fails if an answer has an error or if the engines return different results.
Do not edit the generated notebooks by hand: the next build overwrites them.

## Troubleshooting

| Problem | Fix |
|---|---|
| `JAVA_GATEWAY_EXITED` / Java not found | install Java 17 and set `JAVA_HOME` |
| Oracle `DPY-6005` / connection refused | is the container running? `docker ps`; wait for "DATABASE IS READY" |
| Oracle `ORA-01017` | wrong user or password in `.env` |
| `ORA-00933` / `ORA-00911` | Oracle syntax: no `AS` before table aliases, no `LIMIT` (use `FETCH FIRST n ROWS ONLY`) |
| BigQuery `BQ_PROJECT is not set` | copy `.env.example` to `.env` and fill it in |
| BigQuery `403` / `Access Denied` | run `gcloud auth application-default login`; check the project ID |
| BigQuery `Not found: Dataset` | run `python3 bigquery/load_tables.py`; check `BQ_LOCATION` |
| `%%sql` / `%%oracle` unknown | run the setup cell first |

## License

- **Code** (the build tool, helpers and loaders): [MIT](LICENSE).
- **Content** (questions, answers, explanations, notebooks, cheatsheets, practice data):
  [CC BY-NC-SA 4.0](LICENSE-CONTENT). Free to use, share and adapt for non-commercial use, with credit.
  Not for resale.

## Credits

Built with [Claude Code](https://claude.com/claude-code).
