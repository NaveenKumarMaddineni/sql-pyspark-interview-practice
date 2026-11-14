"""
build.py - builds the practice and solution notebooks from tools/problems/*.py.

For every question it:
  1. runs the solution on each engine (BigQuery, Oracle, Spark SQL, Spark DataFrame API),
  2. checks that all engines give the SAME answer,
  3. writes  <engine>/practice/<topic>.ipynb   (questions + empty cells)
     and     solutions/<engine>/<topic>.ipynb  (questions + answers + results).

Run from the repo root:
    python3 tools/build.py                   # all topics, all engines
    python3 tools/build.py 05 08             # only topics whose id starts with 05 or 08
    python3 tools/build.py --engines spark   # only some engines (others keep cached results)
    python3 tools/build.py --cache-only      # do not run queries, use saved results

Results are cached in tools/.cache (so a rebuild does not re-run unchanged queries).
"""
import argparse
import datetime as dt
import decimal
import hashlib
import html
import importlib.util
import json
import math
import pickle
import re
import sys
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
PROBLEMS = Path(__file__).resolve().parent / "problems"
CACHE = Path(__file__).resolve().parent / ".cache"

TABLES = ["departments", "employees", "customers", "orders",
          "products", "order_items", "user_logins"]
ENGINES = ["bigquery", "oracle", "spark"]
NAMES = {"bigquery": "BigQuery", "oracle": "Oracle", "spark": "Spark"}
MAGIC = {"bigquery": "%%bigquery", "oracle": "%%oracle", "spark": "%%sql"}


# ----------------------------------------------------------------- problems
def Q(title, text, level="medium", sql=None, bq=None, ora=None, ssql=None, sdf=None,
      note="", notes=None, setup=None, compare=True, ordered=False, hint=None, answer=None, df_first=False):
    """One question.
    sql   : one SQL answer that works in all 3 engines (BigQuery gets `practice.` added)
    bq / ora / ssql : engine-specific SQL (a string, or a list = several cells).
                      "SKIP" = this engine does not get the question.
    sdf   : PySpark DataFrame API answer. Must put the answer in a variable `res`.
    note  : explanation shown under every solution.  notes={"oracle": "..."} per engine.
    setup : {"bigquery": [sql, ...], ...} cells that run before (shown in both notebooks).
    compare: check that engines agree.  ordered: compare row order too.
    answer: a written answer (theory question, no code cells).
    df_first: put the DataFrame cell before the SQL cell (when the SQL needs it).
    """
    return dict(title=title, text=text, level=level, sql=sql, bq=bq, ora=ora, ssql=ssql,
                sdf=sdf, note=note, notes=notes or {}, setup=setup or {},
                compare=compare, ordered=ordered, hint=hint, answer=answer, df_first=df_first)


def load_topics(filters):
    topics = []
    for f in sorted(PROBLEMS.glob("[0-9]*.py")):
        if filters and not any(f.stem.startswith(x) for x in filters):
            continue
        spec = importlib.util.spec_from_file_location(f.stem, f)
        mod = importlib.util.module_from_spec(spec)
        mod.Q = Q
        spec.loader.exec_module(mod)
        topic = mod.TOPIC
        topic["id"] = f.stem
        how = getattr(mod, "HOW", [])          # "How it works" text, one per question
        if how and len(how) != len(topic["questions"]):
            raise SystemExit(f"{f.name}: HOW has {len(how)} items, but there are "
                             f"{len(topic['questions'])} questions")
        for q, h in zip(topic["questions"], how):
            q["how"] = h
        topic.setdefault("engines", ENGINES)
        topics.append(topic)
    return topics


def add_dataset(sql):
    """Generic SQL -> BigQuery SQL: FROM employees -> FROM practice.employees."""
    pattern = r"\b(FROM|JOIN|INTO|UPDATE|TABLE)\s+(" + "|".join(TABLES) + r")\b"
    return re.sub(pattern, r"\1 practice.\2", sql, flags=re.IGNORECASE)


def as_list(x):
    if x is None:
        return []
    return list(x) if isinstance(x, (list, tuple)) else [x]


def sql_cells(q, engine):
    """The SQL answer cells for one engine (list of strings), or None if skipped."""
    own = {"bigquery": q["bq"], "oracle": q["ora"], "spark": q["ssql"]}[engine]
    if own == "SKIP":
        return None
    if own is not None:
        return [s.strip() for s in as_list(own)]
    if q["sql"] is not None:
        cells = [s.strip() for s in as_list(q["sql"])]
        return [add_dataset(s) for s in cells] if engine == "bigquery" else cells
    return None


# ----------------------------------------------------------------- running
class Runner:
    def __init__(self, engines, cache_only):
        self.engines, self.cache_only = engines, cache_only
        CACHE.mkdir(exist_ok=True)
        self._spark = self._tables = None
        self.nocache = False      # topics that change tables must always run in order

    def spark(self):
        if self._spark is None:
            sys.path.insert(0, str(ROOT / "spark"))
            from spark_setup import get_spark, load_tables
            self._spark = get_spark("build")
            self._tables = load_tables(self._spark)
        return self._spark

    def spark_ns(self):
        from pyspark.sql import functions as F, Window, types as T
        spark = self.spark()
        ns = dict(spark=spark, F=F, Window=Window, T=T, t=self._tables, ROOT=ROOT)
        ns.update(self._tables)
        return ns

    def run(self, engine, code, kind="sql"):
        """Run one cell. Returns ('ok', value) or ('error', message). Uses the cache."""
        key = hashlib.sha1(f"{engine}|{kind}|{code}".encode()).hexdigest()
        path = CACHE / f"{key}.pkl"
        if path.exists() and not self.nocache:
            return pickle.loads(path.read_bytes())
        if self.cache_only or engine not in self.engines:
            return ("missing", None)
        try:
            out = ("ok", self._run(engine, code, kind))
        except Exception as e:  # keep going, report later
            out = ("error", f"{type(e).__name__}: {e}"[:1500])
        if out[0] == "ok" and not self.nocache:
            path.write_bytes(pickle.dumps(out))
        return out

    def _run(self, engine, code, kind):
        if engine == "bigquery":
            sys.path.insert(0, str(ROOT / "bigquery"))
            from bq_helper import run_bq
            if code.startswith("%%bigquery dry"):
                import contextlib
                import io
                buf = io.StringIO()
                with contextlib.redirect_stdout(buf):
                    run_bq(code.split("\n", 1)[1], dry_run=True)
                return {"stdout": buf.getvalue()}
            return run_bq(code, quiet=True)
        if engine == "oracle":
            sys.path.insert(0, str(ROOT / "oracle"))
            from oracle_helper import run_oracle
            return run_oracle(code)
        if kind == "sql":
            return _spark_out(self.spark().sql(code))["pandas"]   # %%sql shows a pandas table
        ns = self.spark_ns()
        import contextlib
        import io
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            exec(compile(code, "<solution>", "exec"), ns)
        res = ns.get("res")
        if res is None:
            return {"stdout": buf.getvalue()}
        if hasattr(res, "toPandas"):
            out = _spark_out(res)
            if "res.show(" in code:          # the code prints it already: keep data for checks only
                return {"stdout": buf.getvalue(), "pandas_hidden": out["pandas"]}
            out["stdout"] = buf.getvalue()
            return out
        return {"stdout": buf.getvalue(), "value": repr(res)}


def _spark_out(df):
    if not df.columns:
        return {"pandas": None, "show": ""}
    try:
        show = df._show_string(20, 20, False)
    except Exception:
        show = df.limit(20).toPandas().to_string()
    return {"pandas": df.limit(1000).toPandas(), "show": show}


def to_pandas(value):
    import pandas as pd
    if isinstance(value, pd.DataFrame):
        return value
    if isinstance(value, dict):
        for key in ("pandas", "pandas_hidden"):
            if value.get(key) is not None:
                return value[key]
    return None


def norm_value(v):
    if v is None:
        return None
    try:
        if v != v:            # NaN / NaT
            return None
    except Exception:
        pass
    if type(v).__name__ in ("NAType", "NaTType"):
        return None
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, (int, float, decimal.Decimal)) or type(v).__module__ == "numpy":
        try:
            f = float(v)
            return None if math.isnan(f) else round(f, 2)
        except Exception:
            return str(v)
    if isinstance(v, dt.datetime) or type(v).__name__ == "Timestamp":
        if (v.hour, v.minute, v.second) == (0, 0, 0):
            return v.strftime("%Y-%m-%d")
        return v.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, dt.date):
        return v.isoformat()
    if isinstance(v, (list, tuple)) or type(v).__name__ == "ndarray":
        return tuple(norm_value(x) for x in v)
    return str(v)


def fingerprint(df, ordered):
    rows = [tuple(norm_value(v) for v in r) for r in df.itertuples(index=False)]
    if not ordered:
        rows = sorted(rows, key=lambda r: tuple((x is None, str(x)) for x in r))
    return [c.lower() for c in df.columns], rows


# ----------------------------------------------------------------- notebooks
def md(text):
    return {"cell_type": "markdown", "metadata": {}, "source": _lines(text)}


def code(text, outputs=None):
    return {"cell_type": "code", "execution_count": None, "metadata": {},
            "outputs": outputs or [], "source": _lines(text)}


def _lines(text):
    lines = text.strip("\n").split("\n")
    return [l + "\n" for l in lines[:-1]] + [lines[-1]]


def notebook(cells):
    return {"cells": cells, "nbformat": 4, "nbformat_minor": 5,
            "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python",
                                        "name": "python3"},
                         "language_info": {"name": "python"}}}


def result_outputs(status, value):
    import pandas as pd
    if status != "ok":
        return []
    outs = []
    if isinstance(value, dict):
        if value.get("stdout"):
            outs.append({"output_type": "stream", "name": "stdout", "text": _lines(value["stdout"])})
        if value.get("show"):
            outs.append({"output_type": "stream", "name": "stdout", "text": _lines(value["show"])})
        elif value.get("value"):
            outs.append({"output_type": "execute_result", "execution_count": None, "metadata": {},
                         "data": {"text/plain": _lines(value["value"])}})
        if "show" in value or "pandas" not in value:
            return outs
        value = value.get("pandas")
    if isinstance(value, pd.DataFrame):
        shown = value.head(25)
        more = f"\n... {len(value)} rows in total" if len(value) > 25 else ""
        outs.append({"output_type": "execute_result", "execution_count": None, "metadata": {},
                     "data": {"text/plain": _lines(shown.to_string() + more),
                              "text/html": _lines(shown.to_html(max_rows=25, na_rep="NULL")
                                                  + html.escape(more))}})
    elif isinstance(value, str):
        outs.append({"output_type": "execute_result", "execution_count": None, "metadata": {},
                     "data": {"text/plain": [value]}})
    return outs


def setup_cell(engine):
    find_root = ("import sys, pathlib\n"
                 "ROOT = next(p for p in [pathlib.Path.cwd(), *pathlib.Path.cwd().parents]\n"
                 "            if (p / \"settings.py\").exists())   # the repo folder\n")
    if engine == "bigquery":
        return find_root + 'sys.path.insert(0, str(ROOT / "bigquery"))\n%load_ext bq_helper'
    if engine == "oracle":
        return find_root + 'sys.path.insert(0, str(ROOT / "oracle"))\n%load_ext oracle_helper'
    return (find_root + 'sys.path.insert(0, str(ROOT / "spark"))\n'
            "from spark_setup import get_spark, load_tables\n"
            "from pyspark.sql import functions as F, Window, types as T\n\n"
            "spark = get_spark()\n"
            "t = load_tables(spark)          # also makes SQL views with the same names\n"
            'employees, departments = t["employees"], t["departments"]\n'
            'customers, orders = t["customers"], t["orders"]\n'
            'products, order_items = t["products"], t["order_items"]\n'
            'user_logins = t["user_logins"]\n'
            "%load_ext spark_setup")


HOW_TO = {
    "bigquery": "Start each SQL cell with `%%bigquery`. Write tables as `practice.table_name`.\n"
                "Each cell prints how much data it read. `%%bigquery dry` checks the size for free.",
    "oracle": "Start each SQL cell with `%%oracle`. One statement per cell. "
              "Tables are in your own schema, so just write `employees`.",
    "spark": "Start a cell with `%%sql` to write Spark SQL. In a normal Python cell use the "
             "DataFrame API: the tables are ready as variables (`employees`, `orders`, ...). "
             "Put your answer in `res` and call `res.show()`.",
}

LEVEL = {"easy": "🟢 easy", "medium": "🟡 medium", "hard": "🔴 hard"}


def with_magic(engine, c):
    return c if c.startswith("%%") else MAGIC[engine] + "\n" + c


def engine_cells(q, engine):
    """[(kind, code)] the answer cells for this engine. kind = sql | df."""
    cells = [("sql", c) for c in (sql_cells(q, engine) or [])]
    if engine == "spark" and q["sdf"]:
        df_cell = ("df", q["sdf"].strip())
        cells = [df_cell] + cells if q["df_first"] else cells + [df_cell]
    return cells


def question_md(i, q, engine, expect):
    head = f"## Q{i}. {q['title']}  ·  {LEVEL[q['level']]}\n\n{q['text'].strip()}"
    if q["hint"]:
        head += f"\n\n<details><summary>Hint</summary>\n\n{q['hint'].strip()}\n\n</details>"
    if expect:
        head += f"\n\n*Expected result: {expect}*"
    return head


def expect_text(results):
    for status, value in results:
        df = to_pandas(value) if status == "ok" else None
        if df is not None:
            return f"{len(df)} row{'s' if len(df) != 1 else ''} · columns: " \
                   + ", ".join(f"`{c.lower()}`" for c in df.columns)
    return ""


def build(topics, runner):
    problems = []
    for topic in topics:
        nb = {e: {"practice": [], "solution": []} for e in topic["engines"]}
        runner.nocache = topic.get("nocache", False)
        for e in topic["engines"]:
            head = (f"# {topic['title']} ({NAMES[e]})\n\n{topic['intro'].strip()}\n\n"
                    f"**How to run:** {HOW_TO[e]}\n\n"
                    "Tables: see [tables_guide.md](../../cheatsheets/tables_guide.md). "
                    "Run the setup cell first (Shift+Enter).")
            sol_head = head.replace("../../cheatsheets", "../../cheatsheets") + \
                "\n\n> **Solutions.** Try the practice notebook first: " \
                f"[`{e}/practice/{topic['id']}.ipynb`](../../{e}/practice/{topic['id']}.ipynb)"
            if all(q["answer"] for q in topic["questions"]):     # theory quiz: no code
                head = f"# {topic['title']}\n\n{topic['intro'].strip()}"
                nb[e]["practice"].append(md(head))
                nb[e]["solution"].append(md(head + "\n\n> **Answers.** Try the practice notebook first."))
                continue
            nb[e]["practice"] += [md(head), code(setup_cell(e))]
            nb[e]["solution"] += [md(sol_head), code(setup_cell(e))]
            if topic.get("setup", {}).get(e):
                for s in as_list(topic["setup"][e]):
                    kind = "df" if e == "spark" and not s.lstrip().startswith("%%sql") else "sql"
                    body = s.strip().removeprefix("%%sql").strip()
                    st, val = runner.run(e, body, kind)
                    if st == "error":
                        problems.append(f"{topic['id']} setup {e}: {val}")
                    cell = (MAGIC[e] + "\n" + body) if kind == "sql" else body
                    nb[e]["practice"] += [md("Setup for this notebook (run once):"), code(cell)]
                    nb[e]["solution"] += [md("Setup for this notebook (run once):"),
                                          code(cell, result_outputs(st, val))]

        for i, q in enumerate(topic["questions"], 1):
            if q["answer"]:                      # theory question: text only
                for e in topic["engines"]:
                    text = question_md(i, q, e, "")
                    nb[e]["practice"] += [md(text), md("**Your answer:** *(double-click to write)*")]
                    nb[e]["solution"] += [md(text), md("**Answer:**\n\n" + q["answer"].strip())]
                continue
            per_engine, compare_set = {}, []
            for e in topic["engines"]:
                cells = engine_cells(q, e)
                if not cells:
                    continue
                setup = [(("sql" if not (e == "spark" and not s.lstrip().startswith("%%sql")) else "df"),
                          s.strip().removeprefix("%%sql").strip()) for s in as_list(q["setup"].get(e))]
                ran_setup = [(k, c, runner.run(e, c, k)) for k, c in setup]
                ran = [(k, c, runner.run(e, c, k)) for k, c in cells]
                per_engine[e] = (ran_setup, ran)
                for k, c, (st, val) in ran_setup + ran:
                    if st == "error":
                        problems.append(f"{topic['id']} Q{i} {e}/{k}: {val}\n    {c[:300]}")
                    elif st == "missing":
                        problems.append(f"{topic['id']} Q{i} {e}/{k}: not run (cache only)")
                if q["compare"]:
                    sql_last = [r for r in ran if r[0] == "sql"]
                    df_last = [r for r in ran if r[0] == "df"]
                    for label, group in (("sql", sql_last), ("df", df_last)):
                        if group and group[-1][2][0] == "ok":
                            pdf = to_pandas(group[-1][2][1])
                            if pdf is not None:
                                compare_set.append((f"{e}/{label}", pdf))
            if q["compare"] and len(compare_set) > 1:
                base_name, base = compare_set[0]
                fb = fingerprint(base, q["ordered"])
                for name, other in compare_set[1:]:
                    fo = fingerprint(other, q["ordered"])
                    if fb[1] != fo[1] or len(fb[0]) != len(fo[0]):
                        diff = _diff(fb, fo)
                        problems.append(f"{topic['id']} Q{i} '{q['title']}': {base_name} != {name}\n{diff}")
            all_results = [r[2] for e in per_engine for r in per_engine[e][1]]
            expect = expect_text([r for r in all_results if r[0] == "ok"][-1:]) if q["compare"] else ""
            for e, (ran_setup, ran) in per_engine.items():
                text = question_md(i, q, e, expect)
                nb[e]["practice"].append(md(text))
                nb[e]["solution"].append(md(text))
                if q.get("how"):
                    nb[e]["solution"].append(md("**How it works:** " + q["how"].strip()))
                for k, c, (st, val) in ran_setup:
                    cell = with_magic(e, c) if k == "sql" else c
                    nb[e]["practice"].append(code(cell))
                    nb[e]["solution"].append(code(cell, result_outputs(st, val)))
                kinds = {k for k, _, _ in ran}
                if "sql" in kinds and not q["df_first"]:
                    nb[e]["practice"].append(code(f"{MAGIC[e]}\n-- your answer here"))
                if "df" in kinds:
                    nb[e]["practice"].append(code("# DataFrame API: put your answer in res\n"
                                                  "res = ...\nres.show()" if q["sdf"] and "res" in q["sdf"]
                                                  else "# your answer here (DataFrame API)"))
                if "sql" in kinds and q["df_first"]:
                    nb[e]["practice"].append(code(f"{MAGIC[e]}\n-- your answer here"))
                for k, c, (st, val) in ran:
                    if k == "df" and "sql" in kinds and not q["df_first"]:
                        nb[e]["solution"].append(md("**DataFrame API:**"))
                    if k == "sql" and "df" in kinds and q["df_first"]:
                        nb[e]["solution"].append(md("**Spark SQL:**"))
                    cell = with_magic(e, c) if k == "sql" else (
                        c + ("\nres.show()" if re.search(r"^res\s*=", c, re.M) and "res.show(" not in c
                             and hasattr(val, "get") and isinstance(val, dict) and "show" in val else ""))
                    nb[e]["solution"].append(code(cell, result_outputs(st, val)))
                note = "\n\n".join(x for x in [q["note"].strip(), q["notes"].get(e, "").strip()] if x)
                if note:
                    nb[e]["solution"].append(md("**Why / traps:** " + note))
        for e in topic["engines"]:
            for kind, folder in (("practice", ROOT / e / "practice"),
                                 ("solution", ROOT / "solutions" / e)):
                folder.mkdir(parents=True, exist_ok=True)
                text = json.dumps(notebook(nb[e][kind]), indent=1, ensure_ascii=False) + "\n"
                # hide local paths (user name) in the saved outputs
                text = text.replace(str(ROOT), "<repo>").replace(str(Path.home()), "~")
                (folder / f"{topic['id']}.ipynb").write_text(text)
        print(f"built {topic['id']:28} {len(topic['questions']):>3} questions  "
              f"({', '.join(topic['engines'])})")
    return problems


def _diff(a, b):
    out = [f"    columns: {a[0]}  vs  {b[0]}", f"    rows: {len(a[1])} vs {len(b[1])}"]
    sa, sb = set(a[1]), set(b[1])
    for r in list(sa - sb)[:4]:
        out.append(f"    only left : {r}")
    for r in list(sb - sa)[:4]:
        out.append(f"    only right: {r}")
    if sa == sb:
        out.append("    (same rows, different order or duplicates)")
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("topics", nargs="*")
    ap.add_argument("--engines", nargs="*", default=ENGINES)
    ap.add_argument("--cache-only", action="store_true")
    args = ap.parse_args()
    topics = load_topics(args.topics)
    runner = Runner(args.engines, args.cache_only)
    try:
        problems = build(topics, runner)
    except Exception:
        traceback.print_exc()
        sys.exit(2)
    total = sum(len(t["questions"]) for t in topics)
    print(f"\n{len(topics)} topics, {total} questions.")
    if problems:
        print(f"\n{len(problems)} PROBLEM(S):")
        for p in problems:
            print(" -", p)
        sys.exit(1)
    print("All solutions ran and all engines agree.")


if __name__ == "__main__":
    main()
