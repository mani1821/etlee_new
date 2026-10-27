# Databricks notebook source
# MAGIC %md
# MAGIC # ICD / DPI validation: run 1 or 2 YAMLs, one summary e-mail, detailed report below
# MAGIC 1. Fill in one or both YAML widgets (full load and / or incremental). An empty widget is skipped.
# MAGIC 2. Run all. Each YAML is validated, then ONE e-mail summarises the run(s).
# MAGIC 3. The detailed report (every validation, check counts, row counts, failures) is shown at the end of this
# MAGIC    notebook. Run as a workflow, the link in the e-mail opens this run, so readers land on that report.
# MAGIC
# MAGIC E-mail settings (to, subject, smtp_server, ...) come from `settings.email` of the first YAML that has them;
# MAGIC the `email_to` widget overrides the recipients.

# COMMAND ----------

import json
import os
import sys

dbutils.widgets.text("full_yaml", "", "1. Full-load YAML (path, empty = skip)")
dbutils.widgets.text("delta_yaml", "", "2. Incremental YAML (path, empty = skip)")
dbutils.widgets.dropdown("env", "databricks", ["databricks", "oracle"], "Environment")
dbutils.widgets.text("full_params", "{}", "Params for the full-load YAML (JSON, ${NAME} placeholders)")
dbutils.widgets.text("delta_params", "{}", "Params for the incremental YAML (JSON, ${NAME} placeholders)")
dbutils.widgets.text("tables", "", "Only these tables (comma-separated, empty = all)")
dbutils.widgets.text("parallel_tables", "", "Tables at the same time (empty = YAML setting)")
dbutils.widgets.dropdown("send_email", "true", ["true", "false"], "Send the summary e-mail")
dbutils.widgets.text("email_to", "", "E-mail to (empty = settings.email.to)")

# the package: installed wheel, or the folder above notebooks/
try:
    import etl_parity
except ImportError:
    sys.path.insert(0, os.path.dirname(os.getcwd()))
    import etl_parity
from etl_parity import run_parity, send_report

print("etl_parity", etl_parity.__version__, "from", os.path.dirname(etl_parity.__file__))

# COMMAND ----------

def _path(p: str) -> str:
    p = p.strip()
    return p if not p or p.startswith("/") else os.path.normpath(os.path.join(os.getcwd(), p))

yamls = [(label, _path(dbutils.widgets.get(w)), dbutils.widgets.get(pw))
         for label, w, pw in (("full load", "full_yaml", "full_params"), ("incremental", "delta_yaml", "delta_params"))]
yamls = [(label, path, params) for label, path, params in yamls if path]
if not yamls:
    raise ValueError("Fill in at least one YAML widget (full_yaml and / or delta_yaml).")
for label, path, _ in yamls:
    if not os.path.exists(path):
        raise FileNotFoundError(f"{label} YAML not found: {path}")

tables = [t.strip() for t in dbutils.widgets.get("tables").split(",") if t.strip()] or None
parallel = int(dbutils.widgets.get("parallel_tables")) if dbutils.widgets.get("parallel_tables").strip() else None
print("YAMLs:", ", ".join(f"{label}: {path}" for label, path, _ in yamls))

# COMMAND ----------

# MAGIC %md ## 1. Validations (one run per YAML; no e-mail per run)

# COMMAND ----------

runs = []
for label, path, params in yamls:
    print(f"\n==================== {label}: {os.path.basename(path)} ====================")
    try:
        runs.append(run_parity(path, env=dbutils.widgets.get("env"), tables=tables,
                               params=json.loads(params or "{}"), parallel_tables=parallel, send_email=False))
    except Exception as e:                       # e.g. a YAML error: report the other YAML anyway
        print(f"{label} NOT validated: {type(e).__name__}: {e}")

if not runs:
    raise RuntimeError("No YAML could be validated (see the errors above).")
print("\nrun ids:", ", ".join(r.run_id for r in runs))

# COMMAND ----------

# MAGIC %md ## 2. One summary e-mail for the run(s)

# COMMAND ----------

# e-mail settings: the first YAML whose settings have an email block (else the first YAML + email_to widget)
from etl_parity.config import load_config
config = next((p for _, p, _ in yamls if load_config(p).settings.email), yamls[0][1])
email_to = dbutils.widgets.get("email_to").strip() or None

report = send_report(runs, config=config, email_to=email_to,
                     send_on=None if dbutils.widgets.get("send_email") == "true" else "never")
print("subject:", report.subject)
if dbutils.widgets.get("send_email") != "true":
    print("e-mail: off (send_email = false)")
else:
    print("e-mail sent" if report.email_sent else
          f"e-mail NOT sent: {report.email_error or 'no recipients (settings.email.to / email_to) or send_on'}")

# in a job: publish the run ids for later tasks (ignored outside jobs)
try:
    dbutils.jobs.taskValues.set(key="run_ids", value=",".join(report.run_ids))
except Exception:
    pass

# COMMAND ----------

# MAGIC %md ## 3. Detailed report (the e-mail link opens this page)

# COMMAND ----------

displayHTML(report.report_html)

# COMMAND ----------

# MAGIC %md ## 4. Raw results (every check that did not pass)

# COMMAND ----------

results_table = load_config(config).settings.results_table
ids = ", ".join(f"'{i}'" for i in report.run_ids)
display(spark.sql(f"""
    SELECT run_id, table_name, validation, check_name, status, src_value, tgt_value, detail
    FROM {results_table}
    WHERE run_id IN ({ids}) AND status NOT IN ('PASS', 'INFO') AND table_name <> '__run__'
    ORDER BY run_id, table_name, validation, check_name"""))
