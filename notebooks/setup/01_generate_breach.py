# Databricks notebook source
# MAGIC %md
# MAGIC # ▓ SETUP 01 · Generate the breach
# MAGIC
# MAGIC > *Neo-Danube, March 2077. Halcyon Dynamics' central ledger has been hit from the inside.*
# MAGIC
# MAGIC This notebook builds the crime scene. It creates the Unity Catalog layout and runs the seeded
# MAGIC scenario generator (`src/dfu/generator`) straight into a **UC volume**. Change the `seed` or
# MAGIC `difficulty` widget and you get a brand-new, still-solvable case.
# MAGIC
# MAGIC | Schema | Role |
# MAGIC |---|---|
# MAGIC | `evidence_raw` | bronze: landing volume + raw ingested files |
# MAGIC | `evidence_processed` | silver: recovered, normalized, the live ledger |
# MAGIC | `case_findings` | gold: what you'd show the chief |
# MAGIC | `quarantine` | evidence that failed integrity scans |
# MAGIC | `ops` | run logs, commit log, scoreboard |

# COMMAND ----------

# MAGIC %run ../_setup

# COMMAND ----------

# MAGIC %md ## 1 · Unity Catalog layout
# MAGIC If you can't create catalogs (e.g. Free Edition), set the `catalog` widget to one you own, such as `workspace`.

# COMMAND ----------

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG} COMMENT 'Neo-Danube Data Forensics Unit'")
except Exception as e:  # noqa: BLE001
    print(f"Could not create catalog {CATALOG} ({type(e).__name__}); assuming it already exists.")

for schema, comment in [
    ("evidence_raw", "Bronze: raw evidence exactly as recovered"),
    ("evidence_processed", "Silver: cleaned, recovered and validated evidence"),
    ("case_findings", "Gold: investigation results"),
    ("quarantine", "Evidence that failed integrity scans"),
    ("ops", "Unit operations: logs, commit log, scoreboard"),
]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema} COMMENT '{comment}'")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {RAW}.landing COMMENT 'Drop zone for recovered files'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {RAW}.checkpoints COMMENT 'Auto Loader checkpoints & schemas'")

# COMMAND ----------

# MAGIC %md ## 2 · Generate the scenario into the volume

# COMMAND ----------

from dfu.generator import generate

dbutils.fs.rm(LANDING, recurse=True)          # fresh crime scene every run
dbutils.fs.rm(CHECKPOINTS, recurse=True)
for t in ["shard_json_bronze", "shard_csv_bronze"]:   # bronze tables belong to the old scene
    spark.sql(f"DROP TABLE IF EXISTS {RAW}.{t}")
result = generate(LANDING, seed=SEED, difficulty=DIFFICULTY)
m = result["manifest"]

banner("BREACH SCENARIO GENERATED", [
    f"seed {m['seed']} · difficulty {m['difficulty'].upper()}",
    f"{m['identity_records']} identity records from 4 source systems",
    f"{m['ledger_v0_rows']} ledger rows + {m['commits']} commits over {m['days']} days",
    f"{m['shard_records']} stolen records in {m['shard_files']} damaged fragments",
    "answer key sealed in answer_key/sealed.txt (no peeking)",
])

# COMMAND ----------

display(dbutils.fs.ls(LANDING))
