# Databricks notebook source
# MAGIC %md
# MAGIC # ▓ SETUP 02 · Replay 14 days of ledger history
# MAGIC
# MAGIC Creates `evidence_processed.ledger` **with Change Data Feed on**, then replays every commit from
# MAGIC `ledger/ops/day_XX.json` as one `MERGE` each. That gives a real Delta history to investigate:
# MAGIC routine work, the night shift, and the insider's edits all look like ordinary commits.
# MAGIC
# MAGIC 💡 **Why CDF must be on *before* the crime:** Change Data Feed only records changes made after
# MAGIC it's enabled. Turning it on after an incident gives you nothing for the past.

# COMMAND ----------

# MAGIC %run ../_setup

# COMMAND ----------

import glob
import json

from pyspark.sql import functions as F

from dfu.spark.case02_tamper import LEDGER_DDL, commit_to_frame, merge_sql

spark.sql(f"DROP TABLE IF EXISTS {LEDGER}")
spark.sql(f"""
  CREATE TABLE {LEDGER} ({LEDGER_DDL})
  COMMENT 'Halcyon Dynamics central ledger (the crime scene)'
  TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

v0 = (spark.read.schema(LEDGER_DDL.replace("TIMESTAMP", "STRING")).json(f"{LANDING}/ledger/ledger_v0.jsonl")
           .withColumn("created_at", F.to_timestamp("created_at"))
           .withColumn("modified_at", F.to_timestamp("modified_at")))
v0.write.mode("append").saveAsTable(LEDGER)
print(f"version 1 loaded: {v0.count()} rows")

# COMMAND ----------

# MAGIC %md ### Replay the commits
# MAGIC Each commit's note is stamped into the Delta log via `commitInfo.userMetadata`, so it shows up in
# MAGIC `DESCRIBE HISTORY`. Notice the insider used the same innocent note as everyone else.

# COMMAND ----------

log = []
for path in sorted(glob.glob(f"{LANDING}/ledger/ops/day_*.json")):
    day = json.load(open(path))
    for c in day["commits"]:
        commit_to_frame(spark, c).createOrReplaceTempView("_commit_src")
        try:
            spark.conf.set("spark.databricks.delta.commitInfo.userMetadata", f"{c['commit_id']} | {c['note']}")
        except Exception:  # noqa: BLE001  (some serverless configs block this; history still works)
            pass
        spark.sql(merge_sql(LEDGER, "_commit_src"))
        version = spark.sql(f"DESCRIBE HISTORY {LEDGER} LIMIT 1").first()["version"]
        log.append((version, day["day"], day["date"], c["commit_id"], c["note"], len(c["ops"])))
try:
    spark.conf.unset("spark.databricks.delta.commitInfo.userMetadata")
except Exception:  # noqa: BLE001
    pass

(spark.createDataFrame(log, "version BIGINT, day INT, date STRING, commit_id STRING, note STRING, ops INT")
      .write.mode("overwrite").saveAsTable(f"{OPS}.commit_log"))
banner("LEDGER HISTORY REPLAYED", [f"{len(log)} commits replayed", f"table {LEDGER} is now at version {log[-1][0]}"])

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {LEDGER}").select("version", "timestamp", "operation", "operationMetrics", "userMetadata"))
