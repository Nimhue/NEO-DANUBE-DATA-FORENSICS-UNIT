# Databricks notebook source
# MAGIC %md
# MAGIC # CASE 02 · THE TAMPER — *"Someone Rewrote History"*
# MAGIC
# MAGIC > The recovered shard doesn't match Halcyon's ledger. Over 14 days somebody edited, deleted and
# MAGIC > backdated transactions, all disguised as "routine reconciliation". Delta Lake never forgets.
# MAGIC
# MAGIC **Mission:** find every malicious change, prove it, and undo *only* that without destroying two
# MAGIC weeks of honest work.
# MAGIC
# MAGIC | You will learn | Where |
# MAGIC |---|---|
# MAGIC | `DESCRIBE HISTORY` and the Delta transaction log | §1 |
# MAGIC | Time travel: `VERSION AS OF`, `TIMESTAMP AS OF` | §2 |
# MAGIC | Change Data Feed: `table_changes()`, pre/post images | §3 |
# MAGIC | Evidence scoring: strong vs weak signals, the false-positive trap | §4 |
# MAGIC | `RESTORE` vs a surgical `MERGE` from CDF pre-images, `SHALLOW CLONE` as a sandbox | §5 |

# COMMAND ----------

# MAGIC %run ../_setup

# COMMAND ----------

from pyspark.sql import functions as F

from dfu.spark import case02_tamper as c2

commit_log = spark.table(f"{OPS}.commit_log")
FIRST_V = commit_log.agg(F.min("version")).first()[0]
LAST_V = commit_log.agg(F.max("version")).first()[0]
print(f"investigation window: versions {FIRST_V} … {LAST_V}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## §1 · Read the transaction log
# MAGIC Every write to a Delta table is an atomic commit in `_delta_log/`. `DESCRIBE HISTORY` shows who did
# MAGIC what, when, and how many rows moved. Look at `operationMetrics`: some "routine" commits update
# MAGIC suspiciously few rows *and* delete some.

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {LEDGER}")
        .select("version", "timestamp", "operation", "userMetadata",
                F.col("operationMetrics.numTargetRowsUpdated").alias("updated"),
                F.col("operationMetrics.numTargetRowsDeleted").alias("deleted"),
                F.col("operationMetrics.numTargetRowsInserted").alias("inserted"))
        .orderBy("version"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## §2 · Time travel: look at the past directly
# MAGIC Pick a transaction and compare how it looked when the ledger was loaded with how it looks now.

# COMMAND ----------

suspect_txn = (spark.table(f"{PROC}.shard_recovered").alias("s")
               .join(spark.table(LEDGER).alias("l"), "txn_id")
               .where("s.counterparty_account != l.counterparty_account").select("txn_id").first())
if suspect_txn:
    tid = suspect_txn[0]
    display(spark.sql(f"""
        SELECT 'then (v{FIRST_V - 1})' AS seen, txn_id, amount, counterparty_account, created_at, modified_by, modified_at
          FROM {LEDGER} VERSION AS OF {FIRST_V - 1} WHERE txn_id = '{tid}'
        UNION ALL
        SELECT 'now', txn_id, amount, counterparty_account, created_at, modified_by, modified_at
          FROM {LEDGER} WHERE txn_id = '{tid}'"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## §3 · Change Data Feed: every change, with before *and* after
# MAGIC `table_changes()` returns `insert`, `update_preimage`, `update_postimage` and `delete` rows, each
# MAGIC stamped with `_commit_version`. We pair each pre-image with its post-image.

# COMMAND ----------

cdf = spark.sql(f"SELECT * FROM table_changes('{LEDGER}', {FIRST_V}, {LAST_V})")
display(cdf.groupBy("_commit_version", "_change_type").count().orderBy("_commit_version", "_change_type"))
paired = c2.pair_changes(cdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## §4 · Score every change
# MAGIC
# MAGIC | Signal | Weight | Why |
# MAGIC |---|---|---|
# MAGIC | `REDIRECT`: counterparty changed | **2** strong | money now goes somewhere else |
# MAGIC | `BACKDATED`: `created_at` changed | **2** strong | creation time should be immutable |
# MAGIC | `EVIDENCE_DELETE`: a `flagged` row deleted | **2** strong | someone erased what audit had spotted |
# MAGIC | `SHARD_CONTRADICTS`: the stolen original disagrees | **2** strong | a second, independent witness (Case 01) |
# MAGIC | `OFF_HOURS`: edited between 00:00 and 05:00 | 1 weak | …but the night shift exists |
# MAGIC | `ROUND_AMOUNT`: changed to a multiple of 500 | 1 weak | …but people do round numbers |
# MAGIC
# MAGIC A change is tampering at **≥ 2 points**. Weak signals alone can never convict. Try lowering the
# MAGIC threshold to 1 and watch the honest night-shift operator get accused.

# COMMAND ----------

flagged = c2.flag_changes(paired, spark.table(f"{PROC}.shard_recovered"))
flagged.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FIND}.tamper_changes")

timeline = (spark.table(f"{FIND}.tamper_changes").where("is_tamper")
            .select("_commit_version", "txn_id", "change", "flags", "suspicion",
                    "pre_amount", "post_amount", "pre_counterparty_account", "post_counterparty_account",
                    "pre_created_at", "post_created_at", "post_modified_by", "post_modified_at")
            .orderBy("_commit_version", "txn_id"))
timeline.write.mode("overwrite").saveAsTable(f"{FIND}.tamper_timeline")
display(timeline)

# COMMAND ----------

commits = c2.attribute_commits(spark.table(f"{FIND}.tamper_changes")).join(
    commit_log.withColumnRenamed("version", "_commit_version"), "_commit_version", "left")
commits.write.mode("overwrite").saveAsTable(f"{FIND}.commit_audit")
display(commits.where("malicious = 1"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### The false-positive trap
# MAGIC How many changes carry exactly one weak flag? Those are the people you would have accused with a
# MAGIC naive "anything after midnight is suspicious" rule.

# COMMAND ----------

display(spark.table(f"{FIND}.tamper_changes")
        .where("suspicion = 1").groupBy("post_modified_by").agg(F.count("*").alias("innocent_off_hours_edits")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## §5 · Undo the damage
# MAGIC
# MAGIC **Option A: `RESTORE TABLE … TO VERSION AS OF`.** A sledgehammer: it also wipes every honest
# MAGIC insert and settlement since then. We try it on a **shallow clone** (metadata only, no data
# MAGIC copied) so the real ledger is safe.

# COMMAND ----------

sandbox = f"{PROC}.ledger_restore_sandbox"
spark.sql(f"CREATE OR REPLACE TABLE {sandbox} SHALLOW CLONE {LEDGER}")
before = spark.table(sandbox).count()
spark.sql(f"RESTORE TABLE {sandbox} TO VERSION AS OF {FIRST_V - 1}")
after = spark.table(sandbox).count()
honest_inserts = cdf.where("_change_type = 'insert'").count()
honest_settles = spark.table(f"{FIND}.tamper_changes").where("NOT is_tamper AND change = 'update'").count()
banner("OPTION A · RESTORE (on a clone)", [
    f"rows {before} → {after}",
    f"collateral damage: {honest_inserts} honest new transactions and {honest_settles} honest updates erased",
], color="#FF2A6D")
spark.sql(f"DROP TABLE {sandbox}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Option B: surgical restore.** For each tampered transaction take the CDF *pre-image* from just
# MAGIC before its first malicious change, and `MERGE` it back. Deleted evidence gets re-inserted. Every
# MAGIC honest change stays. The restore is itself a new, auditable commit.

# COMMAND ----------

tampered_keys = spark.table(f"{FIND}.tamper_changes").where("is_tamper").select("txn_id", "_commit_version")
restore_src = c2.surgical_restore_source(cdf, tampered_keys)
restore_src.createOrReplaceTempView("_restore_src")
display(restore_src.groupBy("_restore_action").count())

try:
    spark.conf.set("spark.databricks.delta.commitInfo.userMetadata", "DFU surgical restore · case 02")
except Exception:  # noqa: BLE001
    pass
spark.sql(c2.restore_merge_sql(LEDGER, "_restore_src"))
try:
    spark.conf.unset("spark.databricks.delta.commitInfo.userMetadata")
except Exception:  # noqa: BLE001
    pass

n_t = tampered_keys.select("txn_id").distinct().count()
n_c = commits.where("malicious = 1").count()
ops = sorted({o for r in commits.where("malicious = 1").collect() for o in (r["signed_by"] or [])})
banner("CASE 02 · HISTORY RESTORED", [
    f"{n_t} tampered transactions across {n_c} malicious commits",
    f"edits signed by operator logins: {', '.join(ops)}",
    "ledger surgically restored, honest work untouched",
    "→ who owns those logins? Case 03.",
], color="#FF2A6D")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧠 Check yourself
# MAGIC 1. Why can't `DESCRIBE HISTORY` alone tell us *which rows* changed?
# MAGIC 2. A delete row in CDF shows `modified_by = OP-114`. Is OP-114 the deleter?
# MAGIC 3. `VACUUM` ran with the default retention last week. What does that mean for time travel here?
# MAGIC
# MAGIC <details><summary>Answers</summary>
# MAGIC
# MAGIC 1. History is commit-level metadata (counts, operation, user); row-level before/after values come from CDF or from diffing versions.
# MAGIC 2. No. A delete row is the *victim's* last state, so `modified_by` is whoever edited it before. Attribute deletes at commit level instead.
# MAGIC 3. Data files older than the retention window (7 days by default) may be gone, so time travel and CDF beyond it can fail. Forensics needs retention planned ahead.
# MAGIC </details>
