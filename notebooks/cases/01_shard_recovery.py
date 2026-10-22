# Databricks notebook source
# MAGIC %md
# MAGIC # CASE 01 · SHARD RECOVERY — *"Ghost in the Bytes"*
# MAGIC
# MAGIC > A courier was found flatlined in the Csepel Docks. On her deck: a shard holding a copy of
# MAGIC > Halcyon's ledger, stolen **before** anyone touched it. It's the closest thing to an honest
# MAGIC > witness we'll get. The shard is badly damaged.
# MAGIC
# MAGIC **Mission:** recover as much of the shard as possible, record exactly how each record was
# MAGIC recovered, and never silently invent data.
# MAGIC
# MAGIC | You will learn | Where |
# MAGIC |---|---|
# MAGIC | Auto Loader (`cloudFiles`) with an explicit schema + `availableNow` | §1 |
# MAGIC | `_rescued_data`: why drifted / mistyped fields aren't lost | §1, §2 |
# MAGIC | `columnNameOfCorruptRecord`, salvaging truncated records | §2 |
# MAGIC | Reading files whose encoding isn't UTF-8 (`binaryFile` + `decode`) | §3 |
# MAGIC | Rebuilding misaligned CSV rows | §3 |
# MAGIC | Recovery tiers, deduplication with window functions | §4 |
# MAGIC
# MAGIC The damage you'll meet: renamed fields (`amt`), amounts like `"12,500.00 EDD"`, three timestamp
# MAGIC dialects, truncated JSON, pure noise, unquoted commas in CSV, latin-1 bytes, duplicate fragments.

# COMMAND ----------

# MAGIC %run ../_setup

# COMMAND ----------

dbutils.widgets.dropdown("use_autoloader", "true", ["true", "false"], "Use Auto Loader")
USE_AUTOLOADER = dbutils.widgets.get("use_autoloader") == "true"

from pyspark.sql import functions as F

from dfu.spark import case01_recovery as c1

# COMMAND ----------

# MAGIC %md
# MAGIC ## §1 · Ingest the JSON fragments with Auto Loader
# MAGIC
# MAGIC We **declare** the schema we expect instead of inferring it. Anything that doesn't fit (a field
# MAGIC called `amt`, an amount that's a string) lands in **`_rescued_data`** as JSON rather than
# MAGIC becoming a silent `null`. Lines that aren't JSON at all keep their raw text in `_corrupt_record`.
# MAGIC
# MAGIC `trigger(availableNow=True)` processes everything currently in the folder, then stops: a batch
# MAGIC job with streaming's bookkeeping (it'll never re-ingest a fragment it has already seen).

# COMMAND ----------

json_bronze = f"{RAW}.shard_json_bronze"
if USE_AUTOLOADER:
    (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("rescuedDataColumn", "_rescued_data")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("mode", "PERMISSIVE")
        .schema(c1.SHARD_SCHEMA)
        .load(f"{LANDING}/shard/json")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .writeStream
        .option("checkpointLocation", f"{CHECKPOINTS}/shard_json")
        .trigger(availableNow=True)
        .toTable(json_bronze)
        .awaitTermination())
else:
    # Free-Edition / OSS fallback that produces the same columns
    lines = spark.read.text(f"{LANDING}/shard/json").select("value", F.col("_metadata.file_path").alias("_source_file"))
    c1.emulate_autoloader_json(lines).write.mode("overwrite").saveAsTable(json_bronze)

display(spark.table(json_bronze).where("_rescued_data IS NOT NULL OR _corrupt_record IS NOT NULL").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## §2 · Recover the JSON
# MAGIC * **Rescued:** `amt` → `amount` straight out of `_rescued_data`.
# MAGIC * **Repaired:** `"12,500.00 EDD"` → `12500.0`; epoch seconds and `dd/MM/yyyy HH:mm` → timestamps.
# MAGIC * **Salvaged:** truncated lines get a regex pass. A field only counts if its closing delimiter
# MAGIC   survived: `"amount": 360` cut off mid-number might have been `360.47`, so we keep `null` instead.

# COMMAND ----------

json_rec = c1.recover_json(spark.table(json_bronze))
display(json_rec.where("size(repairs) > 0").select("txn_id", "amount", "created_at", "repairs", "_source_file").limit(25))

# COMMAND ----------

# MAGIC %md
# MAGIC ## §3 · The CSV fragments, including the legacy cyberdeck
# MAGIC
# MAGIC Two problems a normal CSV reader hides from you:
# MAGIC 1. **Encoding.** `csv_legacy/` was written in latin-1. Read as UTF-8, `Kőbánya` turns into `K�b�nya`.
# MAGIC    We ingest the raw bytes (`binaryFile`) and `decode()` with the right charset.
# MAGIC 2. **Misaligned rows.** Someone typed commas into `memo` without quoting. A healthy row has 12
# MAGIC    fields; a broken one has more. Since `memo` is the only free-text column, the row can be rebuilt:
# MAGIC    first 8 fields, the last 3, and everything in between is the memo.

# COMMAND ----------

csv_bronze = f"{RAW}.shard_csv_bronze"
if not USE_AUTOLOADER:
    spark.sql(f"DROP TABLE IF EXISTS {csv_bronze}")
for folder, enc in [("csv", "UTF-8"), ("csv_legacy", "ISO-8859-1")]:
    if USE_AUTOLOADER:
        src = (spark.readStream.format("cloudFiles").option("cloudFiles.format", "binaryFile")
                    .load(f"{LANDING}/shard/{folder}"))
        (src.select(F.col("path").alias("_source_file"), "content", F.lit(enc).alias("encoding"))
            .writeStream.option("checkpointLocation", f"{CHECKPOINTS}/shard_{folder}")
            .trigger(availableNow=True).toTable(csv_bronze).awaitTermination())
    else:
        (spark.read.format("binaryFile").load(f"{LANDING}/shard/{folder}")
              .select(F.col("path").alias("_source_file"), "content", F.lit(enc).alias("encoding"))
              .write.mode("append").saveAsTable(csv_bronze))

# decode() wants a literal charset, so decode each encoding separately and union
csv_lines = None
for enc in ["UTF-8", "ISO-8859-1"]:
    part = (spark.table(csv_bronze).where(F.col("encoding") == enc)
                 .select(F.explode(F.split(F.decode("content", enc), r"\r?\n")).alias("value"),
                         "_source_file", "encoding"))
    csv_lines = part if csv_lines is None else csv_lines.unionByName(part)
csv_rec = c1.recover_csv(csv_lines)
display(csv_rec.where("array_contains(repairs, 'realigned_row')").select("txn_id", "memo", "created_at", "modified_by").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## §4 · Tier, deduplicate, report
# MAGIC Every record gets a recovery tier. The courier re-sent some records, so the same `txn_id` can
# MAGIC appear more than once. We keep the **best** copy: lowest tier first, then most complete.

# COMMAND ----------

all_rows = c1.assign_tier(json_rec.unionByName(csv_rec))
all_rows.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{PROC}.shard_all")

recovered = c1.deduplicate(all_rows)
recovered.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{PROC}.shard_recovered")

(all_rows.where("tier = 'LOST'").select("_raw", "_source_file", "source_format")
         .write.mode("overwrite").saveAsTable(f"{QUAR}.shard_lost"))

report = c1.recovery_report(all_rows)
report.write.mode("overwrite").saveAsTable(f"{FIND}.recovery_report")

# COMMAND ----------

tiers = {r["tier"]: r["count"] for r in all_rows.groupBy("tier").count().collect()}
total = sum(tiers.values())
unique = recovered.count()
banner("CASE 01 · SHARD RECOVERED", [
    f"{total} records read from {report.count()} fragments → {unique} unique transactions",
    " · ".join(f"{k} {tiers.get(k, 0)}" for k in ["CLEAN", "RESCUED", "REPAIRED", "SALVAGED", "LOST"]),
    f"recovery rate {100 * (total - tiers.get('LOST', 0)) / total:.1f}%",
    f"→ {PROC}.shard_recovered is now our second witness for Case 02",
], color="#00F0FF")
display(report)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧠 Check yourself
# MAGIC 1. Why declare `amount DOUBLE` instead of `STRING`, when strings would "never fail"?
# MAGIC 2. What would have happened to `amt` without `rescuedDataColumn`?
# MAGIC 3. Why is `availableNow` better than a plain `spark.read` if fragments keep arriving every night?
# MAGIC
# MAGIC <details><summary>Answers</summary>
# MAGIC
# MAGIC 1. A strict type makes bad values *visible* (they get rescued and repaired with a recorded rule) instead of letting them flow downstream as text.
# MAGIC 2. With an explicit schema it would be silently dropped, because the column doesn't exist in the schema.
# MAGIC 3. The checkpoint remembers which files were processed, so reruns are incremental and idempotent.
# MAGIC </details>
