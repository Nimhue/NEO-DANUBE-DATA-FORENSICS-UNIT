# Databricks notebook source
# MAGIC %md
# MAGIC # ■ CASE CLOSED · verify against the sealed answer key
# MAGIC The generator sealed the truth before the investigation started. This notebook opens it and
# MAGIC grades the unit's work. It is also the last task in the job, so **a wrong answer fails the run**.

# COMMAND ----------

# MAGIC %run ../_setup

# COMMAND ----------

from pyspark.sql import functions as F

from dfu.generator import unseal

key = unseal(f"{LANDING}/answer_key/sealed.txt")

caught = {r[0] for r in spark.table(f"{FIND}.tamper_changes").where("is_tamper").select("txn_id").distinct().collect()}
truth = {t["txn_id"] for t in key["tampered"]}
sus = spark.table(f"{FIND}.suspects").collect()
prime = [r for r in sus if r["role"] == "PRIME SUSPECT"]
golden = spark.table(f"{FIND}.golden_identities").collect()
culprit_ids = set(key["culprit"]["record_ids"])
report = spark.table(f"{FIND}.recovery_report").agg(F.sum("records").alias("n"), F.sum("lost").alias("lost")).first()

checks = {
    "Recovered ≥ 97% of the shard": (report["n"] - report["lost"]) / report["n"] >= 0.97,
    "Found every tampered transaction": truth <= caught,
    "Zero false accusations": caught <= truth,
    "Unmasked the culprit's full identity": any(set(g["record_ids"]) == culprit_ids for g in golden),
    "Named the right prime suspect": len(prime) == 1 and set(prime[0]["logins_used"]) == set(key["ghost_operator_ids"]),
    "Found every accomplice": {a["name"] for a in key["accomplices"]} <= {r["canonical_name"] for r in sus},
}
badges = {
    "SHARD WHISPERER": (report["n"] - report["lost"]) / report["n"] >= 0.985,
    "TIME TRAVELER": truth <= caught,
    "NO INNOCENTS HARMED": caught <= truth,
    "GHOST HUNTER": checks["Unmasked the culprit's full identity"],
    "FOLLOW THE MONEY": checks["Found every accomplice"],
}
earned = [b for b, ok in badges.items() if ok]

lines = [("✔ " if ok else "✘ ") + name for name, ok in checks.items()]
lines += [f"culprit: {key['culprit']['name']} · difficulty {key['difficulty'].upper()} · seed {key['seed']}",
          f"badges: {' · '.join(earned) or 'none'}"]
solved = all(checks.values())
banner("■ CASE CLOSED" if solved else "▲ CASE STILL OPEN", lines, color="#FCEE0A" if solved else "#FF003C")

(spark.createDataFrame([(key["seed"], key["difficulty"], solved, len(earned), ",".join(earned))],
                       "seed INT, difficulty STRING, solved BOOLEAN, badges INT, badge_list STRING")
      .withColumn("closed_at", F.current_timestamp())
      .write.mode("append").saveAsTable(f"{OPS}.case_scoreboard"))

assert solved, "Investigation incomplete, see the failed checks above"
