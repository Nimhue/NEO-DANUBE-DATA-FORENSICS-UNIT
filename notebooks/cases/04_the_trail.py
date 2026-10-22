# Databricks notebook source
# MAGIC %md
# MAGIC # CASE 04 · THE TRAIL — *"Follow the Money"*
# MAGIC
# MAGIC > We know *what* was changed (Case 02) and *who* changed it (Case 03). One question left: where
# MAGIC > did the eddies go, and who was waiting at the other end?
# MAGIC
# MAGIC **Mission:** join all findings into one evidence graph and name the network.
# MAGIC
# MAGIC `person → signed_as → operator login → edited → txn → paid_to → account → owned_by → person`
# MAGIC
# MAGIC | You will learn | Where |
# MAGIC |---|---|
# MAGIC | Modelling a graph as plain edge tables | §1 |
# MAGIC | Multi-hop joins across silver and gold | §2 |
# MAGIC | Spotting freshly opened "drop" accounts | §3 |
# MAGIC | Optional: `ai_query()` to write the case report | §4 |

# COMMAND ----------

# MAGIC %run ../_setup

# COMMAND ----------

from pyspark.sql import functions as F

from dfu.spark import case04_trail as c4

records = spark.table(f"{PROC}.identity_records")
clusters = spark.table(f"{PROC}.identity_clusters")
golden = spark.table(f"{FIND}.golden_identities")
accounts = spark.read.json(f"{LANDING}/accounts/accounts.jsonl")
accounts.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{PROC}.accounts")
tampered = spark.table(f"{FIND}.tamper_changes").where("is_tamper")

ops = c4.operator_owners(records, clusters)
accs = c4.account_owners(spark.table(f"{PROC}.accounts"), clusters)

# COMMAND ----------

# MAGIC %md
# MAGIC ## §1 · The evidence graph as an edge table
# MAGIC A graph is just two columns and a relationship. Stored as Delta, it can feed a dashboard, a
# MAGIC network visual, or GraphFrames later.

# COMMAND ----------

edges = c4.trail_edges(tampered, ops, accs, golden)
edges.write.mode("overwrite").saveAsTable(f"{FIND}.trail_edges")
display(edges.groupBy("rel").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## §2 · Name the network

# COMMAND ----------

sus = c4.suspects(tampered, ops, accs, golden)
sus.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FIND}.suspects")
display(spark.table(f"{FIND}.suspects"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## §3 · The drop accounts were opened days before the breach

# COMMAND ----------

display(spark.sql(f"""
  SELECT a.account_id, a.opened_at, datediff(DATE'2077-03-01', to_date(a.opened_at)) AS days_before_breach,
         g.canonical_name AS real_owner, r.full_name AS registered_under
    FROM {PROC}.accounts a
    JOIN {PROC}.identity_records r ON a.owner_record_id = r.record_id
    JOIN {PROC}.identity_clusters c ON c.record_id = r.record_id
    JOIN {FIND}.golden_identities g ON g.cluster_id = c.cluster_id
   WHERE a.account_id IN (SELECT dst FROM {FIND}.trail_edges WHERE rel = 'paid_to')
   ORDER BY a.opened_at"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## §4 · Case report (optional, needs a model serving endpoint)
# MAGIC `ai_query()` calls a foundation model from SQL. If it isn't available in your workspace the cell
# MAGIC falls back to a template, so the pipeline never fails because of it.

# COMMAND ----------

dbutils.widgets.text("llm_endpoint", "databricks-meta-llama-3-3-70b-instruct", "LLM endpoint")
rows = spark.table(f"{FIND}.suspects").collect()
facts = "; ".join(f"{r['role']}: {r['canonical_name']} ({len(r['aliases'])} aliases, {r['tamper_edits']} edits, "
                  f"{r['eddies_received']} EDD received)" for r in rows)
prompt = ("You are a hard-boiled data forensics detective in Neo-Danube, 2077. In 120 words, in noir first person, "
          f"write the closing report for the Halcyon ledger breach. Facts: {facts}. Only use these facts.")
try:
    report = spark.sql("SELECT ai_query(:ep, :p) AS r", args={"ep": dbutils.widgets.get("llm_endpoint"), "p": prompt}).first()["r"]
except Exception as e:  # noqa: BLE001
    prime = next((r for r in rows if r["role"] == "PRIME SUSPECT"), None)
    report = (f"Rain on the Danube again. The ledger lied, but Delta remembered. "
              f"{prime['canonical_name'] if prime else 'The insider'} wore {len(prime['aliases']) if prime else 'many'} names "
              f"and signed with ghosts. The money ran to {sum(1 for r in rows if r['role'].startswith('ACCOMPLICE'))} "
              "friends with fresh accounts. Case closed. (template report: ai_query unavailable)")
spark.createDataFrame([(report,)], "report STRING").write.mode("overwrite").saveAsTable(f"{FIND}.case_report")
banner("CASE 04 · THE TRAIL", [line for line in report.split(". ") if line][:8], color="#00F0FF")
