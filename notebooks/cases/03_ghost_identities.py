# Databricks notebook source
# MAGIC %md
# MAGIC # CASE 03 · GHOST IDENTITIES — *"Who's the Real Merc?"*
# MAGIC
# MAGIC > The edits were signed by operator logins nobody in HR recognises. Neo-Danube has four identity
# MAGIC > systems that don't talk to each other: the civil registry, corporate HR, NetID and the
# MAGIC > StreetNet gossip boards. Somewhere in there, one person is wearing several faces.
# MAGIC
# MAGIC **Mission:** resolve every record to a real person, and find out who owns the ghost logins.
# MAGIC
# MAGIC | You will learn | Where |
# MAGIC |---|---|
# MAGIC | Normalization: accents, casing, name order | §1 |
# MAGIC | Blocking keys: why you never compare every pair | §2 |
# MAGIC | Fuzzy scoring with `levenshtein`, `soundex`, weighted features | §3 |
# MAGIC | Connected components by label propagation, no GraphFrames needed | §4 |
# MAGIC | Golden records with survivorship rules | §5 |

# COMMAND ----------

# MAGIC %run ../_setup

# COMMAND ----------

from pyspark.sql import functions as F

from dfu.forensics.matching import MATCH_THRESHOLD, WEIGHTS
from dfu.spark import case03_identity as c3

records = spark.read.json(f"{LANDING}/identities/identities.jsonl")
records.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{PROC}.identity_records")
records = spark.table(f"{PROC}.identity_records")
display(records.groupBy("source_system").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## §1 · Normalize
# MAGIC `Kovács, Mira` · `MIRA KOVACS` · `mira kovacs` should all become **`kovacs mira`**: lowercase,
# MAGIC accents translated, punctuation dropped, tokens **sorted** so first/last order stops mattering.

# COMMAND ----------

norm = c3.normalize(records).cache()
display(norm.select("source_system", "full_name", "name_norm", "dob", "dob_swapped", "citizen_id").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## §2 · Blocking
# MAGIC Comparing everything with everything is `n(n-1)/2` pairs: fine for 80 records, fatal for 80
# MAGIC million. Each record emits cheap **keys** (soundex of each name token, DOB, DOB with day/month
# MAGIC swapped, citizen-ID prefix) and only records sharing a key get compared.

# COMMAND ----------

keys = c3.blocking_keys(norm)
pairs = c3.candidate_pairs(keys).cache()
n = records.count()
banner("BLOCKING", [f"{n} records → {n * (n - 1) // 2} possible pairs",
                    f"{pairs.count()} candidate pairs after blocking ({100 * pairs.count() / (n * (n - 1) / 2):.1f}%)"],
       color="#00F0FF")

# COMMAND ----------

# MAGIC %md
# MAGIC ## §3 · Score
# MAGIC | Feature | Weight | Scoring |
# MAGIC |---|---|---|
# MAGIC | name | 0.40 | `1 − levenshtein / max length` on normalized names |
# MAGIC | dob | 0.30 | exact 1.0 · day/month swapped 0.7 |
# MAGIC | citizen_id | 0.30 | exact 1.0 · one digit off 0.8 |
# MAGIC | district | 0.05 | same district |
# MAGIC | handle | 0.05 | string similarity |
# MAGIC
# MAGIC Threshold **0.60**: you need two strong signals, or one strong plus a good fuzzy one. The
# MAGIC generator plants **lookalikes** (same surname, same district, similar first name) to punish
# MAGIC name-only matching.

# COMMAND ----------

matches = c3.score_pairs(pairs, norm)
matches.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{PROC}.identity_matches")
display(spark.table(f"{PROC}.identity_matches").orderBy(F.desc("score")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Near misses
# MAGIC Pairs just under the threshold are where an analyst should look by hand. In a real system they'd
# MAGIC go to a review queue.

# COMMAND ----------

display(spark.table(f"{PROC}.identity_matches").where(f"score BETWEEN {MATCH_THRESHOLD - 0.15} AND {MATCH_THRESHOLD}")
        .orderBy(F.desc("score")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## §4 · Cluster
# MAGIC Matches are edges in a graph; a person is a **connected component**. Alias A may match B, and B
# MAGIC match C, while A and C look nothing alike. Label propagation handles that: every record repeatedly
# MAGIC adopts the smallest cluster id among its neighbours until nothing changes.

# COMMAND ----------

clusters = c3.cluster(records, spark.table(f"{PROC}.identity_matches"))
clusters.write.mode("overwrite").saveAsTable(f"{PROC}.identity_clusters")

# COMMAND ----------

# MAGIC %md
# MAGIC ## §5 · Golden records
# MAGIC Survivorship rule: the civil registry wins, then HR, then the rest. Every alias is kept as evidence.

# COMMAND ----------

golden = c3.golden_records(records, spark.table(f"{PROC}.identity_clusters"))
golden.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FIND}.golden_identities")

ghost_logins = [r[0] for r in spark.table(f"{FIND}.commit_audit").where("malicious = 1")
                .select(F.explode("signed_by")).distinct().collect()]
owner = (spark.table(f"{FIND}.golden_identities")
         .where(F.arrays_overlap("operator_ids", F.array(*[F.lit(x) for x in ghost_logins]))))
display(owner)

o = owner.orderBy(F.desc("record_count")).first()
if o:
    banner("CASE 03 · GHOST UNMASKED", [
        f"{records.count()} records → {golden.count()} real people",
        f"ghost logins {', '.join(ghost_logins)} all resolve to ONE person:",
        f"{o['canonical_name'].upper()}  ({o['record_count']} records, {len(o['aliases'])} names)",
        f"also holds operator login(s): {', '.join(x for x in o['operator_ids'] if x not in ghost_logins) or 'none'}",
        "→ who did they pay? Case 04.",
    ], color="#FCEE0A")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧠 Check yourself
# MAGIC 1. Why sort name tokens instead of trying to detect which token is the surname?
# MAGIC 2. Raise the threshold to 0.8. Who falls out of the culprit's cluster, and why?
# MAGIC 3. Why is `localCheckpoint()` inside the label-propagation loop?
# MAGIC
# MAGIC <details><summary>Answers</summary>
# MAGIC
# MAGIC 1. Sources disagree on order and punctuation. Sorting makes the representation order-free without guessing.
# MAGIC 2. Aliases matching on only two features (e.g. a fake name with real DOB and citizen ID) score ~0.6 and get split off. Recall drops.
# MAGIC 3. Each iteration stacks another join onto the plan. Checkpointing truncates the lineage so the plan doesn't grow without bound.
# MAGIC </details>
