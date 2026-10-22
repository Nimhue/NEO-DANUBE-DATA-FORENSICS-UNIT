"""CASE 03 · GHOST IDENTITIES — entity resolution with Spark built-ins only.

Pipeline:  normalize -> block -> score -> cluster (label propagation) -> golden records

No GraphFrames, no UDFs: everything runs on serverless / Free Edition, and every step is a
plain DataFrame you can display() and reason about.
"""
from __future__ import annotations

import unicodedata

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from dfu.forensics.matching import MATCH_THRESHOLD, WEIGHTS

_ACCENTED = "áàâäãåăąéèêëęíìîïóòôöõőøúùûüűůñńçćșşțţýźżžđ"
_PLAIN = "".join((unicodedata.normalize("NFKD", c)[0] if c not in "øđ" else {"ø": "o", "đ": "d"}[c]) for c in _ACCENTED)


def normalize(df: DataFrame) -> DataFrame:
    name = F.translate(F.lower(F.col("full_name")), _ACCENTED, _PLAIN)
    tokens = F.filter(F.split(F.trim(F.regexp_replace(name, r"[^a-z\s]", " ")), r"\s+"), lambda t: F.length(t) > 0)
    return (df.withColumn("name_tokens", F.array_sort(tokens))
              .withColumn("name_norm", F.array_join("name_tokens", " "))
              .withColumn("dob_swapped", F.when(F.length("dob") == 10,
                          F.concat(F.substring("dob", 1, 5), F.substring("dob", 9, 2), F.lit("-"), F.substring("dob", 6, 2))))
              .withColumn("handle_norm", F.lower("handle")))


def blocking_keys(df: DataFrame) -> DataFrame:
    """Explode each record into cheap keys. Two records are compared only if they share a key.
    Without blocking, n records = n*(n-1)/2 comparisons; with it, a tiny fraction."""
    sx = F.transform(F.filter("name_tokens", lambda t: F.length(t) > 2), lambda t: F.concat(F.lit("sx:"), F.soundex(t)))
    keys = F.array_compact(F.concat(sx, F.array(
        F.when(F.col("dob").isNotNull(), F.concat(F.lit("dob:"), "dob")),
        F.when(F.col("dob_swapped").isNotNull(), F.concat(F.lit("dob:"), "dob_swapped")),
        F.when(F.col("citizen_id").isNotNull(), F.concat(F.lit("cid:"), F.substring("citizen_id", 1, 7))),
    )))
    return df.select("record_id", F.explode(F.array_distinct(keys)).alias("block_key"))


def candidate_pairs(keys: DataFrame) -> DataFrame:
    a, b = keys.alias("a"), keys.alias("b")
    return (a.join(b, (F.col("a.block_key") == F.col("b.block_key")) & (F.col("a.record_id") < F.col("b.record_id")))
             .select(F.col("a.record_id").alias("a"), F.col("b.record_id").alias("b")).distinct())


def _sim(x, y):
    return F.when(x.isNull() | y.isNull() | (F.length(x) == 0) | (F.length(y) == 0), F.lit(0.0)).otherwise(
        1 - F.levenshtein(x, y) / F.greatest(F.length(x), F.length(y)))


def score_pairs(pairs: DataFrame, norm: DataFrame) -> DataFrame:
    cols = ["record_id", "name_norm", "dob", "dob_swapped", "citizen_id", "district", "handle_norm"]
    A = norm.select(*[F.col(c).alias(f"a_{c}") for c in cols])
    B = norm.select(*[F.col(c).alias(f"b_{c}") for c in cols])
    j = pairs.join(A, pairs.a == A.a_record_id).join(B, pairs.b == B.b_record_id)
    name = _sim(F.col("a_name_norm"), F.col("b_name_norm"))
    dob = (F.when(F.col("a_dob").isNotNull() & (F.col("a_dob") == F.col("b_dob")), 1.0)
            .when(F.col("a_dob_swapped").isNotNull() & (F.col("a_dob_swapped") == F.col("b_dob")), 0.7).otherwise(0.0))
    cid = (F.when(F.col("a_citizen_id") == F.col("b_citizen_id"), 1.0)
            .when(F.levenshtein("a_citizen_id", "b_citizen_id") == 1, 0.8).otherwise(0.0))
    district = F.when(F.col("a_district") == F.col("b_district"), 1.0).otherwise(0.0)
    handle = _sim(F.col("a_handle_norm"), F.col("b_handle_norm"))
    out = j.select("a", "b", F.round(name, 3).alias("f_name"), F.coalesce(dob, F.lit(0.0)).alias("f_dob"),
                   F.coalesce(cid, F.lit(0.0)).alias("f_citizen_id"), district.alias("f_district"),
                   F.round(handle, 3).alias("f_handle"))
    total = sum(F.lit(w) * F.col(f"f_{k}") for k, w in WEIGHTS.items())
    return (out.withColumn("score", F.round(F.least(F.lit(1.0), total), 3))
               .withColumn("is_match", F.col("score") >= MATCH_THRESHOLD))


def cluster(records: DataFrame, matches: DataFrame, max_iter: int = 25) -> DataFrame:
    """Connected components by min-label propagation.

    Every record starts as its own cluster (label = its id). Each round, a record adopts the
    smallest label among itself and its matched neighbours. Stop when nothing changes.
    """
    edges = matches.where("is_match").select("a", "b")
    edges = edges.unionByName(edges.select(F.col("b").alias("a"), F.col("a").alias("b")))
    labels = records.select("record_id", F.col("record_id").alias("cluster_id"))
    for i in range(max_iter):
        neigh = (edges.join(labels.withColumnRenamed("record_id", "b"), "b")
                      .groupBy(F.col("a").alias("record_id")).agg(F.min("cluster_id").alias("n_label")))
        new = (labels.join(neigh, "record_id", "left")
                     .select("record_id", F.least("cluster_id", F.coalesce("n_label", "cluster_id")).alias("cluster_id")))
        new = new.localCheckpoint()
        changed = new.join(labels.withColumnRenamed("cluster_id", "old"), "record_id").where("cluster_id != old").count()
        labels = new
        if changed == 0:
            break
    return labels


def golden_records(records: DataFrame, clusters: DataFrame) -> DataFrame:
    """One row per real person. The CivReg record (civil registry) wins as the canonical view."""
    j = records.join(clusters, "record_id")
    pri = F.when(F.col("source_system") == "CivReg", 0).when(F.col("source_system") == "CorpHR", 1).otherwise(2)
    w = Window.partitionBy("cluster_id").orderBy(pri, "record_id")
    canon = (j.withColumn("_rn", F.row_number().over(w)).where("_rn = 1")
              .select("cluster_id", F.col("full_name").alias("canonical_name"), F.col("dob").alias("canonical_dob"),
                      F.col("citizen_id").alias("canonical_citizen_id"), F.col("district").alias("home_district")))
    agg = (j.groupBy("cluster_id").agg(
        F.count("*").alias("record_count"),
        F.array_sort(F.collect_set("full_name")).alias("aliases"),
        F.array_sort(F.collect_set("record_id")).alias("record_ids"),
        F.array_sort(F.collect_set("source_system")).alias("sources"),
        F.array_sort(F.collect_set("operator_id")).alias("operator_ids"),
        F.array_sort(F.collect_set("district")).alias("districts_seen")))
    return canon.join(agg, "cluster_id").orderBy(F.desc("record_count"))
