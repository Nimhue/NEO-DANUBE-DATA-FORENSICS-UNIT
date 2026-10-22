"""CASE 04 · THE TRAIL — join every finding into one evidence graph and name names.

    person --signed_as--> operator login --edited--> txn --paid_to--> account --owned_via--> alias --alias_of--> person
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def operator_owners(records: DataFrame, clusters: DataFrame) -> DataFrame:
    return (records.where(F.col("operator_id").isNotNull())
                   .join(clusters, "record_id").select("operator_id", "cluster_id", "record_id"))


def account_owners(accounts: DataFrame, clusters: DataFrame) -> DataFrame:
    return (accounts.join(clusters, accounts.owner_record_id == clusters.record_id, "left")
                    .select("account_id", "account_type", "opened_at", "owner_record_id", "cluster_id"))


def trail_edges(tampered: DataFrame, ops: DataFrame, accs: DataFrame, golden: DataFrame) -> DataFrame:
    """Graph edges, ready for a network visual. `tampered` = flagged updates where is_tamper."""
    upd = tampered.where("change = 'update'")
    names = golden.select("cluster_id", "canonical_name")
    signed = (ops.join(upd.select(F.col("post_modified_by").alias("operator_id")).distinct(), "operator_id")
                 .join(names, "cluster_id")
                 .select(F.col("canonical_name").alias("src"), F.lit("person").alias("src_type"),
                         F.col("operator_id").alias("dst"), F.lit("operator").alias("dst_type"),
                         F.lit("signed_as").alias("rel"), F.lit(1.0).alias("weight")))
    edited = upd.select(F.col("post_modified_by").alias("src"), F.lit("operator").alias("src_type"),
                        F.col("txn_id").alias("dst"), F.lit("txn").alias("dst_type"),
                        F.lit("edited").alias("rel"), F.col("post_amount").alias("weight"))
    paid = (upd.where(F.array_contains("flags", "REDIRECT"))
               .select(F.col("txn_id").alias("src"), F.lit("txn").alias("src_type"),
                       F.col("post_counterparty_account").alias("dst"), F.lit("account").alias("dst_type"),
                       F.lit("paid_to").alias("rel"), F.col("post_amount").alias("weight")))
    owned = (accs.join(paid.select(F.col("dst").alias("account_id")).distinct(), "account_id")
                 .join(names, "cluster_id")
                 .select(F.col("account_id").alias("src"), F.lit("account").alias("src_type"),
                         F.col("canonical_name").alias("dst"), F.lit("person").alias("dst_type"),
                         F.lit("owned_by").alias("rel"), F.lit(1.0).alias("weight")))
    return signed.unionByName(edited).unionByName(paid).unionByName(owned)


def suspects(tampered: DataFrame, ops: DataFrame, accs: DataFrame, golden: DataFrame) -> DataFrame:
    upd = tampered.where("change = 'update'")
    hands = (upd.join(ops, upd.post_modified_by == ops.operator_id)
                .groupBy("cluster_id").agg(F.count("*").alias("tamper_edits"),
                                           F.array_sort(F.collect_set("operator_id")).alias("logins_used")))
    money = (upd.where(F.array_contains("flags", "REDIRECT"))
                .join(accs, upd.post_counterparty_account == accs.account_id)
                .groupBy("cluster_id").agg(F.round(F.sum("post_amount"), 2).alias("eddies_received"),
                                           F.count("*").alias("payments_received"),
                                           F.array_sort(F.collect_set("account_id")).alias("drop_accounts")))
    s = (golden.join(hands, "cluster_id", "left").join(money, "cluster_id", "left")
               .where(F.col("tamper_edits").isNotNull() | F.col("eddies_received").isNotNull()))
    role = (F.when(F.col("tamper_edits") == F.max("tamper_edits").over(Window.partitionBy()), "PRIME SUSPECT")
             .when(F.col("tamper_edits").isNotNull(), "ACCESSORY (edits)")
             .otherwise("ACCOMPLICE (beneficiary)"))
    return (s.withColumn("role", role)
             .select("role", "canonical_name", "cluster_id", "record_count", "aliases", "logins_used",
                     F.coalesce("tamper_edits", F.lit(0)).alias("tamper_edits"),
                     F.coalesce("eddies_received", F.lit(0.0)).alias("eddies_received"),
                     "drop_accounts")
             .orderBy(F.desc("tamper_edits"), F.desc("eddies_received")))
