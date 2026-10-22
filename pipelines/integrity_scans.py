# Lakeflow Declarative Pipeline · INTEGRITY SCANS
#
# Certifies recovered evidence before it can be used in court. Expectations are the forensic
# validity checks: each one is declared once, enforced on every run, and its pass/fail counts
# are recorded in the pipeline event log (that's the integrity dashboard for free).
#
#   @dlt.expect           -> keep the row, count the violation       (warning)
#   @dlt.expect_or_drop   -> drop the row, count it                  (inadmissible)
#   @dlt.expect_or_fail   -> stop the pipeline                        (contaminated evidence)
#
# Newer runtimes also expose this API as `from pyspark import pipelines as dp`.
import dlt
from pyspark.sql import functions as F

CATALOG = spark.conf.get("dfu.catalog", "dfu")  # noqa: F821  (spark is provided by the pipeline)

RULES = {
    "valid_txn_id": "txn_id RLIKE '^TX-[0-9]{6}$'",
    "has_counterparty": "counterparty_account IS NOT NULL",
    "has_amount": "amount IS NOT NULL",
    "positive_amount": "amount > 0",
    "not_from_the_future": "created_at <= TIMESTAMP'2077-03-01'",
    "known_currency": "currency = 'EDD'",
}
ADMISSIBLE = ["valid_txn_id", "has_counterparty", "has_amount"]


@dlt.view(comment="Recovered shard records from Case 01")
def recovered():
    return spark.read.table(f"{CATALOG}.evidence_processed.shard_recovered")  # noqa: F821


@dlt.table(comment="Evidence that passed every admissibility rule",
           table_properties={"evidence_class": "certified"})
@dlt.expect_all_or_drop({k: RULES[k] for k in ADMISSIBLE})
@dlt.expect_all({k: v for k, v in RULES.items() if k not in ADMISSIBLE})
@dlt.expect_or_fail("no_duplicates_after_dedup", "_copies >= 1")
def certified_evidence():
    return dlt.read("recovered").withColumn("certified_at", F.current_timestamp())


@dlt.table(comment="Inadmissible evidence with the reasons it failed")
def inadmissible_evidence():
    df = dlt.read("recovered")
    reasons = F.array_compact(F.array(*[F.when(~F.expr(RULES[k]) | F.expr(RULES[k]).isNull(), F.lit(k)) for k in ADMISSIBLE]))
    return df.withColumn("failed_rules", reasons).where(F.size("failed_rules") > 0)


@dlt.table(comment="Integrity scorecard per recovery tier")
def integrity_scorecard():
    return (dlt.read("recovered").groupBy("tier")
            .agg(F.count("*").alias("records"),
                 *[F.round(100 * F.avg(F.expr(f"CASE WHEN {r} THEN 1.0 ELSE 0.0 END")), 1).alias(f"pct_{k}")
                   for k, r in RULES.items()]))
