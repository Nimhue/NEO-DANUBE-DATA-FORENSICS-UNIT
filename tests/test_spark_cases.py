"""End-to-end on local Spark (no Databricks needed). Skipped if pyspark isn't installed.

Delta-only features (Auto Loader, table_changes, MERGE) are swapped for equivalents:
emulate_autoloader_json() and a fake CDF built from the generator's commits.
"""
import json

import pytest
from conftest import fake_cdf

pytest.importorskip("pyspark")
from pyspark.sql import functions as F  # noqa: E402

from dfu.spark import case01_recovery as c1  # noqa: E402
from dfu.spark import case02_tamper as c2  # noqa: E402
from dfu.spark import case03_identity as c3  # noqa: E402
from dfu.spark import case04_trail as c4  # noqa: E402


@pytest.fixture(scope="module")
def recovered(spark, scenario):
    p = scenario["path"]
    lines = spark.read.text(f"{p}/shard/json").select("value", F.col("_metadata.file_path").alias("_source_file"))
    j = c1.recover_json(c1.emulate_autoloader_json(lines))
    frames = []
    for folder, enc in [("csv", "UTF-8"), ("csv_legacy", "ISO-8859-1")]:
        frames.append(spark.read.format("binaryFile").load(f"{p}/shard/{folder}")
                      .select(F.explode(F.split(F.decode("content", enc), "\r?\n")).alias("value"),
                              F.col("path").alias("_source_file"), F.lit(enc).alias("encoding")))
    csv = c1.recover_csv(frames[0].unionByName(frames[1]))
    return c1.assign_tier(j.unionByName(csv))


def test_case01_recovers_almost_everything(recovered, scenario):
    best = c1.deduplicate(recovered)
    assert best.count() >= 0.97 * scenario["manifest"]["shard_records"]
    tiers = {r["tier"] for r in recovered.select("tier").distinct().collect()}
    assert {"CLEAN", "RESCUED", "REPAIRED", "SALVAGED", "LOST"} <= tiers


def test_case01_recovered_values_are_true(spark, recovered, scenario):
    best = c1.deduplicate(recovered)
    truth = spark.createDataFrame([(t["txn_id"], t["counterparty_account"], float(t["amount"]))
                                   for t in scenario["base"]], "txn_id string, cp string, amt double")
    bad = best.join(truth, "txn_id").where("counterparty_account != cp OR abs(amount - amt) > 0.001").count()
    assert bad == 0


def test_cases_02_to_04_catch_the_culprit(spark, recovered, scenario):
    key = scenario["key"]
    cdf = fake_cdf(spark, scenario["base"], scenario["days"])
    flagged = c2.flag_changes(c2.pair_changes(cdf), c1.deduplicate(recovered)).cache()
    caught = {r.txn_id for r in flagged.where("is_tamper").select("txn_id").distinct().collect()}
    assert caught == {t["txn_id"] for t in key["tampered"]}
    commits = c2.attribute_commits(flagged).where("malicious = 1").count()
    assert commits == len(key["malicious_commits"])

    restore = c2.surgical_restore_source(cdf, flagged.where("is_tamper").select("txn_id", "_commit_version"))
    assert restore.count() == len(caught)

    recs = spark.read.json(f"{scenario['path']}/identities")
    norm = c3.normalize(recs)
    matches = c3.score_pairs(c3.candidate_pairs(c3.blocking_keys(norm)), norm)
    clusters = c3.cluster(recs, matches)
    golden = c3.golden_records(recs, clusters)
    culprit = set(key["culprit"]["record_ids"])
    assert any(set(r.record_ids) == culprit for r in golden.collect())

    accounts = spark.read.json(f"{scenario['path']}/accounts")
    ops = c4.operator_owners(recs, clusters)
    accs = c4.account_owners(accounts, clusters)
    tampered = flagged.where("is_tamper")
    sus = c4.suspects(tampered, ops, accs, golden).collect()
    prime = [s for s in sus if s.role == "PRIME SUSPECT"]
    assert len(prime) == 1 and set(prime[0].logins_used) == set(key["ghost_operator_ids"])
    named = {s.canonical_name for s in sus}
    for acc in key["accomplices"]:
        assert acc["name"] in named, json.dumps(acc)
    edges = c4.trail_edges(tampered, ops, accs, golden)
    assert edges.where("rel = 'owned_by'").count() >= 2
