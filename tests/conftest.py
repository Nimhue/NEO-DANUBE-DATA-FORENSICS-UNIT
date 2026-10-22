import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


@pytest.fixture(scope="session")
def scenario(tmp_path_factory):
    from dfu.generator import generate
    out = tmp_path_factory.mktemp("landing")
    g = generate(str(out), seed=2077, difficulty="corpo")
    g["path"] = str(out)
    return g


@pytest.fixture(scope="session")
def spark():
    pyspark = pytest.importorskip("pyspark")  # noqa: F841
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[2]").appName("dfu-tests")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "4")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def fake_cdf(spark, base, days):
    """Build what `table_changes()` would return, without needing Delta locally."""
    from dfu.spark.case02_tamper import LEDGER_COLS
    state = {t["txn_id"]: dict(t) for t in base}
    rows, version = [], 0
    for d in days:
        for c in d["commits"]:
            version += 1
            ts = f"{d['date']}T12:00:00"
            for o in c["ops"]:
                r, tid = o["row"], o["row"]["txn_id"]
                if o["op"] == "insert":
                    rows.append({**r, "_change_type": "insert", "_commit_version": version, "_commit_timestamp": ts})
                elif o["op"] == "update":
                    rows.append({**state[tid], "_change_type": "update_preimage", "_commit_version": version, "_commit_timestamp": ts})
                    rows.append({**r, "_change_type": "update_postimage", "_commit_version": version, "_commit_timestamp": ts})
                else:
                    rows.append({**state[tid], "_change_type": "delete", "_commit_version": version, "_commit_timestamp": ts})
                if o["op"] == "delete":
                    state.pop(tid, None)
                else:
                    state[tid] = dict(r)
    ddl = ", ".join(f"{c} {'DOUBLE' if c == 'amount' else 'STRING'}" for c in LEDGER_COLS)
    ddl += ", _change_type STRING, _commit_version BIGINT, _commit_timestamp STRING"
    from pyspark.sql import functions as F
    df = spark.createDataFrame([{k: (float(v) if k == "amount" else v) for k, v in r.items()} for r in rows], ddl)
    for c in ("created_at", "modified_at", "_commit_timestamp"):
        df = df.withColumn(c, F.to_timestamp(c))
    return df
