import json
import os

import pytest

from dfu.forensics.matching import resolve
from dfu.forensics.tamper import SUSPICION_THRESHOLD, replay
from dfu.generator import DIFFICULTY, generate, unseal


def test_same_seed_same_breach(tmp_path):
    a = generate(str(tmp_path / "a"), seed=42, difficulty="nomad")
    b = generate(str(tmp_path / "b"), seed=42, difficulty="nomad")
    assert a["key"] == b["key"]
    assert open(tmp_path / "a/ledger/ledger_v0.jsonl").read() == open(tmp_path / "b/ledger/ledger_v0.jsonl").read()


def test_files_written(scenario):
    p = scenario["path"]
    for sub in ["shard/json", "shard/csv", "shard/csv_legacy", "ledger/ops", "identities", "accounts", "answer_key"]:
        assert os.listdir(os.path.join(p, sub)), sub
    assert len(os.listdir(os.path.join(p, "ledger/ops"))) == 14
    m = json.load(open(os.path.join(p, "manifest.json")))
    assert m["ledger_v0_rows"] == 1500


def test_answer_key_round_trip(scenario):
    key = unseal(os.path.join(scenario["path"], "answer_key/sealed.txt"))
    assert key["culprit"]["person_id"] == scenario["key"]["culprit"]["person_id"]
    assert len(key["ghost_operator_ids"]) == 3
    assert key["tampered"]


def test_legacy_fragments_really_are_latin1(scenario):
    folder = os.path.join(scenario["path"], "shard/csv_legacy")
    raw = b"".join(open(os.path.join(folder, f), "rb").read() for f in os.listdir(folder))
    with pytest.raises(UnicodeDecodeError):
        raw.decode("utf-8")


@pytest.mark.parametrize("difficulty", list(DIFFICULTY))
@pytest.mark.parametrize("seed", [1, 7, 2077])
def test_every_case_is_solvable(tmp_path, difficulty, seed):
    """Fair play: the reference rules must catch exactly the culprit and exactly the tampering."""
    g = generate(str(tmp_path), seed=seed, difficulty=difficulty)
    changes = replay(g["base"], g["days"])
    caught = {c["txn_id"] for c in changes if c["score"] >= SUSPICION_THRESHOLD}
    assert caught == {t["txn_id"] for t in g["key"]["tampered"]}

    recs = [{k: v for k, v in r.items() if not k.startswith("_")} for r in g["identities"]]
    _, clusters = resolve(recs)
    culprit_ids = set(g["key"]["culprit"]["record_ids"])
    assert any(set(members) == culprit_ids for members in clusters.values())
