"""Export one scenario as site/data.js for the static showcase (no Spark needed).

    python tools/export_site_data.py --seed 2077 --difficulty corpo

Uses the same reference rules as the tests (src/dfu/forensics), so the site shows exactly what the
Databricks pipeline finds for that seed.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from collections import Counter, defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from dfu.forensics.matching import MATCH_THRESHOLD, resolve  # noqa: E402
from dfu.forensics.tamper import SUSPICION_THRESHOLD, replay  # noqa: E402
from dfu.generator import generate  # noqa: E402

TIER_OF = {"garbage": "LOST", "truncated": "SALVAGED", "broken_row": "REPAIRED", "string_amount": "REPAIRED",
           "mixed_timestamp": "REPAIRED", "legacy_encoding": "REPAIRED", "schema_drift": "RESCUED", "clean": "CLEAN"}
TIER_RANK = ["LOST", "SALVAGED", "REPAIRED", "RESCUED", "CLEAN"]


def tier(issues):
    ts = {TIER_OF.get(i) for i in issues if i in TIER_OF} or {"CLEAN"}
    return next(t for t in TIER_RANK if t in ts)


def main(seed: int, difficulty: str, out: str):
    tmp = tempfile.mkdtemp()
    g = generate(tmp, seed, difficulty)
    key, man = g["key"], g["manifest"]

    # ---------------- case 01
    frags = defaultdict(list)
    for t in man["shard_truth"]:
        frags[t["file"]].append({"id": t["txn_id"], "issues": t["issues"], "tier": tier(t["issues"])})
    samples = {}
    for name in sorted(frags):
        path = os.path.join(tmp, "shard", name)
        enc = "latin-1" if "legacy" in name else "utf-8"
        lines = open(path, encoding=enc, errors="replace").read().splitlines()
        if name.endswith(".csv"):
            lines = lines[1:]
        for rec, line in zip(frags[name], lines):
            for iss in rec["issues"]:
                if iss not in samples and iss != "duplicate":
                    samples[iss] = {"file": name, "raw": line[:260]}
    case01 = {"fragments": [{"file": f, "format": f.split("/")[0], "records": r} for f, r in sorted(frags.items())],
              "samples": samples,
              "tiers": dict(Counter(r["tier"] for rs in frags.values() for r in rs)),
              "duplicates": 25}

    # ---------------- case 02
    shard_by_id = {t["txn_id"]: t for t in g["base"]}
    changes = replay(g["base"], g["days"], shard_by_id)
    commits = []
    v = 0
    for d in g["days"]:
        for c in d["commits"]:
            v += 1
            ch = [x for x in changes if x["version"] == v]
            commits.append({"v": v + 1, "day": d["day"], "date": d["date"], "id": c["commit_id"], "note": c["note"],
                            "n": len(ch), "ins": sum(x["change"] == "insert" for x in ch),
                            "upd": sum(x["change"] == "update" for x in ch), "del": sum(x["change"] == "delete" for x in ch),
                            "max": max((x["score"] for x in ch), default=0),
                            "bad": c["commit_id"] in key["malicious_commits"],
                            "ops": sorted({x["operator"] for x in ch if x["operator"] and x["score"] >= SUSPICION_THRESHOLD})})
    flagged = [{"v": x["version"] + 1, "txn": x["txn_id"], "change": x["change"], "flags": x["flags"], "score": x["score"],
                "op": x["operator"], "pre": x["pre"], "post": x["post"]} for x in changes if x["score"] >= 1]
    tampered_ids = {t["txn_id"] for t in key["tampered"]}
    honest = sorted({x["txn_id"] for x in changes if x["change"] == "update" and x["score"] == 0})[:6]
    scrub_ids = [t["txn_id"] for t in key["tampered"] if t["kind"] != "evidence_delete"][:7] + \
                [t["txn_id"] for t in key["tampered"] if t["kind"] == "evidence_delete"][:2] + honest
    state = {t["txn_id"]: dict(t) for t in g["base"] if t["txn_id"] in scrub_ids}
    timeline = {tid: [{"v": 1, **{k: state[tid][k] for k in ("amount", "counterparty_account", "created_at", "status", "modified_by", "modified_at")}}]
                for tid in state}
    v = 0
    for d in g["days"]:
        for c in d["commits"]:
            v += 1
            for o in c["ops"]:
                tid = o["row"]["txn_id"]
                if tid in timeline:
                    snap = None if o["op"] == "delete" else {k: o["row"][k] for k in
                                                              ("amount", "counterparty_account", "created_at", "status", "modified_by", "modified_at")}
                    timeline[tid].append({"v": v + 1, **(snap or {"deleted": True})})
    case02 = {"versions": v + 1, "commits": commits, "flagged": flagged, "timeline": timeline,
              "tampered_ids": sorted(tampered_ids), "threshold": SUSPICION_THRESHOLD,
              "honest_inserts": sum(x["change"] == "insert" for x in changes),
              "honest_updates": sum(x["change"] == "update" and x["score"] < SUSPICION_THRESHOLD for x in changes)}

    # ---------------- case 03
    recs = [{k: v for k, v in r.items() if not k.startswith("_")} for r in g["identities"]]
    pairs, clusters = resolve(recs)
    cl_of = {rid: c for c, m in clusters.items() for rid in m}
    case03 = {"records": recs, "pairs": [p for p in pairs if p["score"] >= 0.3], "threshold": MATCH_THRESHOLD,
              "total_pairs": len(recs) * (len(recs) - 1) // 2, "candidate_pairs": len(pairs),
              "ghost_ops": key["ghost_operator_ids"]}

    # ---------------- case 04
    by_rec = {r["record_id"]: r for r in recs}
    canon = {}
    for c, members in clusters.items():
        ms = sorted(members, key=lambda r: ({"CivReg": 0, "CorpHR": 1}.get(by_rec[r]["source_system"], 2), r))
        canon[c] = by_rec[ms[0]]["full_name"]
    op_owner = {r["operator_id"]: cl_of[r["record_id"]] for r in recs if r.get("operator_id")}
    acc_owner = {a["account_id"]: cl_of.get(a["owner_record_id"]) for a in g["accounts"]}
    acc_meta = {a["account_id"]: a for a in g["accounts"]}
    bad_upd = [x for x in changes if x["score"] >= SUSPICION_THRESHOLD and x["change"] == "update"]
    edges, people = [], defaultdict(lambda: {"edits": 0, "eddies": 0.0, "logins": set(), "accounts": set()})
    for x in bad_upd:
        pc = op_owner.get(x["operator"])
        people[pc]["edits"] += 1
        people[pc]["logins"].add(x["operator"])
        edges.append({"s": x["operator"], "t": x["txn_id"], "rel": "edited", "w": x["post"]["amount"]})
        if "REDIRECT" in x["flags"]:
            acc = x["post"]["counterparty_account"]
            edges.append({"s": x["txn_id"], "t": acc, "rel": "paid_to", "w": x["post"]["amount"]})
            oc = acc_owner.get(acc)
            people[oc]["eddies"] += x["post"]["amount"]
            people[oc]["accounts"].add(acc)
    for c, p in people.items():
        for op in p["logins"]:
            edges.append({"s": canon[c], "t": op, "rel": "signed_as", "w": 1})
        for acc in p["accounts"]:
            edges.append({"s": acc, "t": canon[c], "rel": "owned_by", "w": 1})
    top = max(people.values(), key=lambda p: p["edits"])["edits"]
    suspects = sorted([{"name": canon[c], "cluster": c, "records": len(clusters[c]), "record_ids": sorted(clusters[c]),
                        "aliases": sorted({by_rec[r]["full_name"] for r in clusters[c]}),
                        "role": "PRIME SUSPECT" if p["edits"] == top and top else ("ACCESSORY" if p["edits"] else "ACCOMPLICE"),
                        "edits": p["edits"], "eddies": round(p["eddies"], 2), "logins": sorted(p["logins"]),
                        "accounts": [{"id": a, "opened": acc_meta[a]["opened_at"][:10]} for a in sorted(p["accounts"])]}
                       for c, p in people.items()], key=lambda s: (-s["edits"], -s["eddies"]))
    dedup = {(e["s"], e["t"], e["rel"]): e for e in edges}
    case04 = {"edges": list(dedup.values()), "suspects": suspects}

    data = {"seed": seed, "difficulty": difficulty, "manifest": {k: v for k, v in man.items() if k != "shard_truth"},
            "case01": case01, "case02": case02, "case03": case03, "case04": case04,
            "culprit": key["culprit"]["name"]}
    with open(out, "w", encoding="utf-8") as f:
        f.write("// generated by tools/export_site_data.py, do not edit\nwindow.DFU_DATA = ")
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
        f.write(";\n")
    print(f"wrote {out} ({os.path.getsize(out) // 1024} KB) · culprit {key['culprit']['name']} · "
          f"{len(suspects)} suspects · {len(flagged)} flagged changes")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=2077)
    ap.add_argument("--difficulty", default="corpo")
    ap.add_argument("--out", default=os.path.join(ROOT, "site", "data.js"))
    a = ap.parse_args()
    main(a.seed, a.difficulty, a.out)
