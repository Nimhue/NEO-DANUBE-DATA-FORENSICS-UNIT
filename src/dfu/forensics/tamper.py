"""Reference (pure-Python) tamper-detection rules.

Mirrors notebooks/cases/02_the_tamper.py, which runs the same rules over the
Delta Change Data Feed. Each change is scored:

  strong evidence (2 pts): REDIRECT, BACKDATED, EVIDENCE_DELETE, SHARD_CONTRADICTS
  weak evidence   (1 pt) : OFF_HOURS, ROUND_AMOUNT

A change scoring >= 2 is treated as tampering. A single weak flag is never
enough on its own (the night shift exists, and sometimes people round numbers).
"""
from __future__ import annotations

from datetime import datetime

STRONG = {"REDIRECT", "BACKDATED", "EVIDENCE_DELETE", "SHARD_CONTRADICTS"}
WEAK = {"OFF_HOURS", "ROUND_AMOUNT"}
SUSPICION_THRESHOLD = 2


def flags_for_change(change_type: str, pre: dict | None, post: dict | None, shard: dict | None = None) -> list[str]:
    f = []
    if change_type == "delete":
        if pre and pre.get("status") == "flagged":
            f.append("EVIDENCE_DELETE")
        return f
    if change_type != "update" or not pre or not post:
        return f
    hour = datetime.fromisoformat(post["modified_at"]).hour
    if 0 <= hour < 5:
        f.append("OFF_HOURS")
    if post["amount"] != pre["amount"] and float(post["amount"]) % 500 == 0:
        f.append("ROUND_AMOUNT")
    if post["created_at"] != pre["created_at"]:
        f.append("BACKDATED")
    if post["counterparty_account"] != pre["counterparty_account"]:
        f.append("REDIRECT")
    if shard and (shard.get("counterparty_account") != post["counterparty_account"]
                  or shard.get("created_at") != post["created_at"]):
        f.append("SHARD_CONTRADICTS")
    return f


def score(flags: list[str]) -> int:
    return sum(2 if x in STRONG else 1 for x in flags)


def replay(base: list[dict], days: list[dict], shard_by_id: dict | None = None) -> list[dict]:
    """Replay every commit like Delta would and return one row per change (a fake CDF)."""
    state = {t["txn_id"]: dict(t) for t in base}
    version, out = 0, []
    for d in days:
        for c in d["commits"]:
            version += 1
            for o in c["ops"]:
                row = o["row"]
                pre = state.get(row["txn_id"])
                kind = o["op"]
                fl = flags_for_change(kind, pre, row if kind == "update" else None,
                                      (shard_by_id or {}).get(row["txn_id"]))
                out.append({"version": version, "commit_id": c["commit_id"], "day": d["day"], "change": kind,
                            "txn_id": row["txn_id"], "operator": row.get("modified_by") if kind != "delete" else None,
                            "flags": fl, "score": score(fl),
                            "pre": {k: pre[k] for k in ("amount", "counterparty_account", "created_at", "status")}
                            if pre else None,
                            "post": {k: row[k] for k in ("amount", "counterparty_account", "created_at", "status", "modified_at")}
                            if kind != "delete" else None})
                if kind == "delete":
                    state.pop(row["txn_id"], None)
                else:
                    state[row["txn_id"]] = dict(row)
    return out
