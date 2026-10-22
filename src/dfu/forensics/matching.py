"""Reference (pure-Python) implementation of the DFU identity-matching rules.

The Spark notebook in notebooks/cases/03_ghost_identities.py implements the
same rules with Spark built-ins. This module exists so that:
  * the rules can be unit-tested without a cluster,
  * the static showcase site can be generated from the same logic,
  * you can read the algorithm in plain Python before reading the Spark version.
"""
from __future__ import annotations

import unicodedata
from itertools import combinations

WEIGHTS = {"name": 0.40, "dob": 0.30, "citizen_id": 0.30, "district": 0.05, "handle": 0.05}
MATCH_THRESHOLD = 0.60


# ---------------------------------------------------------------- normalizers
def strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


def norm_name(name: str | None) -> str:
    """lowercase, no accents, no punctuation, tokens sorted (so 'Kovács, Mira' == 'Mira Kovacs')."""
    if not name:
        return ""
    s = strip_accents(name).lower()
    s = "".join(c if c.isalpha() or c.isspace() else " " for c in s)
    return " ".join(sorted(s.split()))


def swap_day_month(dob: str | None) -> str | None:
    if not dob or len(dob) != 10:
        return None
    y, m, d = dob.split("-")
    return f"{y}-{d}-{m}"


def levenshtein(a: str, b: str) -> int:
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        for j, cb in enumerate(b, 1):
            cur.append(min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (ca != cb)))
        prev = cur
    return prev[-1]


def similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return 1 - levenshtein(a, b) / max(len(a), len(b))


def soundex(word: str) -> str:
    """Classic American Soundex (same as Spark's soundex())."""
    word = "".join(c for c in strip_accents(word).upper() if c.isalpha())
    if not word:
        return ""
    codes = {**dict.fromkeys("BFPV", "1"), **dict.fromkeys("CGJKQSXZ", "2"),
             **dict.fromkeys("DT", "3"), "L": "4", **dict.fromkeys("MN", "5"), "R": "6"}
    out, last = word[0], codes.get(word[0], "")
    for c in word[1:]:
        code = codes.get(c, "")
        if code and code != last:
            out += code
        if c not in "HW":
            last = code
    return (out + "000")[:4]


# ---------------------------------------------------------------- scoring
def score_pair(a: dict, b: dict) -> dict:
    """Return feature scores + total for two identity records."""
    name = similarity(norm_name(a.get("full_name")), norm_name(b.get("full_name")))
    if a.get("dob") and a.get("dob") == b.get("dob"):
        dob = 1.0
    elif a.get("dob") and swap_day_month(a.get("dob")) == b.get("dob"):
        dob = 0.7
    else:
        dob = 0.0
    ca, cb = a.get("citizen_id") or "", b.get("citizen_id") or ""
    if ca and ca == cb:
        cid = 1.0
    elif ca and cb and levenshtein(ca, cb) == 1:
        cid = 0.8
    else:
        cid = 0.0
    district = 1.0 if a.get("district") and a.get("district") == b.get("district") else 0.0
    handle = similarity((a.get("handle") or "").lower(), (b.get("handle") or "").lower())
    feats = {"name": round(name, 3), "dob": dob, "citizen_id": cid, "district": district, "handle": round(handle, 3)}
    total = min(1.0, sum(WEIGHTS[k] * v for k, v in feats.items()))
    return {**feats, "score": round(total, 3)}


def blocking_keys(r: dict) -> set[str]:
    """Cheap keys that put plausible duplicates in the same bucket."""
    keys = {f"sx:{soundex(t)}" for t in norm_name(r.get("full_name")).split() if len(t) > 2}
    if r.get("dob"):
        keys.add(f"dob:{r['dob']}")
        sw = swap_day_month(r["dob"])
        if sw:
            keys.add(f"dob:{sw}")
    if r.get("citizen_id"):
        keys.add(f"cid:{r['citizen_id'][:7]}")
    return keys


def candidate_pairs(records: list[dict]) -> set[tuple[str, str]]:
    buckets: dict[str, list[str]] = {}
    for r in records:
        for k in blocking_keys(r):
            buckets.setdefault(k, []).append(r["record_id"])
    pairs = set()
    for ids in buckets.values():
        for x, y in combinations(sorted(set(ids)), 2):
            pairs.add((x, y))
    return pairs


def resolve(records: list[dict], threshold: float = MATCH_THRESHOLD):
    """Block -> score -> cluster (union-find). Returns (scored_pairs, clusters)."""
    by_id = {r["record_id"]: r for r in records}
    scored = []
    for x, y in sorted(candidate_pairs(records)):
        s = score_pair(by_id[x], by_id[y])
        scored.append({"a": x, "b": y, **s})

    parent = {rid: rid for rid in by_id}

    def find(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    for p in scored:
        if p["score"] >= threshold:
            ra, rb = find(p["a"]), find(p["b"])
            if ra != rb:
                parent[max(ra, rb)] = min(ra, rb)

    clusters: dict[str, list[str]] = {}
    for rid in by_id:
        clusters.setdefault(find(rid), []).append(rid)
    return scored, clusters
