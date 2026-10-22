"""Neo-Danube breach scenario generator.

Pure Python (stdlib only) so it runs anywhere: your laptop, CI, or a Databricks
notebook writing into a Unity Catalog volume.

    python -m dfu.generator --seed 2077 --difficulty corpo --out ./landing

Produces
    shard/json/*.jsonl          stolen ledger fragments (some corrupted)
    shard/csv/*.csv             more fragments, broken rows, quoted memos
    shard/csv_legacy/*.csv      fragments in latin-1 from an old cyberdeck
    ledger/ledger_v0.jsonl      Halcyon's ledger before the breach window
    ledger/ops/day_XX.json      14 days of commits (legit + malicious)
    identities/identities.jsonl identity records from 4 source systems
    accounts/accounts.jsonl     account ownership
    answer_key/sealed.txt       base64 answer key (no peeking, detective)
    manifest.json               what was generated, for the showcase site
"""
from __future__ import annotations

import base64
import csv
import io
import json
import os
import random
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone

DIFFICULTY = {
    #             people  extra aliases  night-shift noise  shard corruption  lookalike pairs
    "street_kid": dict(people=24, extra_aliases=1, noise=0.5, corruption=0.15, lookalikes=2),
    "corpo":      dict(people=36, extra_aliases=2, noise=1.0, corruption=0.25, lookalikes=4),
    "nomad":      dict(people=48, extra_aliases=3, noise=1.5, corruption=0.35, lookalikes=6),
    "legend":     dict(people=64, extra_aliases=4, noise=2.0, corruption=0.45, lookalikes=9),
}

DISTRICTS = ["Óbuda Arcology", "Pest Sprawl", "Csepel Docks", "Gellért Spire",
             "Margit Ring", "Kőbánya Stacks", "Buda Heights", "Ferenc Grid"]
FIRST = ["Mira", "Dorian", "Ilona", "Kenji", "Zsófia", "Radu", "Anaïs", "Teodor", "Yuki", "Bence",
         "Livia", "Marek", "Noor", "Csaba", "Ioana", "Viktor", "Hana", "Levente", "Sorin", "Petra",
         "Aiko", "Gábor", "Elena", "Tomas", "Rin", "Dóra", "Mihai", "Lena", "Ádám", "Selin",
         "Bogdan", "Emese", "Kaito", "Vera", "Andrei", "Réka", "Oskar", "Daria", "Nándor", "Saya",
         "Cristian", "Boróka", "Ivo", "Tünde", "Haruto", "Anca", "Lőrinc", "Maja", "Felix", "Kinga",
         "Stefan", "Orsolya", "Jun", "Alina", "Zoltán", "Iris", "Matei", "Eszter", "Ryo", "Nadia",
         "Vlad", "Hédi", "Sota", "Corina"]
LAST = ["Kovács", "Vance", "Tanaka", "Popescu", "Szabó", "Moreau", "Horváth", "Ionescu", "Nakamura",
        "Varga", "Dumitru", "Kiss", "Okafor", "Molnár", "Sato", "Constantin", "Farkas", "Lindqvist",
        "Balogh", "Marin", "Kimura", "Németh", "Stoica", "Papp", "Reyes", "Takács", "Florea",
        "Juhász", "Ishikawa", "Lukács", "Petrov", "Oláh", "Radu", "Mészáros", "Hayashi", "Simon"]
NICK = {"Dorian": "Dor", "Zsófia": "Zsófi", "Teodor": "Teo", "Ilona": "Ili", "Levente": "Levi",
        "Viktor": "Vik", "Bogdan": "Bogi", "Cristian": "Cris", "Alexandra": "Alex", "Nándor": "Nándi",
        "Orsolya": "Orsi", "Mihai": "Mișu", "Andrei": "Andi", "Gábor": "Gabi", "Eszter": "Eszti",
        "Kenji": "Ken", "Matei": "Mat", "Stefan": "Ștef", "Radu": "Rad", "Zoltán": "Zoli"}
SOURCES = ["CivReg", "CorpHR", "NetID", "StreetNet"]
CORP_ACCOUNTS = ["HAL-OPS", "HAL-GRID", "HAL-WATER", "HAL-TRANSIT"]
CATEGORIES = ["maintenance", "payroll", "procurement", "security", "logistics", "energy", "consulting"]
VENDORS = ["Kiroshi Optics Supply", "Danube Freight Collective", "Arc Lattice Ltd", "NeonWorks Signage",
           "Militech-adjacent Logistics", "Spire Cleaning Drones", "Chrome & Coolant Co", "Gridline Services",
           "Ferro Dynamics", "Blue Lotus Catering", "Static Bloom Media", "Hollow Pine Security"]

EPOCH = datetime(2077, 3, 1)          # breach window starts here
DAYS = 14


@dataclass
class Person:
    person_id: str
    first: str
    last: str
    dob: str
    district: str
    citizen_id: str
    handle: str
    role: str = "civilian"            # civilian | operator | night_shift | culprit | accomplice
    operator_id: str | None = None


# ---------------------------------------------------------------- helpers
def _typo(rng: random.Random, s: str) -> str:
    if len(s) < 4:
        return s
    i = rng.randrange(1, len(s) - 1)
    kind = rng.choice(["swap", "drop", "replace", "double"])
    if kind == "swap":
        return s[:i] + s[i + 1] + s[i] + s[i + 2:]
    if kind == "drop":
        return s[:i] + s[i + 1:]
    if kind == "double":
        return s[:i] + s[i] + s[i:]
    return s[:i] + rng.choice("aeiourstn") + s[i + 1:]


def _deaccent(s: str) -> str:
    import unicodedata
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


def _cid_typo(rng: random.Random, cid: str) -> str:
    digits = list(cid)
    idxs = [i for i, c in enumerate(digits) if c.isdigit()]
    i = rng.choice(idxs)
    digits[i] = str((int(digits[i]) + rng.randint(1, 8)) % 10)
    return "".join(digits)


def _swap_dm(dob: str) -> str:
    y, m, d = dob.split("-")
    return f"{y}-{d}-{m}"


def _ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


# ---------------------------------------------------------------- generator
class BreachGenerator:
    def __init__(self, seed: int = 2077, difficulty: str = "corpo"):
        if difficulty not in DIFFICULTY:
            raise ValueError(f"difficulty must be one of {list(DIFFICULTY)}")
        self.seed, self.difficulty = seed, difficulty
        self.cfg = DIFFICULTY[difficulty]
        self.rng = random.Random(seed)
        self._rec = 0
        self._acc = 0
        self._txn_n = 0
        self._malicious_commits: list[str] = []

    # ---------- people
    def _people(self) -> list[Person]:
        rng, n = self.rng, self.cfg["people"]
        used, people = set(), []
        while len(people) < n:
            f, ln = rng.choice(FIRST), rng.choice(LAST)
            if (f, ln) in used:
                continue
            used.add((f, ln))
            people.append(self._new_person(len(people), f, ln))
        # lookalikes: same surname + district, similar first name, different everything else
        for k in range(self.cfg["lookalikes"]):
            src = people[rng.randrange(len(people))]
            f = src.first[:-1] + rng.choice("aeio") if len(src.first) > 3 else src.first + "a"
            if (f, src.last) in used:
                continue
            used.add((f, src.last))
            p = self._new_person(len(people), f, src.last)
            p.district = src.district
            people.append(p)

        idx = list(range(len(people)))
        rng.shuffle(idx)
        roles = ["culprit", "accomplice", "accomplice", "night_shift"] + ["operator"] * 5
        for i, role in zip(idx, roles):
            people[i].role = role
        op_n = 0
        for p in people:
            if p.role in ("operator", "night_shift", "culprit"):
                op_n += 1
                p.operator_id = f"OP-{100 + op_n * 7}"
        return people

    def _new_person(self, i: int, first: str, last: str) -> Person:
        rng = self.rng
        dob = date(2077 - rng.randint(22, 58), rng.randint(1, 12), rng.randint(1, 28)).isoformat()
        cid = f"ND-{rng.randint(10000000, 99999999)}"
        handle = rng.choice(["", "x", "_", "0"]) + _deaccent(first).lower()[:4] + rng.choice(
            ["", "_", "."]) + _deaccent(last).lower()[:3] + str(rng.randint(1, 99))
        return Person(f"P{i + 1:03d}", first, last, dob, rng.choice(DISTRICTS), cid, handle)

    # ---------- identity records
    def _record(self, p: Person, source: str, **over) -> dict:
        self._rec += 1
        r = {"record_id": f"ID-{self._rec:04d}", "source_system": source,
             "full_name": f"{p.first} {p.last}", "dob": p.dob, "district": p.district,
             "citizen_id": p.citizen_id, "handle": p.handle, "operator_id": None,
             "registered_at": _ts(EPOCH - timedelta(days=self.rng.randint(30, 3000)))}
        r.update(over)
        r["_person_id"] = p.person_id  # ground truth, stripped before writing
        return r

    def _alias(self, p: Person, source: str, strength: int, **over) -> dict:
        """An alias keeps two of {name, dob, citizen_id} recognisable and mangles the rest.

        Every alias is re-rolled until it is *solvable*: it must still score above the
        match threshold against the person's canonical record. Hard, but fair.
        """
        from dfu.forensics.matching import MATCH_THRESHOLD, score_pair
        canon = {"full_name": f"{p.first} {p.last}", "dob": p.dob, "citizen_id": p.citizen_id,
                 "district": p.district, "handle": p.handle}
        for _ in range(40):
            saved = self._rec
            r = self._alias_once(p, source, strength, **over)
            if score_pair(canon, r)["score"] >= MATCH_THRESHOLD + 0.04:
                return r
            self._rec = saved
        return r

    def _alias_once(self, p: Person, source: str, strength: int, **over) -> dict:
        rng = self.rng
        keep = set(rng.sample(["name", "dob", "cid"], 2))
        first, last = p.first, p.last
        if "name" in keep:
            tweaks = rng.sample(["nick", "typo", "order", "deaccent", "upper"], k=min(strength, 3))
            if "nick" in tweaks:
                first = NICK.get(first, first)
            if "typo" in tweaks:
                last = _typo(rng, last)
            name = f"{first} {last}"
            if "deaccent" in tweaks:
                name = _deaccent(name)
            if "order" in tweaks:
                name = f"{name.split(' ', 1)[1]}, {name.split(' ', 1)[0]}"
            if "upper" in tweaks:
                name = name.upper()
        else:
            name = f"{rng.choice(FIRST)} {rng.choice(LAST)}"   # fully fake name
        dob = p.dob if "dob" in keep else (_swap_dm(p.dob) if int(p.dob[-2:]) <= 12 and rng.random() < .5
                                           else None)
        cid = p.citizen_id if "cid" in keep else (_cid_typo(rng, p.citizen_id) if rng.random() < .6 else None)
        if "dob" in keep and "cid" in keep and "name" not in keep:
            pass
        # make sure the alias is still solvable: two strong signals or one strong + one fuzzy
        if dob is None and cid is None:
            cid = _cid_typo(rng, p.citizen_id)
        handle = p.handle.replace("a", "4").replace("e", "3") + rng.choice(["", "_x", "77"])
        return self._record(p, source, full_name=name, dob=dob, citizen_id=cid,
                            district=rng.choice(DISTRICTS), handle=handle, **over)

    def _identities(self, people: list[Person]) -> tuple[list[dict], dict]:
        rng, recs = self.rng, []
        canonical, ghost_ops, aliases = {}, [], {}
        for p in people:
            c = self._record(p, "CivReg", handle=None)
            canonical[p.person_id] = c
            recs.append(c)
            if p.operator_id:
                recs.append(self._record(p, "CorpHR", operator_id=p.operator_id,
                                         full_name=f"{p.last}, {p.first}"))
            if p.role == "civilian" and rng.random() < 0.45:
                recs.append(self._alias(p, rng.choice(["NetID", "StreetNet"]), 1))

        culprit = next(p for p in people if p.role == "culprit")
        for g in range(3):                       # ghost operator logins
            op = f"OP-{900 + g * 13 + rng.randint(0, 9)}"
            ghost_ops.append(op)
            recs.append(self._alias(culprit, "CorpHR", 2, operator_id=op))
        for _ in range(self.cfg["extra_aliases"]):
            recs.append(self._alias(culprit, "StreetNet", 3))
        for p in people:
            if p.role == "accomplice":
                aliases[p.person_id] = []
                for _ in range(1 + self.cfg["extra_aliases"] // 2 + 1):
                    a = self._alias(p, rng.choice(["NetID", "StreetNet"]), 2)
                    aliases[p.person_id].append(a)
                    recs.append(a)
        culprit_alias = next(r for r in recs if r["_person_id"] == culprit.person_id
                             and r["source_system"] == "StreetNet")
        aliases[culprit.person_id] = [culprit_alias]
        rng.shuffle(recs)
        return recs, {"canonical": canonical, "ghost_ops": ghost_ops, "drop_owners": aliases}

    # ---------- accounts
    def _new_account(self, owner_record_id, kind, district, name=None) -> dict:
        self._acc += 1
        return {"account_id": f"ACC-{self._acc:05d}", "owner_record_id": owner_record_id,
                "owner_name": name, "account_type": kind, "district": district,
                "opened_at": _ts(EPOCH - timedelta(days=self.rng.randint(5, 2000)))}

    def _accounts(self, people, ident) -> tuple[list[dict], list[str]]:
        accs = []
        for p in people:
            accs.append(self._new_account(ident["canonical"][p.person_id]["record_id"], "personal", p.district))
        for v in VENDORS:
            accs.append(self._new_account(None, "vendor", self.rng.choice(DISTRICTS), v))
        drops = []
        for pid, alias_recs in ident["drop_owners"].items():
            for a in alias_recs[:2]:
                acc = self._new_account(a["record_id"], "personal", a["district"])
                acc["opened_at"] = _ts(EPOCH - timedelta(days=self.rng.randint(3, 20)))  # freshly opened
                accs.append(acc)
                drops.append(acc["account_id"])
        return accs, drops

    # ---------- ledger
    def _txn(self, accounts, operators, created: datetime) -> dict:
        rng = self.rng
        self._txn_n += 1
        cp = rng.choice([a for a in accounts if a["account_type"] in ("vendor", "personal")
                         and not a.get("_drop")])
        amount = round(rng.lognormvariate(7.2, 1.1), 2)
        status = rng.choices(["settled", "pending", "flagged", "void"], [70, 20, 6, 4])[0]
        op = rng.choice(operators)
        return {"txn_id": f"TX-{self._txn_n:06d}", "account_id": rng.choice(CORP_ACCOUNTS),
                "counterparty_account": cp["account_id"], "amount": amount, "currency": "EDD",
                "category": rng.choice(CATEGORIES), "district": rng.choice(DISTRICTS), "status": status,
                "memo": f"{rng.choice(CATEGORIES)} ref {rng.randint(1000, 9999)}",
                "created_at": _ts(created), "modified_at": _ts(created), "modified_by": op}

    def _ledger(self, people, accounts, drops, ghost_ops):
        rng, cfg = self.rng, self.cfg
        for a in accounts:
            a["_drop"] = a["account_id"] in drops
        day_ops = [p.operator_id for p in people if p.role in ("operator", "culprit")]
        night = next(p.operator_id for p in people if p.role == "night_shift")
        base = []
        for _ in range(1500):
            created = EPOCH - timedelta(days=rng.randint(1, 60), hours=rng.randint(7, 18), minutes=rng.randint(0, 59))
            base.append(self._txn(accounts, day_ops, created))
        state = {t["txn_id"]: dict(t) for t in base}
        touched_malicious: set[str] = set()
        tampered: list[dict] = []
        days = []
        malicious_days = sorted(rng.sample(range(1, DAYS + 1), 8))
        for d in range(1, DAYS + 1):
            day = EPOCH + timedelta(days=d - 1)
            commits = []
            # --- legit daytime commit
            ops = []
            live = [t for t in state.values() if t["txn_id"] not in touched_malicious]
            pending = [x for x in live if x["status"] == "pending"]
            for t in rng.sample(pending, k=min(18, len(pending))):
                new = dict(t, status="settled", modified_by=rng.choice(day_ops),
                           modified_at=_ts(day + timedelta(hours=rng.randint(8, 17), minutes=rng.randint(0, 59))))
                ops.append({"op": "update", "row": new})
            for t in rng.sample(live, k=4):  # small fee corrections, non-round
                if t["txn_id"] in {o["row"]["txn_id"] for o in ops}:
                    continue
                new = dict(t, amount=round(t["amount"] + rng.choice([-1, 1]) * rng.uniform(0.5, 40), 2),
                           modified_by=rng.choice(day_ops),
                           modified_at=_ts(day + timedelta(hours=rng.randint(9, 16), minutes=rng.randint(0, 59))))
                ops.append({"op": "update", "row": new})
            if rng.random() < 0.5:   # a legit round-number correction (noise)
                t = rng.choice(live)
                if t["txn_id"] not in {o["row"]["txn_id"] for o in ops}:
                    new = dict(t, amount=float(round(t["amount"], -2) or 100), modified_by=rng.choice(day_ops),
                               modified_at=_ts(day + timedelta(hours=rng.randint(10, 15))))
                    ops.append({"op": "update", "row": new})
            for _ in range(rng.randint(4, 9)):   # new transactions
                created = day + timedelta(hours=rng.randint(8, 17), minutes=rng.randint(0, 59))
                t = self._txn(accounts, day_ops, created)
                t["status"] = "pending"
                ops.append({"op": "insert", "row": t})
            voids = [t for t in live if t["status"] == "void"]
            for t in rng.sample(voids, k=min(len(voids), rng.randint(0, 2))):
                if t["txn_id"] not in {o["row"]["txn_id"] for o in ops}:
                    ops.append({"op": "delete", "row": t})
            commits.append({"commit_id": self._cid(d, "A"), "note": "routine reconciliation", "ops": ops})
            self._apply(state, ops)

            # --- legit night shift (off-hours, status only) -> false-positive bait
            n_night = int(rng.randint(2, 6) * cfg["noise"])
            pend = [t for t in state.values() if t["status"] == "pending" and t["txn_id"] not in touched_malicious]
            if n_night and pend:
                nops = [{"op": "update", "row": dict(t, status="settled", modified_by=night,
                                                     modified_at=_ts(day + timedelta(hours=23, minutes=rng.randint(0, 120))))}
                        for t in rng.sample(pend, k=min(n_night, len(pend)))]
                commits.append({"commit_id": self._cid(d, "N"), "note": "night shift settlement", "ops": nops})
                self._apply(state, nops)

            # --- malicious commit
            if d in malicious_days:
                mops = []
                pool = [t for t in state.values() if t["txn_id"] not in touched_malicious
                        and t["status"] in ("settled", "pending") and t["amount"] > 800
                        and t["txn_id"] in {b["txn_id"] for b in base}]
                for t in rng.sample(pool, k=rng.randint(3, 5)):
                    kind = rng.choice(["redirect", "redirect", "redirect_backdate", "skim"])
                    at = day + timedelta(hours=rng.randint(1, 4), minutes=rng.randint(0, 59))
                    new = dict(t, modified_by=rng.choice(ghost_ops), modified_at=_ts(at))
                    if kind.startswith("redirect"):
                        new["counterparty_account"] = rng.choice(drops)
                        new["amount"] = float(rng.choice([2500, 5000, 7500, 10000, 12000, 15000, 20000]))
                    if kind == "redirect_backdate":
                        new["created_at"] = _ts(datetime.fromisoformat(t["created_at"]) - timedelta(days=rng.randint(20, 45)))
                    if kind == "skim":
                        new["amount"] = float(int(t["amount"] * 1.6 / 500 + 1) * 500)
                        new["counterparty_account"] = rng.choice(drops)
                    mops.append({"op": "update", "row": new})
                    touched_malicious.add(t["txn_id"])
                    tampered.append({"txn_id": t["txn_id"], "kind": kind, "day": d, "operator": new["modified_by"],
                                     "before": {k: t[k] for k in ("amount", "counterparty_account", "created_at")},
                                     "after": {k: new[k] for k in ("amount", "counterparty_account", "created_at")}})
                flagged = [t for t in state.values() if t["status"] == "flagged" and t["txn_id"] not in touched_malicious]
                for t in rng.sample(flagged, k=min(len(flagged), rng.randint(1, 3))):
                    mops.append({"op": "delete", "row": t})
                    touched_malicious.add(t["txn_id"])
                    tampered.append({"txn_id": t["txn_id"], "kind": "evidence_delete", "day": d,
                                     "operator": rng.choice(ghost_ops), "before": {"status": "flagged"}, "after": None})
                commits.append({"commit_id": self._cid(d, "X"), "note": "routine reconciliation", "ops": mops})
                self._malicious_commits.append(commits[-1]["commit_id"])
                self._apply(state, mops)
            days.append({"day": d, "date": day.date().isoformat(), "commits": commits})
        return base, days, tampered, malicious_days

    def _cid(self, day: int, kind: str) -> str:
        """Opaque commit id: nothing in the id gives the game away."""
        import hashlib
        return "c" + hashlib.sha1(f"{self.seed}:{day}:{kind}".encode()).hexdigest()[:9]

    @staticmethod
    def _apply(state, ops):
        for o in ops:
            if o["op"] == "delete":
                state.pop(o["row"]["txn_id"], None)
            else:
                state[o["row"]["txn_id"]] = dict(o["row"])

    # ---------- the stolen shard
    def _shard(self, base, tampered):
        rng, corr = self.rng, self.cfg["corruption"]
        tampered_ids = {t["txn_id"] for t in tampered}
        must = [t for t in base if t["txn_id"] in tampered_ids and rng.random() < 0.85]
        rest = rng.sample([t for t in base if t["txn_id"] not in tampered_ids], 900)
        rows = must + rest
        rng.shuffle(rows)
        files, truth = {}, []
        chunks = [rows[i:i + 60] for i in range(0, len(rows), 60)]
        for i, chunk in enumerate(chunks):
            fmt = "json" if i % 3 != 2 else ("csv_legacy" if i % 6 == 5 else "csv")
            name = f"{fmt}/frag_{i:03d}.{'jsonl' if fmt == 'json' else 'csv'}"
            drift = fmt == "json" and i % 4 == 1          # later firmware renamed fields
            lines = []
            for t in chunk:
                rec = dict(t)
                kinds = []
                if rng.random() < corr * 0.5:
                    rec["created_at"] = rng.choice([
                        str(int(datetime.fromisoformat(t["created_at"]).replace(tzinfo=timezone.utc).timestamp())),
                        datetime.fromisoformat(t["created_at"]).strftime("%d/%m/%Y %H:%M")])
                    kinds.append("mixed_timestamp")
                if fmt == "json":
                    if drift:
                        rec["amt"] = rec.pop("amount")
                        rec["x_sig"] = f"{rng.getrandbits(32):08x}"
                        kinds.append("schema_drift")
                    if rng.random() < corr * 0.4:
                        key = "amt" if drift else "amount"
                        rec[key] = f"{rec[key]:,.2f} EDD"
                        kinds.append("string_amount")
                    line = json.dumps(rec, ensure_ascii=False)
                    r = rng.random()
                    if r < corr * 0.25:
                        line = line[: rng.randint(15, len(line) - 10)]
                        kinds.append("truncated")
                    elif r < corr * 0.32:
                        line = "".join(rng.choice("░▒▓█▄▀#@$%&") for _ in range(rng.randint(20, 60)))
                        kinds.append("garbage")
                    lines.append(line)
                else:
                    if rng.random() < corr * 0.2:
                        rec["memo"] = rec["memo"] + ', "urgent", see ' + rng.choice(LAST)
                        kinds.append("broken_row")
                        rec["_broken"] = True
                    elif rng.random() < corr * 0.4:
                        rec["amount"] = f"{rec['amount']:,.2f} EDD"
                        kinds.append("string_amount")
                    lines.append(rec)
                if fmt == "csv_legacy":
                    kinds.append("legacy_encoding")
                if rng.random() < 0.04:
                    kinds.append("duplicate")
                truth.append({"txn_id": t["txn_id"], "file": name, "issues": kinds or ["clean"]})
            files[name] = (fmt, lines)
        # duplicates: replay some records into a final "resend" fragment
        dups = [rows[i] for i in rng.sample(range(len(rows)), 25)]
        files["json/frag_resend.jsonl"] = ("json", [json.dumps(t, ensure_ascii=False) for t in dups])
        return files, truth, len(rows)

    # ---------- write
    def write(self, out: str) -> dict:
        people = self._people()
        idents, ident_meta = self._identities(people)
        accounts, drops = self._accounts(people, ident_meta)
        base, days, tampered, mal_days = self._ledger(people, accounts, drops, ident_meta["ghost_ops"])
        files, shard_truth, shard_n = self._shard(base, tampered)

        def path(*p):
            fp = os.path.join(out, *p)
            os.makedirs(os.path.dirname(fp), exist_ok=True)
            return fp

        cols = list(base[0].keys())
        for name, (fmt, lines) in files.items():
            if fmt == "json":
                with open(path("shard", name), "w", encoding="utf-8") as f:
                    f.write("\n".join(lines) + "\n")
            else:
                enc = "latin-1" if fmt == "csv_legacy" else "utf-8"
                buf = io.StringIO()
                w = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
                w.writerow(cols)
                for rec in lines:
                    row = [rec.get(c, "") for c in cols]
                    if rec.get("_broken"):  # unquoted commas -> wrong column count
                        buf.write(",".join(str(v) for v in row) + "\n")
                    else:
                        w.writerow(row)
                with open(path("shard", name), "w", encoding=enc, errors="replace", newline="") as f:
                    f.write(buf.getvalue())

        with open(path("ledger", "ledger_v0.jsonl"), "w", encoding="utf-8") as f:
            for t in base:
                f.write(json.dumps(t, ensure_ascii=False) + "\n")
        for d in days:
            with open(path("ledger", "ops", f"day_{d['day']:02d}.json"), "w", encoding="utf-8") as f:
                json.dump(d, f, ensure_ascii=False, indent=1)
        with open(path("identities", "identities.jsonl"), "w", encoding="utf-8") as f:
            for r in idents:
                f.write(json.dumps({k: v for k, v in r.items() if not k.startswith("_")}, ensure_ascii=False) + "\n")
        with open(path("accounts", "accounts.jsonl"), "w", encoding="utf-8") as f:
            for a in accounts:
                f.write(json.dumps({k: v for k, v in a.items() if not k.startswith("_")}, ensure_ascii=False) + "\n")

        culprit = next(p for p in people if p.role == "culprit")
        accomplices = [p for p in people if p.role == "accomplice"]
        key = {
            "seed": self.seed, "difficulty": self.difficulty,
            "culprit": {"person_id": culprit.person_id, "name": f"{culprit.first} {culprit.last}",
                        "real_operator_id": culprit.operator_id,
                        "record_ids": sorted(r["record_id"] for r in idents if r["_person_id"] == culprit.person_id)},
            "ghost_operator_ids": ident_meta["ghost_ops"],
            "accomplices": [{"person_id": p.person_id, "name": f"{p.first} {p.last}",
                             "record_ids": sorted(r["record_id"] for r in idents if r["_person_id"] == p.person_id)}
                            for p in accomplices],
            "drop_accounts": drops,
            "malicious_days": mal_days,
            "malicious_commits": list(self._malicious_commits),
            "tampered": tampered,
            "identity_truth": {r["record_id"]: r["_person_id"] for r in idents},
        }
        with open(path("answer_key", "sealed.txt"), "w") as f:
            f.write(base64.b64encode(json.dumps(key).encode()).decode())

        manifest = {"seed": self.seed, "difficulty": self.difficulty, "people": len(people),
                    "identity_records": len(idents), "accounts": len(accounts), "ledger_v0_rows": len(base),
                    "days": DAYS, "commits": sum(len(d["commits"]) for d in days),
                    "shard_records": shard_n, "shard_files": len(files), "shard_truth": shard_truth}
        with open(path("manifest.json"), "w") as f:
            json.dump(manifest, f, indent=1)
        return {"manifest": manifest, "key": key, "people": [asdict(p) for p in people],
                "identities": idents, "accounts": accounts, "base": base, "days": days}


def generate(out: str, seed: int = 2077, difficulty: str = "corpo") -> dict:
    return BreachGenerator(seed, difficulty).write(out)


def unseal(path: str) -> dict:
    with open(path) as f:
        return json.loads(base64.b64decode(f.read()))
