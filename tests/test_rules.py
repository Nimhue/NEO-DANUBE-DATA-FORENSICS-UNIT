from dfu.forensics.matching import levenshtein, norm_name, score_pair, soundex
from dfu.forensics.tamper import flags_for_change, score


def test_name_normalization_handles_order_accents_and_case():
    assert norm_name("Kovács, Mira") == norm_name("MIRA KOVACS") == "kovacs mira"


def test_levenshtein():
    assert levenshtein("kitten", "sitting") == 3
    assert levenshtein("", "abc") == 3


def test_soundex_matches_spark_convention():
    assert soundex("Robert") == "R163"
    assert soundex("Tymczak") == "T522"


def test_alias_with_swapped_dob_still_matches():
    a = {"full_name": "Mira Kovács", "dob": "2041-03-07", "citizen_id": "ND-12345678"}
    b = {"full_name": "Kovacs, Mira", "dob": "2041-07-03", "citizen_id": "ND-12345679"}
    s = score_pair(a, b)
    assert s["dob"] == 0.7 and s["citizen_id"] == 0.8 and s["score"] >= 0.6


def test_lookalike_does_not_match():
    a = {"full_name": "Mira Kovács", "dob": "2041-03-07", "citizen_id": "ND-12345678", "district": "Pest Sprawl"}
    b = {"full_name": "Mirela Kovács", "dob": "2039-11-21", "citizen_id": "ND-87654321", "district": "Pest Sprawl"}
    assert score_pair(a, b)["score"] < 0.6


BASE = {"amount": 1234.56, "counterparty_account": "ACC-1", "created_at": "2077-02-01T10:00:00",
        "status": "settled", "modified_at": "2077-02-01T10:00:00"}


def test_night_shift_alone_is_not_tampering():
    post = dict(BASE, modified_at="2077-03-02T00:30:00", status="settled")
    f = flags_for_change("update", BASE, post)
    assert f == ["OFF_HOURS"] and score(f) < 2


def test_redirect_to_round_number_at_3am_is():
    post = dict(BASE, modified_at="2077-03-02T03:10:00", amount=10000.0, counterparty_account="ACC-666")
    f = flags_for_change("update", BASE, post)
    assert set(f) == {"OFF_HOURS", "ROUND_AMOUNT", "REDIRECT"} and score(f) == 4


def test_deleting_flagged_evidence():
    assert flags_for_change("delete", dict(BASE, status="flagged"), None) == ["EVIDENCE_DELETE"]
    assert flags_for_change("delete", dict(BASE, status="void"), None) == []
