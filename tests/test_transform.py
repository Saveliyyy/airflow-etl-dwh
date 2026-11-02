from datetime import date

import pandas as pd
import pytest

from etl.source import generate_day
from etl.transform import TransformReport, clean_events, read_jsonl, validate

DS = date(2026, 10, 15)


def make_raw(rows):
    return pd.DataFrame(rows).astype({"user_id": "Int64", "account_id": "Int64"})


def event(event_id, time="2026-10-15T09:00:00Z", user_id=10001, product="mail", event_type="mail_open"):
    return {
        "event_id": event_id,
        "event_time": time,
        "account_id": 1,
        "user_id": user_id,
        "product": product,
        "event_type": event_type,
        "platform": "web",
    }


def test_duplicates_removed():
    df, rep = clean_events(make_raw([event("a"), event("a"), event("b")]), DS)
    assert len(df) == 2
    assert rep.duplicates == 1


def test_missing_user_dropped():
    df, rep = clean_events(make_raw([event("a", user_id=None), event("b")]), DS)
    assert list(df["event_id"]) == ["b"]
    assert rep.missing_user == 1


def test_product_normalized():
    df, _ = clean_events(make_raw([event("a", product=" MAIL "), event("b", product="Mail")]), DS)
    assert set(df["product"]) == {"mail"}


def test_unknown_values_dropped():
    raw = make_raw([event("a", product="drive"), event("b", event_type="debug_ping"), event("c")])
    df, rep = clean_events(raw, DS)
    assert list(df["event_id"]) == ["c"]
    assert rep.unknown_product == 1 and rep.unknown_event_type == 1


def test_moscow_date_boundary():
    # 22:30 UTC 14 октября = 01:30 МСК 15 октября → входит в день
    # 21:30 UTC 15 октября = 00:30 МСК 16 октября → не входит
    raw = make_raw(
        [
            event("in", time="2026-10-14T22:30:00Z"),
            event("out", time="2026-10-15T21:30:00Z"),
        ]
    )
    df, rep = clean_events(raw, DS)
    assert list(df["event_id"]) == ["in"]
    assert rep.wrong_date == 1
    assert df["event_time"].iloc[0] == pd.Timestamp("2026-10-15 01:30:00")


def test_validate_rejects_high_drop_share():
    df = pd.DataFrame({"event_id": [str(i) for i in range(2000)]})
    rep = TransformReport(rows_in=4000, rows_out=2000)
    with pytest.raises(ValueError, match="Отброшено"):
        validate(df, rep)


def test_end_to_end_on_generated_day(tmp_path):
    path = generate_day(DS, tmp_path, n_events=5000, seed=1)
    df, rep = clean_events(read_jsonl(path), DS)
    validate(df, rep)
    assert df["event_id"].is_unique
    assert set(df["product"]) <= {"mail", "calendar", "messenger", "vks"}
    assert (df["event_date"] == DS).all()
    assert rep.duplicates > 0 and rep.wrong_date > 0
    assert rep.rows_in == rep.rows_out + rep.duplicates + rep.missing_user + rep.unknown_product \
        + rep.unknown_event_type + rep.wrong_date
