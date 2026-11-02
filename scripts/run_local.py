"""Те же шаги, что в DAG, но без Airflow — для быстрой проверки на своей машине.

    docker compose up -d clickhouse
    python scripts/run_local.py 2026-10-01 2026-10-07
"""
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from etl.clickhouse import get_client, load_raw, run_sql_file  # noqa: E402
from etl.source import generate_day  # noqa: E402
from etl.transform import clean_events, read_jsonl, validate  # noqa: E402


def run_day(client, ds: date) -> None:
    raw_path = generate_day(ds, ROOT / "data" / "landing")
    df, rep = clean_events(read_jsonl(raw_path), ds)
    validate(df, rep)
    load_raw(client, df, ds.isoformat())
    run_sql_file(client, "build_marts.sql", ds=ds.isoformat())
    checks = run_sql_file(client, "dq_checks.sql", ds=ds.isoformat())
    status = "OK" if all(ok for _, ok in checks) else "FAIL"
    print(f"{ds}: загружено {rep.rows_out:,}, отброшено {rep.drop_share:.1%}, проверки {status}")


def main() -> None:
    start = date.fromisoformat(sys.argv[1])
    end = date.fromisoformat(sys.argv[2]) if len(sys.argv) > 2 else start
    client = get_client()
    run_sql_file(client, "schema.sql")
    d = start
    while d <= end:
        run_day(client, d)
        d += timedelta(days=1)


if __name__ == "__main__":
    main()
