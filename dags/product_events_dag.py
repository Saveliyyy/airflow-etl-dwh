"""Ежедневная загрузка продуктовых событий в DWH на ClickHouse.

    extract → transform_and_validate → load_raw → build_marts → dq_checks

- Каждый запуск обрабатывает ровно один логический день ({{ ds }}), поэтому DAG можно
  перезапустить за любую дату или догрузить историю (catchup / airflow dags backfill).
- Между задачами передаётся путь к файлу, а не сами данные: XCom не для больших объёмов.
- Загрузка идемпотентна: партиция дня удаляется и записывается заново.
"""
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow.decorators import dag, task

DATA_DIR = Path("/opt/airflow/data")

default_args = {
    "owner": "analytics",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="product_events_daily",
    schedule="0 3 * * *",  # 03:00 UTC = 06:00 МСК, когда события за вчера уже собраны
    start_date=datetime(2026, 10, 1),
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=["dwh", "clickhouse", "product"],
    doc_md=__doc__,
)
def product_events_daily():
    @task
    def extract(ds: str = None) -> str:
        from etl.source import generate_day

        path = generate_day(date.fromisoformat(ds), DATA_DIR / "landing")
        return str(path)

    @task
    def transform_and_validate(raw_path: str, ds: str = None) -> dict:
        from etl.transform import clean_events, read_jsonl, validate

        df, rep = clean_events(read_jsonl(Path(raw_path)), date.fromisoformat(ds))
        validate(df, rep)

        staging = DATA_DIR / "staging" / f"events_{ds}.parquet"
        staging.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(staging, index=False)

        stats = {k: v for k, v in rep.__dict__.items() if k != "notes"}
        print(f"Очистка {ds}: {stats}, отброшено {rep.drop_share:.2%}")
        return {"path": str(staging), **stats}

    @task
    def load_raw(info: dict, ds: str = None) -> int:
        from etl.clickhouse import get_client, load_raw as load

        client = get_client()
        df = pd.read_parquet(info["path"])
        n = load(client, df, ds)
        client.insert(
            "dwh.etl_runs",
            [[date.fromisoformat(ds), info["rows_in"], info["rows_out"], info["duplicates"], info["missing_user"],
              info["unknown_product"], info["unknown_event_type"], info["wrong_date"]]],
            column_names=["ds", "rows_in", "rows_out", "duplicates", "missing_user", "unknown_product",
                          "unknown_event_type", "wrong_date"],
        )
        return n

    @task
    def build_marts(_: int, ds: str = None) -> None:
        from etl.clickhouse import get_client, run_sql_file

        run_sql_file(get_client(), "build_marts.sql", ds=ds)

    @task
    def dq_checks(ds: str = None) -> None:
        from etl.clickhouse import get_client, run_sql_file

        results = run_sql_file(get_client(), "dq_checks.sql", ds=ds)
        failed = [name for name, ok in results if not ok]
        for name, ok in results:
            print(f"{'OK  ' if ok else 'FAIL'} {name}")
        if failed:
            raise ValueError(f"Не пройдены проверки качества: {failed}")

    raw = extract()
    info = transform_and_validate(raw)
    loaded = load_raw(info)
    marts = build_marts(loaded)
    marts >> dq_checks()


product_events_daily()
