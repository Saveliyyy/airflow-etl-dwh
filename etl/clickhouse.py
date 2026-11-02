"""Работа с ClickHouse: выполнение SQL-файлов с подстановкой даты и загрузка датафреймов."""
import os
from pathlib import Path

import pandas as pd

SQL_DIR = Path(__file__).resolve().parents[1] / "sql"


def get_client():
    import clickhouse_connect  # импорт внутри, чтобы transform можно было тестировать без драйвера

    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    )


def split_statements(sql: str) -> list[str]:
    """Делит файл на отдельные запросы: HTTP-интерфейс выполняет один запрос за раз.
    Комментарии убираем построчно, чтобы точка с запятой внутри них не ломала разбиение."""
    lines = [line for line in sql.splitlines() if not line.strip().startswith("--")]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def run_sql_file(client, name: str, **params) -> list:
    sql = (SQL_DIR / name).read_text(encoding="utf-8").format(**params)
    results = []
    for stmt in split_statements(sql):
        if stmt.lstrip().upper().startswith("SELECT"):
            results.extend(client.query(stmt).result_rows)
        else:
            client.command(stmt)
    return results


def load_raw(client, df: pd.DataFrame, ds: str) -> int:
    """Идемпотентная загрузка дня: удаляем партицию и вставляем заново."""
    client.command(f"ALTER TABLE dwh.raw_events DROP PARTITION '{ds}'")
    client.insert_df("dwh.raw_events", df)
    return len(df)
