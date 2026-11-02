"""Очистка и валидация сырых событий. Чистые функции на pandas — их легко тестировать
отдельно от Airflow и ClickHouse (см. tests/test_transform.py)."""
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import pandas as pd

from etl.source import EVENT_TYPES

VALID_PRODUCTS = set(EVENT_TYPES)
VALID_EVENT_TYPES = {e for events in EVENT_TYPES.values() for e in events}
LOCAL_TZ = "Europe/Moscow"


@dataclass
class TransformReport:
    """Сколько строк отброшено и почему — уходит в лог задачи и в таблицу etl_runs."""

    rows_in: int = 0
    duplicates: int = 0
    missing_user: int = 0
    unknown_product: int = 0
    unknown_event_type: int = 0
    wrong_date: int = 0
    rows_out: int = 0
    notes: list = field(default_factory=list)

    @property
    def drop_share(self) -> float:
        return 1 - self.rows_out / self.rows_in if self.rows_in else 0.0


def read_jsonl(path: Path) -> pd.DataFrame:
    return pd.read_json(path, lines=True, dtype={"event_id": str, "user_id": "Int64", "account_id": "Int64"})


def clean_events(raw: pd.DataFrame, ds: date) -> tuple[pd.DataFrame, TransformReport]:
    rep = TransformReport(rows_in=len(raw))
    df = raw.copy()

    # 1. Дубликаты — по event_id (клиент мог отправить событие дважды)
    before = len(df)
    df = df.drop_duplicates(subset="event_id", keep="first")
    rep.duplicates = before - len(df)

    # 2. Без пользователя событие бесполезно для DAU/MAU
    mask = df["user_id"].isna()
    rep.missing_user = int(mask.sum())
    df = df[~mask]

    # 3. Нормализуем продукт и отбрасываем неизвестные значения
    df["product"] = df["product"].astype(str).str.strip().str.lower()
    mask = ~df["product"].isin(VALID_PRODUCTS)
    rep.unknown_product = int(mask.sum())
    df = df[~mask]

    mask = ~df["event_type"].isin(VALID_EVENT_TYPES)
    rep.unknown_event_type = int(mask.sum())
    df = df[~mask]

    # 4. Время: UTC → Москва, дата события по московскому времени
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True).dt.tz_convert(LOCAL_TZ)
    df["event_date"] = df["event_time"].dt.date
    mask = df["event_date"] != ds
    rep.wrong_date = int(mask.sum())
    df = df[~mask]

    df = df.assign(
        # В ClickHouse колонка DateTime('Europe/Moscow') — с точностью до секунды
        event_time=df["event_time"].dt.tz_localize(None).dt.floor("s"),
        account_id=df["account_id"].astype("uint32"),
        user_id=df["user_id"].astype("uint64"),
    )
    rep.rows_out = len(df)
    cols = ["event_date", "event_time", "event_id", "account_id", "user_id", "product", "event_type", "platform"]
    return df[cols].reset_index(drop=True), rep


def validate(df: pd.DataFrame, rep: TransformReport, max_drop_share: float = 0.1, min_rows: int = 1000) -> None:
    """Ворота качества: если данных подозрительно мало или отброшено слишком много,
    задача падает, и в DWH не попадает мусор. Лучше упавший DAG, чем тихо неверный дашборд."""
    if len(df) < min_rows:
        raise ValueError(f"Слишком мало строк после очистки: {len(df)} < {min_rows}")
    if rep.drop_share > max_drop_share:
        raise ValueError(f"Отброшено {rep.drop_share:.1%} строк — больше порога {max_drop_share:.0%}")
    if df["event_id"].duplicated().any():
        raise ValueError("После очистки остались дубликаты event_id")
