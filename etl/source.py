"""Имитация источника: ежедневная выгрузка сырых событий продукта в JSON Lines.

В реальности это был бы API, S3 или Kafka. Здесь генерируем файл за день и специально
добавляем типичные проблемы сырых данных, которые ETL должен разобрать:
  - дубликаты событий (повторная отправка с клиента)
  - пропуски user_id
  - продукт в разном регистре и с пробелами ("Mail ", "MAIL")
  - неизвестные типы событий
  - время в UTC, а аналитике нужна дата по Москве
"""
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np

PRODUCTS = ["mail", "calendar", "messenger", "vks"]
EVENT_TYPES = {
    "mail": ["mail_open", "mail_send"],
    "calendar": ["event_create", "event_view"],
    "messenger": ["message_send", "chat_open"],
    "vks": ["call_start", "call_join"],
}
PLATFORMS = ["desktop", "web", "ios", "android"]


def generate_day(ds: date, out_dir: Path, n_events: int = 20_000, seed: int | None = None) -> Path:
    rng = np.random.default_rng(seed if seed is not None else int(ds.strftime("%Y%m%d")))
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"events_{ds.isoformat()}.jsonl"

    # События дня по Москве: с 21:00 UTC предыдущего дня до 21:00 UTC текущего,
    # плюс немного «опоздавших» событий за прошлые сутки — их ETL должен отбросить.
    start = datetime(ds.year, ds.month, ds.day, tzinfo=timezone.utc) - timedelta(hours=3)
    rows = []
    for i in range(n_events):
        product = rng.choice(PRODUCTS, p=[0.45, 0.2, 0.25, 0.1])
        late = rng.random() < 0.01
        offset = rng.uniform(-86_400, 0) if late else rng.uniform(0, 86_400)
        ts = start + timedelta(seconds=float(offset))
        account_id = int(rng.integers(1, 300))
        user_id = account_id * 10_000 + int(rng.integers(0, 50))
        rows.append(
            {
                "event_id": f"{ds:%Y%m%d}-{i:07d}",
                "event_time": ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "account_id": account_id,
                "user_id": None if rng.random() < 0.005 else user_id,
                "product": str(product),
                "event_type": str(rng.choice(EVENT_TYPES[product])),
                "platform": str(rng.choice(PLATFORMS)),
            }
        )

    # Грязь
    for r in rng.choice(rows, size=len(rows) // 50, replace=False):
        r["product"] = rng.choice([r["product"].upper(), r["product"].title() + " ", f" {r['product']}"])
    for r in rng.choice(rows, size=len(rows) // 200, replace=False):
        r["event_type"] = "debug_ping"
    duplicates = [dict(r) for r in rng.choice(rows, size=len(rows) // 100, replace=False)]

    with path.open("w", encoding="utf-8") as f:
        for r in rows + duplicates:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    return path
