# ETL продуктовых событий: Airflow → ClickHouse DWH

Ежедневный пайплайн, который забирает сырые события продукта, очищает их, загружает в хранилище на ClickHouse, строит витрины для аналитики и проверяет качество данных.

## Схема

```
источник (JSON Lines за день)
   │  extract
   ▼
transform_and_validate ── дубликаты, пропуски, регистр, неизвестные значения, UTC → МСК
   │                       ворота качества: мало строк или >10% отброшено → задача падает
   ▼
load_raw ─────────────── dwh.raw_events (партиция = день, перезапись партиции)
   │                      dwh.etl_runs (журнал: сколько пришло и почему отброшено)
   ▼
build_marts ──────────── dwh.mart_daily_product (DAU, клиенты, события по продукту и платформе)
   │                      dwh.mart_daily_users_state (uniqState для честного WAU/MAU)
   ▼
dq_checks ────────────── витрина = сырым данным, нет дублей, DAU не упал вдвое к прошлой неделе
```

## Что здесь показано

- **Airflow**: TaskFlow API, расписание, `catchup` для догрузки истории, ретраи, передача пути к файлу вместо данных через XCom
- **Идемпотентность**: каждый день перезаписывает свою партицию, поэтому перезапуск DAG за любую дату не задваивает данные
- **DWH-слои**: raw → mart, `AggregatingMergeTree` с `uniqState`, чтобы скользящий MAU считался из дневной витрины (`sql/mau_from_mart.sql`)
- **Качество данных**: валидация до загрузки и SQL-проверки после неё, журнал отброшенных строк
- **Тесты**: очистка вынесена в чистые функции на pandas и покрыта `pytest`, включая границу суток по московскому времени

## Структура

```
dags/product_events_dag.py   DAG
etl/source.py                имитация источника с «грязными» данными
etl/transform.py             очистка и валидация
etl/clickhouse.py            выполнение SQL и загрузка в ClickHouse
sql/schema.sql               таблицы хранилища
sql/build_marts.sql          пересчёт витрин за день
sql/dq_checks.sql            проверки качества
sql/mau_from_mart.sql        скользящий MAU из витрины
scripts/run_local.py         те же шаги без Airflow
tests/test_transform.py      тесты очистки
```

## Запуск

Через Airflow:

```bash
docker compose up -d
docker compose logs airflow | grep -i password   # пароль admin для http://localhost:8080
```

В интерфейсе включите DAG `product_events_daily`. С `catchup=True` он сам догрузит все дни с 1 октября 2026.

Без Airflow:

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
docker compose up -d clickhouse
python scripts/run_local.py 2026-10-01 2026-10-07
pytest
```
