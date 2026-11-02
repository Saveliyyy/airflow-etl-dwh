-- Слои хранилища:
--   raw   — очищенные события как есть, одна партиция = один день
--   mart  — дневные агрегаты для дашбордов и продуктовой аналитики
--   etl_runs — журнал загрузок (сколько строк пришло и отброшено)
-- Каждая дневная загрузка сначала удаляет свою партицию, поэтому перезапуск DAG
-- за любой день идемпотентен: данные не задваиваются.

CREATE DATABASE IF NOT EXISTS dwh;

CREATE TABLE IF NOT EXISTS dwh.raw_events
(
    event_date  Date,
    event_time  DateTime('Europe/Moscow'),
    event_id    String,
    account_id  UInt32,
    user_id     UInt64,
    product     LowCardinality(String),
    event_type  LowCardinality(String),
    platform    LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY event_date
ORDER BY (product, account_id, user_id, event_time);

CREATE TABLE IF NOT EXISTS dwh.mart_daily_product
(
    event_date      Date,
    product         LowCardinality(String),
    platform        LowCardinality(String),
    dau             UInt64,
    active_accounts UInt64,
    events          UInt64,
    events_per_user Float64
)
ENGINE = MergeTree
PARTITION BY event_date
ORDER BY (event_date, product, platform);

-- Состояния uniq позволяют честно считать WAU/MAU из дневных строк (см. mau_from_mart.sql)
CREATE TABLE IF NOT EXISTS dwh.mart_daily_users_state
(
    event_date  Date,
    product     LowCardinality(String),
    users       AggregateFunction(uniq, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY event_date
ORDER BY (event_date, product);

CREATE TABLE IF NOT EXISTS dwh.etl_runs
(
    ds                  Date,
    loaded_at           DateTime DEFAULT now(),
    rows_in             UInt32,
    rows_out            UInt32,
    duplicates          UInt32,
    missing_user        UInt32,
    unknown_product     UInt32,
    unknown_event_type  UInt32,
    wrong_date          UInt32
)
ENGINE = MergeTree
ORDER BY (ds, loaded_at);
