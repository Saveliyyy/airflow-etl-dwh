-- Пересчёт витрин за один день. {ds} подставляется из DAG.
-- Порядок: удалить партицию дня → вставить заново. Перезапуск даёт тот же результат.

ALTER TABLE dwh.mart_daily_product DROP PARTITION '{ds}';

INSERT INTO dwh.mart_daily_product
SELECT
    event_date,
    product,
    platform,
    uniqExact(user_id)            AS dau,
    uniqExact(account_id)         AS active_accounts,
    count()                       AS events,
    round(count() / uniqExact(user_id), 2) AS events_per_user
FROM dwh.raw_events
WHERE event_date = '{ds}'
GROUP BY event_date, product, platform;

ALTER TABLE dwh.mart_daily_users_state DROP PARTITION '{ds}';

INSERT INTO dwh.mart_daily_users_state
SELECT event_date, product, uniqState(user_id)
FROM dwh.raw_events
WHERE event_date = '{ds}'
GROUP BY event_date, product;
