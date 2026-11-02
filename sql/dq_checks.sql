-- Проверки качества после загрузки дня {ds}. Каждая строка: имя проверки и ok (1/0).

SELECT 'raw_not_empty' AS check_name, count() > 0 AS ok
FROM dwh.raw_events WHERE event_date = '{ds}';

SELECT 'raw_no_duplicate_event_id' AS check_name, count() = uniqExact(event_id) AS ok
FROM dwh.raw_events WHERE event_date = '{ds}';

SELECT 'mart_matches_raw' AS check_name,
       (SELECT sum(events) FROM dwh.mart_daily_product WHERE event_date = '{ds}')
     = (SELECT count() FROM dwh.raw_events WHERE event_date = '{ds}') AS ok;

-- DAU не должен упасть больше чем вдвое к тому же дню недели неделей раньше (если он есть)
SELECT 'dau_no_sharp_drop' AS check_name,
       (SELECT uniqExact(user_id) FROM dwh.raw_events WHERE event_date = toDate('{ds}') - 7) = 0
    OR (SELECT uniqExact(user_id) FROM dwh.raw_events WHERE event_date = '{ds}')
     >= 0.5 * (SELECT uniqExact(user_id) FROM dwh.raw_events WHERE event_date = toDate('{ds}') - 7) AS ok;
