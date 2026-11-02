-- Пример запроса к витрине: скользящий MAU (28 дней) по продуктам,
-- посчитанный из дневных состояний uniq без обращения к сырым событиям.
-- Каждый день «размножаем» на 28 дат, в окно которых он попадает, затем сливаем состояния.

SELECT
    window_end,
    product,
    uniqMerge(users) AS mau_28d
FROM
(
    SELECT
        event_date + n AS window_end,
        product,
        users
    FROM dwh.mart_daily_users_state
    ARRAY JOIN range(28) AS n
)
WHERE window_end <= (SELECT max(event_date) FROM dwh.mart_daily_users_state)
GROUP BY window_end, product
ORDER BY window_end, product;
