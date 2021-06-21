SELECT 77 AS metric_id,
    o.data_source,
    o.updated_at,
    o.observation_id,
    o.datetime_id,
    o.place_id,
    sum(o.value) OVER (
        PARTITION BY o.place_id
        ORDER BY dt.dt_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS value
FROM metric_76 o
    JOIN datetime dt ON dt.dt_id = o.datetime_id
ORDER BY o.place_id,
    dt.dt_date;