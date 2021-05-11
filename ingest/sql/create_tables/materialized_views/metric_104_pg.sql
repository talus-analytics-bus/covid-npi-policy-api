WITH obs AS (
    SELECT d.dt_date - '1900-01-01'::date AS days_since_epoch,
        d.dt_date - '1900-01-01'::date AS start,
        d.dt_date - '1900-01-07'::date AS "end",
        o.metric_id,
        o.value,
        o.data_source,
        o.updated_at,
        o.place_id,
        o.datetime_id,
        o.observation_id
    FROM metric_103 o
        JOIN datetime d ON o.datetime_id = d.dt_id
)
SELECT 104 AS metric_id,
    sum(b.value) AS value,
    a.data_source,
    a.updated_at,
    a.observation_id,
    a.datetime_id,
    a.place_id
FROM obs a
    JOIN obs b ON b.days_since_epoch >= a."end"
    AND b.days_since_epoch <= a.start
    AND a.place_id = b.place_id
GROUP BY a.observation_id,
    a.data_source,
    a.updated_at,
    a.datetime_id,
    a.place_id;