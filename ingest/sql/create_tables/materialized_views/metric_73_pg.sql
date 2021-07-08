create materialized view metric_73 as (
    WITH obs AS (
        SELECT d.dt_date - '1900-01-01'::date AS days_since_epoch,
            o.metric_id,
            o.value,
            o.data_source,
            o.updated_at,
            o.place_id,
            o.datetime_id,
            o.observation_id
        FROM observation o
            JOIN datetime d ON o.datetime_id = d.dt_id
    )
    SELECT 73 AS metric_id,
        a.value - b.value AS value,
        a.data_source,
        a.updated_at,
        a.place_id,
        a.datetime_id,
        row_number() OVER (PARTITION BY true::boolean) AS observation_id
    FROM obs a
        JOIN obs b ON a.days_since_epoch = (b.days_since_epoch + 1)
        AND a.place_id = b.place_id
        AND a.data_source = b.data_source
    WHERE a.metric_id = 72
        AND b.metric_id = 72
);