-- new cases by county by date
create or replace view metric_103 as(
        WITH obs AS (
            SELECT d.dt::date - '1900-01-01'::date AS days_since_epoch,
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
        SELECT 103 AS metric_id,
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
        WHERE a.metric_id = 102
            AND b.metric_id = 102
    );
-- 7d running sums
create MATERIALIZED view if not exists metric_104 as (
    SELECT 104 AS metric_id,
        o.data_source,
        o.updated_at,
        o.observation_id,
        o.datetime_id,
        o.place_id,
        p.name,
        sum(o.value) OVER (
            PARTITION BY o.place_id
            ORDER BY d.dt ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS value
    FROM metric_103 o
        JOIN datetime d ON o.datetime_id = d.dt_id
        JOIN place p ON p.place_id = o.place_id
    ORDER BY p.name,
        d.dt
);