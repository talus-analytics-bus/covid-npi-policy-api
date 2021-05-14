create view policy_date as (
    select id as "fk_policy_id",
        date_start_effective as "start_date",
        COALESCE(
            COALESCE(date_end_actual, date_end_anticipated),
            current_date
        ) as "end_date"
    from "policy"
)