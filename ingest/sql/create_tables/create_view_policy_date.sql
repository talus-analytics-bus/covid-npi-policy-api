drop view if exists policy_date;
drop materialized view if exists policy_date;
create materialized view policy_date as (
    select id as "fk_policy_id",
        date_start_effective as "start_date",
        COALESCE(
            COALESCE(date_end_actual, date_end_anticipated),
            current_date
        ) as "end_date"
    from "policy"
);
create index policy_date_fk_policy_id_idx on policy_date (fk_policy_id);
create index policy_date_start_date_idx on policy_date (start_date);
create index policy_date_end_date_idx on policy_date (end_date);