drop view if exists policy_day_dates;
drop materialized view if exists policy_day_dates;
create materialized view policy_day_dates as (
    select p.id as fk_policy_id,
        dd.day_date
    from policy p
        join policy_date pd on pd.fk_policy_id = p.id
        join day_date dd on dd.day_date between pd.start_date and pd.end_date
    order by dd.day_date
);
create index policy_day_dates_fk_policy_id_idx on policy_day_dates (fk_policy_id);
create index policy_day_dates_day_date_idx on policy_day_dates (day_date);