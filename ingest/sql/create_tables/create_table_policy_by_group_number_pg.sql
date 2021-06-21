drop view if exists policy_by_group_number;
drop materialized view if exists policy_by_group_number;
create materialized view policy_by_group_number as (
    select distinct on (min(id), group_number) min(id) as fk_policy_id,
        group_number as group_number
    from "policy"
    group by group_number
    order by min(id),
        group_number
);
create index policy_fk_policy_id on policy_by_group_number (fk_policy_id);
create index policy_group_number_idx on policy_by_group_number (group_number);