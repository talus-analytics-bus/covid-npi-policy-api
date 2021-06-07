create view policy_by_group_number as (
    select distinct on (min(id), group_number) min(id) as fk_policy_id,
        group_number as group_number
    from "policy"
    group by group_number
    order by min(id),
        group_number
)