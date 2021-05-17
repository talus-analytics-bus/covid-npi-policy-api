-- with policy_group as (
--     select *
--     from policy
-- ) 
with policy_group as (
    select distinct on (group_number) *
    from "policy"
    order by group_number,
        id
)
select count(distinct p.group_number)
from policy_group p
    join place_to_policy p2p on p2p.policy = p.id
    join place pl on pl.id = p2p.place
    join policy_date pd on pd.fk_policy_id = p.id
where pl.level = 'State / Province'
    and pl.area1 = 'New Jersey'
    and pl.iso3 = 'USA'
    and p.date_start_effective is not null
    and p.primary_ph_measure is not null
    and DATE '2021-05-10' between pd.start_date and pd.end_date;