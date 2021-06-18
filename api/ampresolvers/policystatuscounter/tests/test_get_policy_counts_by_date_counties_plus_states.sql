with filtered_policies as (
    select p.*,
        pl.id as loc_id
    from policy_by_group_number p
        join place_to_policy p2p on p2p.policy = p.fk_policy_id
        join place pl on pl.id = p2p.place
    where level = 'Local plus state/province'
        and pl.iso3 = 'USA'
)
select pdd.day_date,
    p.loc_id,
    count(*)
from filtered_policies p
    join policy_day_dates pdd on pdd.fk_policy_id = p.fk_policy_id
group by pdd.day_date,
    p.loc_id
order by count desc
limit 1;