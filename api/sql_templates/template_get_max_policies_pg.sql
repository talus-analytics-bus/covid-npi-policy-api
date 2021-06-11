with filtered_policies as (
    select distinct on (p.fk_policy_id) p.*,
        pl.id as loc_id
    from policy_by_group_number p
        join place_to_policy p2p on p2p.policy = p.fk_policy_id
        join place pl on pl.id = p2p.place
        join policy pol on pol.id = p.fk_policy_id
    where %(place_filters_sql) s %(policy_filters_sql) s
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