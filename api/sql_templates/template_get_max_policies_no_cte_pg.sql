select pdd.day_date,
    pl.id,
    count(*) as num_policies
from policy p
    join policy_by_group_number pbgn on pbgn.fk_policy_id = p.id
    join policy_day_dates pdd on pdd.fk_policy_id = p.id
    join place_to_policy p2p on p2p.policy = p.id
    join place pl on pl.id = p2p.place
where %(place_filters_sql) s %(policy_filters_sql) s
group by pdd.day_date,
    pl.id
order by num_policies desc
limit 1;