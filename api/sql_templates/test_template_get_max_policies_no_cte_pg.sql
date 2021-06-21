select pdd.day_date,
    pl.id,
    count(*) as num_policies
from policy p
    join policy_by_group_number pbgn on pbgn.fk_policy_id = p.id
    join policy_day_dates pdd on pdd.fk_policy_id = p.id
    join place_to_policy p2p on p2p.policy = p.id
    join place pl on pl.id = p2p.place
where pl.level = 'State / Province'
    and pl.iso3 = 'USA'
    and (
        p.primary_ph_measure = 'Vaccinations'
        or p.primary_ph_measure = 'Military mobilization'
        or p.primary_ph_measure = 'Social distancing'
    )
group by pdd.day_date,
    pl.id
order by num_policies desc
limit 1;
-- select distinct p.fk_policy_id
-- from policy_by_group_number p
--     join place_to_policy p2p on p2p.policy = p.fk_policy_id
--     join policy_day_dates pdd on pdd.fk_policy_id = p.fk_policy_id
-- where p2p.place = 106479
--     and pdd.day_date = DATE '2021-05-10';