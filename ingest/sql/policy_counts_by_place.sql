-- sql to get counts by place
create view policy_counts_by_place as (select pl.level, pl.iso3, count(distinct p.id) as policy_count from policy p
join place_to_policy pp on p.id = pp.policy
join place pl on pl.id = pp.place
group by pl.level, pl.iso3
order by 1, 2);
