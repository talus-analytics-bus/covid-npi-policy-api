with filtered_policies as (
    select p.id,
        pl.ansi_fips as "place_loc"
    from "policy" p
        join place_to_policy p2p on p2p.policy = p.id
        join place pl on pl.id = p2p.place
    where pl.level = 'Local'
        and pl.iso3 = 'USA'
        and (
            p.primary_ph_measure = 'Vaccinations'
            or p.primary_ph_measure = 'Military mobilization'
            or p.primary_ph_measure = 'Social distancing'
        )
    order by 2
),
date_counts as (
    select d.day_date::date as "date",
        fp."place_loc",
        count(pd.fk_policy_id) as "num_active_policies"
    from policy_date pd
        join day_date d on d.day_date between pd.start_date and pd.end_date
        join filtered_policies fp on fp.id = pd.fk_policy_id
    group by d.day_date,
        fp."place_loc"
) (
    select *
    from date_counts
    where "num_active_policies" = (
            select min("num_active_policies") as "num_active_policies"
            from date_counts
        )
    order by 3 desc,
        1 desc,
        2
    limit 1
)
UNION
(
    select *
    from date_counts
    where "num_active_policies" = (
            select max("num_active_policies") as "num_active_policies"
            from date_counts
        )
    order by 3 desc,
        1 desc,
        2
    limit 1
);