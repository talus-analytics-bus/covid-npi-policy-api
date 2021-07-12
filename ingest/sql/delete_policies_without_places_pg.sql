with policy_to_delete as (
    select id
    from policy p
        left join place_to_policy p2p on p2p.policy = p.id
    where p2p.policy is null
)
delete from policy
where id in (
        select id
        from policy_to_delete
    );