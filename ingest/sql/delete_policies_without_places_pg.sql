delete from policy
where id not in (
        select policy
        from place_to_policy
    );