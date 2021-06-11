alter table place
add column delete_me boolean;
update place
set delete_me = id in (
        select pl.id
        from place pl
            left join place_to_policy p2p on p2p.place = pl.id
        where p2p.policy is null
            and level = 'Local'
    );
delete from place
where delete_me;
alter table place drop column delete_me;