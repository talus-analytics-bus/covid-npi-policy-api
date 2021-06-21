create table max_policy_count (
    max_policy_count_id integer primary key not null,
    map_type map_type not null,
    max_value integer not null
);
insert into max_policy_count (1, 'us', 209)