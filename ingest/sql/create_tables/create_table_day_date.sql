create table day_date (day_date date primary key not null);
insert into day_date ("day_date")
select generate_series(
        '2019-01-01'::date,
        '2025-01-01'::date,
        '1 day'
    ) as "day_date";