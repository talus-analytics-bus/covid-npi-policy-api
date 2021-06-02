alter table datetime
add column dt_date date;
update datetime
set dt_date = dt::date;