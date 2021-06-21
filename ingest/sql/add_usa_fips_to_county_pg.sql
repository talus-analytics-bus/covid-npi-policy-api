update place
set iso3 = 'USA'
where level = 'Local plus state/province'
    and ansi_fips is not null
    and iso3 is null;