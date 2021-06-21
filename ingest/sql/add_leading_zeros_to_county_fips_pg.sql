update place
set ansi_fips = '0' || ansi_fips
where level = 'Local plus state/province'
    and length(ansi_fips) = 4;
update place
set ansi_fips = '0' || ansi_fips
where level = 'Local'
    and length(ansi_fips) = 4