-- Do not assign tribal nations to countries or provinces
update place
set iso3 = 'Unspecified',
    country_name = area1
where level = 'Tribal nation'