from api.routing import GeoRes
from typing import Tuple
from api.ampresolvers.core import PolicyStatusCounter
import datetime
from pony.orm.core import db_session
from api.util import use_relpath
from db import db
from api.models import PlaceObs

db.generate_mapping(create_tables=False)


def test_countries():
    compare_max(
        sql_fn="test_get_policy_counts_by_date_countries.sql",
        level="Country",
        loc_field="iso3",
        by_group_number=True,
    )
    compare_max(
        sql_fn="test_get_policy_counts_by_date_countries_no_merge.sql",
        level="Country",
        loc_field="iso3",
        by_group_number=False,
    )


def test_states():
    compare_max(
        sql_fn="test_get_policy_counts_by_date_states.sql",
        level="State / Province",
        loc_field="area1",
        by_group_number=True,
    )
    compare_max(
        sql_fn="test_get_policy_counts_by_date_states_no_merge.sql",
        level="State / Province",
        loc_field="area1",
        by_group_number=False,
    )


def test_counties():
    compare_max(
        sql_fn="test_get_policy_counts_by_date_counties.sql",
        level="Local",
        loc_field="area2",
        by_group_number=True,
    )
    compare_max(
        sql_fn="test_get_policy_counts_by_date_counties_no_merge.sql",
        level="Local",
        loc_field="area2",
        by_group_number=False,
    )


@db_session
def compare_max(
    sql_fn: str, level: str, loc_field: str, by_group_number: bool
) -> None:
    with open(
        use_relpath(sql_fn, __file__),
        "r",
    ) as raw_sql:
        cursor = db.execute(raw_sql.read())
        rows = cursor.fetchall()
        counter: PolicyStatusCounter = PolicyStatusCounter()
        min_max_counts: Tuple[PlaceObs, PlaceObs] = counter.get_max_min_counts(
            geo_res=GeoRes.country,
            filters_no_dates={
                "primary_ph_measure": [
                    "Vaccinations",
                    "Military mobilization",
                    "Social distancing",
                ]
            },
            level=level,
            loc_field=loc_field,
            by_group_number=by_group_number,
        )
        max: PlaceObs = min_max_counts[1]
        day_date, iso3, value = get_fields_from_placeobs(max)
        assert len(rows) == 2
        assert rows[1] == (day_date, iso3, value)


def get_fields_from_placeobs(max):
    day_date: datetime.date = max.datestamp
    iso3: str = max.place_name
    value: int = max.value
    return day_date, iso3, value
