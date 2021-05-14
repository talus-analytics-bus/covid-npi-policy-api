from typing import Tuple
from api.ampresolvers.core import PolicyStatusCounter
import datetime
from pony.orm.core import db_session
from api.util import use_relpath
from db import db
from api.models import PlaceObs

db.generate_mapping(create_tables=False)


@db_session
def test_countries():
    raw_sql: str = None
    with open(
        use_relpath("test_get_policy_counts_by_date_countries.sql", __file__),
        "r",
    ) as raw_sql:
        cursor = db.execute(raw_sql.read())
        rows = cursor.fetchall()
        counter: PolicyStatusCounter = PolicyStatusCounter()
        min_max_counts: Tuple[PlaceObs, PlaceObs] = counter.get_max_min_counts(
            filters_no_dates={
                "primary_ph_measure": ["Vaccinations", "Military mobilization"]
            },
            level="Country",
            loc_field="iso3",
        )
        max: PlaceObs = min_max_counts[0]
        day_date, iso3, value = get_fields_from_placeobs(max)
        assert len(rows) == 2
        assert rows[1] == (day_date, iso3, value)


def get_fields_from_placeobs(max):
    day_date: datetime.date = max.datestamp
    iso3: str = max.place_name
    value: int = max.value
    return day_date, iso3, value


@db_session
def test_states():
    raw_sql: str = None
    with open(
        use_relpath("test_get_policy_counts_by_date_states.sql", __file__),
        "r",
    ) as raw_sql:
        cursor = db.execute(raw_sql.read())
        rows = cursor.fetchall()
        counter: PolicyStatusCounter = PolicyStatusCounter()
        min_max_counts: Tuple[PlaceObs, PlaceObs] = counter.get_max_min_counts(
            filters_no_dates={
                "primary_ph_measure": [
                    "Vaccinations",
                    "Military mobilization",
                ],
                "iso3": ["USA"],
            },
            level="State / Province",
            loc_field="area1",
        )
        max: PlaceObs = min_max_counts[0]
        day_date, iso3, value = get_fields_from_placeobs(max)
        assert len(rows) == 2
        assert rows[1] == (day_date, iso3, value)
