from api.ampresolvers.core import PolicyStatusCounter
import datetime
from pony.orm.core import db_session
from api.util import use_relpath
from db import db
from api.models import PlaceObs, PlaceObsList

db.generate_mapping(create_tables=False)


def test_countries():
    compare_max(
        sql_fn="test_get_policy_counts_by_date_countries.sql",
        geo_res="country",
        by_group_number=True,
    )
    compare_max(
        sql_fn="test_get_policy_counts_by_date_countries_no_merge.sql",
        geo_res="country",
        by_group_number=False,
    )


def test_states():
    compare_max(
        sql_fn="test_get_policy_counts_by_date_states.sql",
        geo_res="state",
        by_group_number=True,
    )
    compare_max(
        sql_fn="test_get_policy_counts_by_date_states_no_merge.sql",
        geo_res="state",
        by_group_number=False,
    )


def test_counties():
    compare_max(
        sql_fn="test_get_policy_counts_by_date_counties.sql",
        geo_res="county",
        by_group_number=True,
    )
    compare_max(
        sql_fn="test_get_policy_counts_by_date_counties_no_merge.sql",
        geo_res="county",
        by_group_number=False,
    )


@db_session
def compare_max(sql_fn: str, geo_res: str, by_group_number: bool) -> None:
    with open(
        use_relpath(sql_fn, __file__),
        "r",
    ) as raw_sql:
        cursor = db.execute(raw_sql.read())
        rows = cursor.fetchall()
        counter: PolicyStatusCounter = PolicyStatusCounter()
        res: PlaceObsList = counter.get_policy_status_counts(
            geo_res=geo_res,
            filters={
                "primary_ph_measure": [
                    "Vaccinations",
                    "Military mobilization",
                    "Social distancing",
                ]
            },
            by_group_number=by_group_number,
        )
        max: PlaceObs = res.max_all_time
        day_date, iso3, value = get_fields_from_placeobs(max)
        assert len(rows) == 2
        assert rows[0] == (day_date, iso3, value)


def get_fields_from_placeobs(obs):
    day_date: datetime.date = obs.datestamp
    iso3: str = obs.place_name
    value: int = obs.value
    return day_date, iso3, value
