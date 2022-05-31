from api.types import GeoRes
from api.ampresolvers.policystatuscounter.helpers import StaticMaxMinCounter
from typing import Dict, Tuple
from api.ampresolvers import PolicyStatusCounter
import datetime
from pony.orm.core import db_session
from api.utils import use_relpath
from db import db
from api.models import PlaceObs, PlaceObsList

db.generate_mapping(create_tables=False)


def test_countries():
    compare_max(
        sql_fn="test_get_policy_counts_by_date_countries.sql",
        geo_res=GeoRes.country,
        by_group_number=True,
    )


def test_states():
    compare_max(
        sql_fn="test_get_policy_counts_by_date_states.sql",
        geo_res=GeoRes.state,
        by_group_number=True,
    )


def test_counties():
    compare_max(
        sql_fn="test_get_policy_counts_by_date_counties.sql",
        geo_res=GeoRes.county,
        by_group_number=True,
    )


def test_counties_plus_states():
    compare_max(
        sql_fn="test_get_policy_counts_by_date_counties_plus_states.sql",
        geo_res=GeoRes.county_plus_state,
        by_group_number=True,
    )


def test_min_max_counter():
    """
    Perform rationality checks on max/min number of policies in effect in
    any given location on any given date.

    """
    counter: StaticMaxMinCounter = StaticMaxMinCounter()
    max_min_counts: Dict[
        GeoRes, Tuple[PlaceObs, PlaceObs]
    ] = counter.get_max_min_counts()

    county_max: int = max_min_counts[GeoRes.county][0].value
    county_plus_state_max: int = max_min_counts[GeoRes.county_plus_state][0].value
    state_max: int = max_min_counts[GeoRes.state][0].value
    global_max: int = max_min_counts[GeoRes.country][0].value
    assert county_max <= county_plus_state_max
    assert state_max <= county_plus_state_max
    assert county_max > 0
    assert state_max > 0
    assert county_plus_state_max > 0
    assert global_max > 0


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
            filters={},
            by_group_number=by_group_number,
        )
        max: PlaceObs = res.max_all_time
        day_date, place_id, value = get_fields_from_placeobs(max)

        assert len(rows) == 1
        assert rows[0] == (day_date, place_id, value)


def get_fields_from_placeobs(obs: PlaceObs) -> Tuple[datetime.date, int, int]:
    """Given a place observation return its values.

    Args:
        obs (PlaceObs): The place observation

    Returns:
        Tuple[datetime.date, int, int]: Its date, ID, and value.
    """
    day_date: datetime.date = obs.datestamp
    id: int = obs.place_id
    value: int = obs.value
    return day_date, id, value
