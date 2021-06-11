from ingest.util import upsert
from ingest.plugins import get_place_loc
from typing import List
from alive_progress import alive_bar
from pony.orm.core import BindingError, commit, db_session, select, get
from db_metric.models import Place as MetricPlace
from db.models import Place as AmpPlace
from db import db
from db_metric import db as db_metric


def add_missing_usa_local_areas():
    """If missing, adds a place for each USA local area represented in the
    Metric database that is not already in the AMP database

    """
    # Prepare database connections
    try:
        db.generate_mapping()
    except BindingError:
        pass
    try:
        db_metric.generate_mapping()
    except BindingError:
        pass

    # Get all county places from metrics database
    metric_counties: List[MetricPlace] = select(
        i
        for i in MetricPlace
        if i.place_type == "county" and i.fips is not None
    )

    # Get all local places from AMP database
    amp_counties: List[AmpPlace] = select(
        i for i in AmpPlace if i.level == "Local" and i.iso3 == "USA"
    )[:][:]

    # For each county place from metrics database
    metric_county: MetricPlace = None
    with alive_bar(
        len(metric_counties), title="Adding missing counties to AMP database"
    ) as bar:
        for metric_county in metric_counties:
            # If it is not in the AMP database, add it
            bar()
            missing: bool = not any(
                x
                for x in amp_counties
                if int(x.ansi_fips) == int(metric_county.fips)
            )
            if missing:
                metric_state_name: str = metric_county.description
                new_amp_place: AmpPlace = AmpPlace(
                    level="Local",
                    iso3="USA",
                    country_name="United States of America (USA)",
                    area1=metric_state_name,
                    area2=metric_county.name + ", " + metric_state_name,
                    ansi_fips=metric_county.fips,
                )
                new_amp_place.loc = get_place_loc(new_amp_place)
                commit()
                print("Added AMP place for " + new_amp_place.loc)


@db_session
def add_local_plus_state_places():
    """If missing, adds a local level place linked to both local and state
    policies for each USA county.

    """
    # get all USA counties
    # Prepare database connections
    try:
        db.generate_mapping()
    except BindingError:
        pass

    # Get all local places from AMP database
    counties: List[AmpPlace] = select(
        i for i in AmpPlace if i.level == "Local" and i.iso3 == "USA"
    )[:][:]

    # for each county, get its state
    county_place: AmpPlace = None
    with alive_bar(
        len(counties), title="Adding local plus state/province places"
    ) as bar:
        for county_place in counties:
            bar()
            state_place: AmpPlace = get(
                i
                for i in AmpPlace
                if i.area1 == county_place.area1
                and i.level == "State / Province"
            )
            # upsert place that is level "Local plus state / province"
            county_plus_state_place: AmpPlace = None
            _action, county_plus_state_place = upsert(
                AmpPlace,
                {
                    "level": "Local plus state/province",
                    "ansi_fips": county_place.ansi_fips,
                },
                {
                    "area1": county_place.area1,
                    "area2": county_place.area2,
                    "loc": county_place.loc,
                    "country_name": county_place.country_name,
                },
            )

            # link all policies
            county_plus_state_place.policies = (
                county_place.policies + state_place.policies
            )

        commit()
