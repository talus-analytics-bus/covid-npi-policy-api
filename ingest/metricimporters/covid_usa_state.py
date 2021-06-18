"""
Methods to ingest USA state-level COVID data into the Talus
Metrics database.

"""
import pprint
from typing import Dict
from alive_progress import alive_bar
from datetime import datetime, date
from pony.orm.core import Database, commit, select
import db_metric
from ingest.util import nyt_caseload_csv_to_dict, upsert

# pretty printing: for printing JSON objects legibly
pp = pprint.PrettyPrinter(indent=4)


def upsert_nyt_state_covid_data(
    db: Database,
    db_amp: Database,
    all_dt_dict: Dict[str, db_metric.models.DateTime],
):
    """Upsert NYT state-level COVID caseload data and derived metrics for
    the USA.

    Args:
        db (Database): Metrics database connection (PonyORM)

        db_amp (Database): COVID AMP database connection (PonyORM)

        all_dt_dict (Dict[str, db_metric.models.DateTime]): Lookup table by
        date string of Metrics database datetime records
    """
    print("\nFetching data from New York Times GitHub...")
    download_url = (
        "https://raw.githubusercontent.com/nytimes/covid-19-data/"
        "master/us-states.csv"
    )
    data = nyt_caseload_csv_to_dict(download_url)
    print("Done.")

    print("\nUpserting relevant metrics...")

    # upsert metric for daily US caseload
    _action, covid_total_cases_provinces = upsert(
        db.Metric,
        {
            "metric_name": "covid_total_cases_provinces",
            "metric_id": 72,
        },
        {
            "temporal_resolution": "daily",
            "spatial_resolution": "state",
            "spatial_extent": "country",
            "min_time": "2020-01-01",
            "max_time": "2025-01-01",
            "unit_type": "count",
            "unit": "cases",
            "num_type": "int",
            "metric_definition": "The total cumulative number of COVID-19 "
            "cases by date and state / province",
        },
    )
    commit()

    # upsert metric for daily US deaths
    _action, covid_total_deaths_provinces = upsert(
        db.Metric,
        {
            "metric_name": "covid_total_deaths_provinces",
            "metric_id": 92,
        },
        {
            "temporal_resolution": "daily",
            "spatial_resolution": "state",
            "spatial_extent": "country",
            "min_time": "2020-01-01",
            "max_time": "2025-01-01",
            "unit_type": "count",
            "unit": "deaths",
            "num_type": "int",
            "metric_definition": "The total cumulative number of COVID-19 "
            "deaths by date and state / province",
        },
    )
    commit()

    # upsert metric for daily US NEW caseload
    upsert(
        db.Metric,
        {"metric_name": "covid_new_cases_provinces", "metric_id": 73},
        {
            "temporal_resolution": "daily",
            "spatial_resolution": "state",
            "spatial_extent": "country",
            "min_time": "2020-01-01",
            "max_time": "2025-01-01",
            "unit_type": "count",
            "unit": "cases",
            "num_type": "int",
            "metric_definition": "The number of new COVID-19 cases by date "
            "and state / province",
            "is_view": True,
            "view_name": "metric_73",
        },
    )
    commit()

    # upsert metric for daily US NEW deaths
    upsert(
        db.Metric,
        {"metric_name": "covid_new_deaths_provinces", "metric_id": 93},
        {
            "temporal_resolution": "daily",
            "spatial_resolution": "state",
            "spatial_extent": "country",
            "min_time": "2020-01-01",
            "max_time": "2025-01-01",
            "unit_type": "count",
            "unit": "deaths",
            "num_type": "int",
            "metric_definition": "The number of new COVID-19 deaths by date "
            "and state / province",
            "is_view": True,
            "view_name": "metric_93",
        },
    )
    commit()

    # upsert metric for 7-day US NEW caseload
    upsert(
        db.Metric,
        {
            "metric_name": "covid_new_cases_provinces_7d",
            "metric_id": 74,
        },
        {
            "temporal_resolution": "daily",
            "spatial_resolution": "state",
            "spatial_extent": "country",
            "min_time": "2020-01-01",
            "max_time": "2025-01-01",
            "unit_type": "count",
            "unit": "cases",
            "num_type": "int",
            "metric_definition": "The number of new COVID-19 cases in the "
            "last 7 days by date and state / province",
            "is_view": True,
            "view_name": "metric_74",
        },
    )
    commit()

    # upsert metric for 7-day US NEW caseload
    upsert(
        db.Metric,
        {
            "metric_name": "covid_new_deaths_provinces_7d",
            "metric_id": 94,
        },
        {
            "temporal_resolution": "daily",
            "spatial_resolution": "state",
            "spatial_extent": "country",
            "min_time": "2020-01-01",
            "max_time": "2025-01-01",
            "unit_type": "count",
            "unit": "deaths",
            "num_type": "int",
            "metric_definition": "The number of new COVID-19 deaths in the "
            "last 7 days by date and state / province",
            "is_view": True,
            "view_name": "metric_94",
        },
    )
    commit()

    print("Done.")

    print("\nUpserting observations...")

    # get all places indexed by name
    all_places_list = select((i.place_id, i.name) for i in db.Place)[:][:]
    all_places_dict = {v[1]: v[0] for v in all_places_list}

    missing = set()
    updated_at = datetime.now()
    last_datum_date = None
    n = len(data.keys())
    with alive_bar(
        n, title="Importing state-level cases and deaths data"
    ) as bar:
        for name in data:
            bar()
            place = all_places_dict.get(name, None)
            if place is None:
                missing.add(name)
                continue
            else:
                for d in data[name]:

                    dt = None
                    try:
                        dt = all_dt_dict[d["date"]]
                    except Exception:
                        print("error: missing dt")
                        # input('error: missing dt. Press enter to continue.')
                        continue

                    last_datum_date = d["date"]
                    upsert(
                        db.Observation,
                        {
                            "metric": covid_total_cases_provinces,
                            "date_time": dt["dt_id"],
                            "place": place,
                            "data_source": "New York Times",  # TODO correct
                        },
                        {
                            "value": d["cases"],
                            "updated_at": updated_at,
                        },
                    )
                    action, obs_affected_deaths = upsert(
                        db.Observation,
                        {
                            "metric": covid_total_deaths_provinces,
                            "date_time": dt["dt_id"],
                            "place": place,
                            "data_source": "New York Times",  # TODO correct
                        },
                        {
                            "value": d["deaths"],
                            "updated_at": updated_at,
                        },
                    )

    # update version for state data
    upsert(
        db_amp.Version,
        {
            "name": "COVID-19 state case data",
        },
        {
            "map_types": "{us,us-county,us-county-plus-state}",
            "date": date.today(),
            "last_datum_date": last_datum_date,
        },
    )

    # update version for overall COVID-19 case data, used when a general
    # "last updated date" for COVID-19 data is needed
    upsert(
        db_amp.Version,
        {
            "name": "COVID-19 case data",
        },
        {
            "map_types": "{}",
            "date": date.today(),
            "last_datum_date": last_datum_date,
        },
    )

    if len(missing) > 0:
        print(
            "These places in the NYT dataset were missing from the COVID AMP"
            " places database:"
        )
        pp.pprint(missing)

    print("Done.")
