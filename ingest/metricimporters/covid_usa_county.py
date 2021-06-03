"""
Methods to ingest USA county-level COVID data into the Talus
Metrics database.

"""
import db_metric
from db.models import Version
from ingest.util import nyt_county_caseload_csv_to_dict, upsert

import pprint
from typing import Dict, Set
from alive_progress import alive_bar
from datetime import datetime, date, timedelta
from pony.orm.core import Database, commit, select, get

# pretty printing: for printing JSON objects legibly
pp = pprint.PrettyPrinter(indent=4)


def upsert_nyt_county_covid_data(
    db: Database,
    db_amp: Database,
    all_dt_dict: Dict[str, db_metric.models.DateTime],
):
    """Upsert NYT county-level COVID caseload data and derived metrics for
    the USA.

    Args:
        db (Database): Metrics database connection (PonyORM)

        db_amp (Database): COVID AMP database connection (PonyORM)

        all_dt_dict (Dict[str, db_metric.models.DateTime]): Lookup table by
        date string of Metrics database datetime records
    """
    print("\nFetching county-level data from New York Times GitHub...")
    download_url = (
        "https://raw.githubusercontent.com/nytimes/covid-19-data"
        "/master/us-counties.csv"
    )

    # determine most recent data date in database and only request data for
    # newer dates
    for_dates: Set[str] = None
    data_version: Version = get(
        i for i in db_amp.Version if i.type == "COVID-19 county case data"
    )
    for_dates = set()

    most_recent_data_date: date = (
        data_version.last_datum_date
        if (
            data_version is not None
            and data_version.last_datum_date is not None
        )
        else date(2020, 1, 27)
    )

    # get strings of dates between most recent data date and present day
    today: date = date.today()
    iter_date: date = most_recent_data_date

    while iter_date != today:
        for_dates.add(str(iter_date))
        iter_date += timedelta(days=1)

    data = nyt_county_caseload_csv_to_dict(download_url, for_dates=for_dates)
    print("Done.")

    print("\nUpserting relevant metrics...")

    # upsert metric for daily US caseload
    _action, covid_total_cases_counties = upsert(
        db.Metric,
        {
            "metric_id": 102,
        },
        {
            "metric_name": "covid_total_cases_counties",
            "temporal_resolution": "daily",
            "spatial_resolution": "county",
            "spatial_extent": "country",
            "min_time": "2020-01-01",
            "max_time": "2025-01-01",
            "unit_type": "count",
            "unit": "cases",
            "num_type": "int",
            "metric_definition": "The total cumulative number of COVID-19"
            " cases by date and USA county",
        },
    )
    commit()

    # upsert metric for daily US deaths
    _action, covid_total_deaths_counties = upsert(
        db.Metric,
        {
            "metric_id": 122,
        },
        {
            "metric_name": "covid_total_deaths_counties",
            "temporal_resolution": "daily",
            "spatial_resolution": "county",
            "spatial_extent": "country",
            "min_time": "2020-01-01",
            "max_time": "2025-01-01",
            "unit_type": "count",
            "unit": "deaths",
            "num_type": "int",
            "metric_definition": "The total cumulative number of COVID-19"
            " deaths by date and county",
        },
    )
    commit()

    # upsert metric for daily US NEW caseload
    upsert(
        db.Metric,
        {"metric_id": 103},
        {
            "metric_name": "covid_new_cases_counties",
            "temporal_resolution": "daily",
            "spatial_resolution": "county",
            "spatial_extent": "country",
            "min_time": "2020-01-01",
            "max_time": "2025-01-01",
            "unit_type": "count",
            "unit": "cases",
            "num_type": "int",
            "metric_definition": "The number of new COVID-19 cases by date"
            " and county",
            "is_view": True,
            "view_name": "metric_103",
        },
    )
    commit()

    # upsert metric for daily US NEW deaths
    upsert(
        db.Metric,
        {"metric_id": 123},
        {
            "metric_name": "covid_new_cases_counties",
            "temporal_resolution": "daily",
            "spatial_resolution": "county",
            "spatial_extent": "country",
            "min_time": "2020-01-01",
            "max_time": "2025-01-01",
            "unit_type": "count",
            "unit": "deaths",
            "num_type": "int",
            "metric_definition": "The number of new COVID-19 deaths by date"
            " and county",
            "is_view": True,
            "view_name": "metric_123",
        },
    )
    commit()

    # upsert metric for 7-day US NEW caseload
    upsert(
        db.Metric,
        {
            "metric_id": 104,
        },
        {
            "metric_name": "covid_new_cases_counties_7d",
            "temporal_resolution": "daily",
            "spatial_resolution": "county",
            "spatial_extent": "country",
            "min_time": "2020-01-01",
            "max_time": "2025-01-01",
            "unit_type": "count",
            "unit": "cases",
            "num_type": "int",
            "metric_definition": "The number of new COVID-19 cases in the"
            " last 7 days by date and county",
            "is_view": True,
            "view_name": "metric_104",
        },
    )
    commit()

    # upsert metric for 7-day US NEW caseload
    action, covid_new_deaths_counties_7d = upsert(
        db.Metric,
        {
            "metric_id": 124,
        },
        {
            "metric_name": "covid_new_deaths_counties_7d",
            "temporal_resolution": "daily",
            "spatial_resolution": "county",
            "spatial_extent": "country",
            "min_time": "2020-01-01",
            "max_time": "2025-01-01",
            "unit_type": "count",
            "unit": "deaths",
            "num_type": "int",
            "metric_definition": "The number of new COVID-19 deaths in the"
            " last 7 days by date and county",
            "is_view": True,
            "view_name": "metric_124",
        },
    )
    commit()

    print("Done.")

    print("\nUpserting observations...")

    # get all counties indexed by FIPS
    all_places_list = select(
        (i.place_id, i.fips) for i in db.Place if i.place_type == "county"
    )[:][:]
    all_places_dict = {v[1]: v[0] for v in all_places_list}

    missing = set()
    updated_at = datetime.now()
    last_datum_date = None
    n = len(data.keys())
    with alive_bar(
        n, title="Importing county-level cases and deaths data"
    ) as bar:
        name_data: str = ""
        for name_data in data:
            bar()
            # prepend zeros
            name_db: str = name_data
            # if len(name_data) == 4:
            #     name_db = "0" + name_data
            # else:
            #     name_db = name_data
            place = all_places_dict.get(name_db, None)
            if place is None:
                missing.add(name_db)
                continue
            else:
                for d in data[name_data]:

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
                            "metric": covid_total_cases_counties,
                            "date_time": dt["dt_id"],
                            "place": place,
                            "data_source": "New York Times",  # TODO correct
                        },
                        {
                            "value": d["cases"],
                            "updated_at": updated_at,
                        },
                        do_commit=False,
                    )
                    upsert(
                        db.Observation,
                        {
                            "metric": covid_total_deaths_counties,
                            "date_time": dt["dt_id"],
                            "place": place,
                            "data_source": "New York Times",  # TODO correct
                        },
                        {
                            "value": d["deaths"],
                            "updated_at": updated_at,
                        },
                        do_commit=False,
                    )

    # commit data
    commit()

    # update version
    upsert(
        db_amp.Version,
        {
            "type": "COVID-19 county case data",
        },
        {
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
