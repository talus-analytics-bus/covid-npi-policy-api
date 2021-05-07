"""
Methods to ingest global country-level COVID data into the Talus
Metrics database.

"""
import pprint
from typing import Dict
from alive_progress import alive_bar
from datetime import datetime, date
from pony.orm.core import Database, commit
import db_metric
from ingest.util import upsert, jhu_caseload_csv_to_dict

# pretty printing: for printing JSON objects legibly
pp = pprint.PrettyPrinter(indent=4)


def upsert_jhu_country_covid_data(
    db: Database,
    db_amp: Database,
    all_dt_dict: Dict[str, db_metric.models.DateTime],
):
    """Upsert JHU country-level COVID caseload data and derived metrics for
    the USA.

    Args:
        db (Database): Metrics database connection (PonyORM)

        db_amp (Database): COVID AMP database connection (PonyORM)

        all_dt_dict (Dict[str, db_metric.models.DateTime]): Lookup table by
        date string of Metrics database datetime records
    """
    print("\nFetching data from JHU GitHub...")
    download_url = (
        "https://raw.githubusercontent.com/CSSEGISandData"
        "/COVID-19/master/csse_covid_19_data/"
        "csse_covid_19_time_series/"
        "time_series_covid19_confirmed_global.csv"
    )
    download_url_deaths = (
        "https://raw.githubusercontent.com/"
        "CSSEGISandData/COVID-19/master/csse_covid_19_data/"
        "csse_covid_19_time_series/"
        "time_series_covid19_deaths_global.csv"
    )
    data = jhu_caseload_csv_to_dict(download_url, db)
    data_deaths = jhu_caseload_csv_to_dict(download_url_deaths, db)
    print("Done.")

    print("\nUpserting relevant metrics...")

    # upsert metric for daily US caseload
    _action, covid_total_cases_countries = upsert(
        db.Metric,
        {
            "metric_name": "covid_total_cases_countries",
            "metric_id": 75,
        },
        {
            "temporal_resolution": "daily",
            "spatial_resolution": "country",
            "spatial_extent": "planet",
            "min_time": "2020-01-01",
            "max_time": "2025-01-01",
            "unit_type": "count",
            "unit": "cases",
            "num_type": "int",
            "metric_definition": "The total cumulative number of "
            "COVID-19 cases by date and country",
        },
    )
    commit()

    # upsert metric for daily US NEW caseload
    upsert(
        db.Metric,
        {"metric_name": "covid_new_cases_countries", "metric_id": 76},
        {
            "temporal_resolution": "daily",
            "spatial_resolution": "country",
            "spatial_extent": "planet",
            "min_time": "2020-01-01",
            "max_time": "2025-01-01",
            "unit_type": "count",
            "unit": "cases",
            "num_type": "int",
            "metric_definition": "The number of new COVID-19 cases by"
            " date and country",
            "is_view": True,
            "view_name": "metric_76",
        },
    )
    commit()

    # upsert metric for 7-day US NEW caseload
    upsert(
        db.Metric,
        {
            "metric_name": "covid_new_cases_countries_7d",
            "metric_id": 77,
        },
        {
            "temporal_resolution": "daily",
            "spatial_resolution": "country",
            "spatial_extent": "planet",
            "min_time": "2020-01-01",
            "max_time": "2025-01-01",
            "unit_type": "count",
            "unit": "cases",
            "num_type": "int",
            "metric_definition": "The number of new COVID-19 cases in"
            " the last 7 days by date and country",
            "is_view": True,
            "view_name": "metric_77",
        },
    )
    commit()

    # upsert metric for daily US deaths
    _action, covid_total_deaths_countries = upsert(
        db.Metric,
        {
            "metric_name": "covid_total_deaths_countries",
            "metric_id": 95,
        },
        {
            "temporal_resolution": "daily",
            "spatial_resolution": "country",
            "spatial_extent": "planet",
            "min_time": "2020-01-01",
            "max_time": "2025-01-01",
            "unit_type": "count",
            "unit": "deaths",
            "num_type": "int",
            "metric_definition": "The total cumulative number of "
            "COVID-19 deaths by date and country",
        },
    )
    commit()

    print("Done.")

    print("\nUpserting observations...")

    updated_at = datetime.now()
    last_datum_date = None
    n_cases = len(data)
    with alive_bar(
        n_cases, title="Importing national-level cases data"
    ) as bar:
        for d in data:
            bar()
            dt = None
            try:
                dt = all_dt_dict[d["date"]]
            except Exception:
                input("error: missing dt. Press enter to continue.")
                continue

            last_datum_date = d["date"]
            upsert(
                db.Observation,
                {
                    "metric": covid_total_cases_countries,
                    "date_time": dt["dt_id"],
                    "place": d["place"],
                    "data_source": "JHU CSSE COVID-19 Dataset",
                },
                {
                    "value": d["value"],
                    "updated_at": updated_at,
                },
            )

    n_deaths = len(data_deaths)
    with alive_bar(
        n_deaths, title="Importing national-level deaths data"
    ) as bar:
        for d in data_deaths:
            bar()
            dt = None
            try:
                dt = all_dt_dict[d["date"]]
            except Exception:
                input("error: missing dt. Press enter to continue.")
                continue

            last_datum_date = d["date"]
            upsert(
                db.Observation,
                {
                    "metric": covid_total_deaths_countries,
                    "date_time": dt["dt_id"],
                    "place": d["place"],
                    "data_source": "JHU CSSE COVID-19 Dataset",
                },
                {
                    "value": d["value"],
                    "updated_at": updated_at,
                },
            )

    # update version
    upsert(
        db_amp.Version,
        {
            "type": "COVID-19 case data (countries)",
        },
        {
            "date": date.today(),
            "last_datum_date": last_datum_date,
        },
    )

    print("Done.")
