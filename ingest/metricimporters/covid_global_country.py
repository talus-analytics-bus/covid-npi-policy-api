"""
Methods to ingest global country-level COVID data into the Talus
Metrics database.

"""
from collections import defaultdict
import pprint
from typing import Dict
from alive_progress import alive_bar
from datetime import datetime, date
from pony.orm.core import Database, commit, db_session, select
import requests
import db_metric
from ingest.util import upsert

# pretty printing: for printing JSON objects legibly
pp = pprint.PrettyPrinter(indent=4)


def upsert_jhu_country_covid_data(
    db: Database,
    db_amp: Database,
    all_dt_dict: Dict[str, db_metric.models.DateTime],
):
    """Upsert JHU country-level COVID caseload data and derived metrics for
    global countries.

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

    # upsert metric for daily country caseload
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

    # upsert metric for daily country NEW caseload
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

    # upsert metric for 7-day country NEW caseload
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

    # upsert metric for daily country deaths
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


@db_session
def jhu_caseload_csv_to_dict(download_url: str, db):

    r = requests.get(download_url, allow_redirects=True)
    rows = r.iter_lines(decode_unicode=True)

    # remove the header row from the generator
    headers_raw = next(rows).split(",")
    dates_raw = headers_raw[4:]
    dates = list()
    for d in dates_raw:
        date_parts = d.split("/")
        mm = date_parts[0] if len(date_parts[0]) == 2 else "0" + date_parts[0]
        dd = date_parts[1] if len(date_parts[1]) == 2 else "0" + date_parts[1]
        yyyy = (
            date_parts[2] if len(date_parts[2]) == 4 else "20" + date_parts[2]
        )
        date_str = yyyy + "-" + mm + "-" + dd
        dates.append(date_str)

    headers = headers_raw[0:4] + dates
    skip = ("Lat", "Long", "Province/State")

    # keep dictionary of special row lists that need to be summed
    special_country_rows = defaultdict(list)
    row_lists = list()
    for row in rows:
        row_list = row.split(",")
        if row_list[1] in (
            "Australia",
            "China",
            "Canada",
        ):
            special_country_rows[row_list[1]].append(row_list)
        # elif row_list[1] in (
        #     "England",
        #     "Northern Ireland",
        #     "Wales",
        #     "Scotland",
        # ):
        #     # if row is home nation of UK, recode it as a country
        #     # TODO check this
        #     new_row_list = row_list.copy()
        #     new_row_list[1] = new_row_list[0]
        #     new_row_list[0] = ""
        #     row_lists.append(row_list)
        #     row_lists.append(new_row_list)
        else:
            row_lists.append(row_list)

    # condense special country rows into single rows
    new_row_lists = list()
    for place_name in special_country_rows:
        row_list = ["", place_name, "lat", "lon"]
        L = special_country_rows[place_name]

        # Using naive method to sum list of lists
        # Source: https://www.geeksforgeeks.org/python-ways-to-sum-list-of-
        # lists-and-return-sum-list/
        res = list()
        for j in range(0, len(L[0][4:])):
            tmp = 0
            for i in range(0, len(L)):
                tmp = tmp + int(L[i][4:][j])
            res.append(tmp)
        row_list += res
        new_row_lists.append(row_list)

    row_lists += new_row_lists

    missing_names = set()
    data = list()
    for row_list in row_lists:
        if row_list[0] != "":
            continue

        datum = dict()
        idx = 0
        for header in headers:
            if header in skip:
                idx += 1
                continue
            else:
                if header == "Country/Region":
                    datum["name"] = (
                        row_list[idx].replace('"', "").replace("*", "")
                    )

                else:
                    datum[header] = int(float(row_list[idx]))
                idx += 1

        # get place ISO, ID from name
        p = select(
            i
            for i in db.Place
            if i.name == datum["name"] or datum["name"] in i.other_names
        ).first()
        if p is None:
            missing_names.add(datum["name"])
            continue
        else:
            datum["place"] = p

        # reshape again
        for cur_date in dates:
            datum_final = dict()
            datum_final["date"] = cur_date
            datum_final["value"] = datum[cur_date]
            datum_final["place"] = datum["place"]
            data.append(datum_final)

    print(
        "These places in the JHU dataset were missing from the COVID AMP "
        "places database:"
    )
    pp.pprint(missing_names)

    # return output
    return data
