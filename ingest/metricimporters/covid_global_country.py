"""
Methods to ingest global country-level COVID data into the Talus
Metrics database.

"""


import logging
import requests
import csv
import io
from datetime import datetime, date, timedelta
from collections import defaultdict
from typing import Any, DefaultDict, Dict, Iterator, List, Set, Tuple, Union
from unicodedata import numeric

from tqdm import tqdm
from pony.orm.core import Database, commit, db_session, select
from requests.models import Response

import db_metric
from ingest.util import upsert
from db_metric.models import Place
from ingest.metricimporters.helpers import get_place_from_name

# logger
logger: logging.Logger = logging.getLogger(__name__)


def upsert_jhu_country_covid_data(
    db: Database,
    db_amp: Database,
    all_dt_dict: Dict[str, db_metric.models.DateTime],
    do_global: bool = True,
    do_global_daily: bool = True,
):
    """Upsert JHU country-level COVID caseload data and derived metrics for
    global countries.

    Args:
        db (Database): Metrics database connection (PonyORM)

        db_amp (Database): COVID AMP database connection (PonyORM)

        all_dt_dict (Dict[str, db_metric.models.DateTime]): Lookup table by
        date string of Metrics database datetime records

        do_global (bool, optional): If true, ingest COVID-19 case and
        death data for for countries from time series report (single file)

        do_global_daily (bool, optional): If true, ingest COVID-19 case and
        death data for select states/provinces from the JHU CSSE daily reports.
        Note: This is currently used to ingest data for countries that
        comprise the United Kingdom and only ingests data for those locations.
        If you want to ingest data for all countries, ensure `do_global` is True.

    """
    logger.info("\nFetching data from JHU GitHub...")
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
    download_url_daily: str = (
        "https://raw.githubusercontent.com/"
        "CSSEGISandData/COVID-19/master/"
        "csse_covid_19_data/csse_covid_19_daily_reports/"
    )
    data: list = list()
    data_deaths: list = list()
    if do_global:
        data = jhu_caseload_csv_to_dict(download_url, db)
        data_deaths = jhu_caseload_csv_to_dict(download_url_deaths, db)

    # concatenate data from daily reports, if ingesting
    if do_global_daily:
        data_daily: list = list()
        data_deaths_daily: list = list()
        [data_daily, data_deaths_daily] = jhu_daily_csv_to_dict(
            download_url_daily,
            db,
            province_names={
                "Scotland",
                "Wales",
                "Northern Ireland",
                "England",
            },
        )
        data += data_daily
        data_deaths += data_deaths_daily
    logger.info("Done.")
    logger.info("\nUpserting relevant metrics...")

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

    logger.info("Done.")

    logger.info("\nUpserting observations...")

    updated_at = datetime.now()
    last_datum_date = None
    logger.info("Importing national-level cases data")
    for d in tqdm(data):
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

    logger.info("Importing national-level deaths data")
    for d in tqdm(data_deaths):
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
            "name": "COVID-19 case data (countries)",
        },
        {
            "map_types": "{global}",
            "date": date.today(),
            "last_datum_date": last_datum_date,
        },
    )

    logger.info("Done.")


def jhu_daily_csv_to_dict(
    download_url_daily: str,
    db: Database,
    province_names: Set[str],
) -> Tuple[List[dict], List[dict]]:
    """Get JHU country-level COVID caseload data and derived metrics for
    global countries from daily reports. This approach is needed to get data
    from UK home nations England, Wales, Northern Ireland, and Scotland, since
    JHU does not publish their COVID-19 data in the composite CSV file of all
    dates.

    Args:
        download_url_daily (str): The base URL for the download location of
        daily CSV report files.

        db (Database): Metrics database connection (PonyORM)

        db_amp (Database): COVID AMP database connection (PonyORM)

        all_dt_dict (Dict[str, db_metric.models.DateTime]): Lookup table by
        date string of Metrics database datetime records

        loc_names (Set[str]): The names of the provinces for which data should
        be extracted and represented at the country level.

    Returns:
        Tuple[List[dict], List[dict]]: (1) List of case data records and (2)
        list of death data records.
    """

    # For each date within the date range provided (YYYY-MM-DD)
    date_range: Tuple[date, date] = (date(2020, 1, 27), date.today())

    dates_to_check: List[date] = list()
    cur_date: date = date_range[0]
    while cur_date <= date_range[1]:
        dates_to_check.append(cur_date)
        cur_date = cur_date + timedelta(days=1)

    data: List[Dict[str, Union[str | numeric]]] = list()
    data_deaths: List[Dict[str, Union[str | numeric]]] = list()
    missing_place_names: Set[str] = set()
    date_to_check: date = None
    logger.info(
        "Importing national-level cases and deaths data from daily reports"
    )
    for date_to_check in tqdm(dates_to_check):
        url_date_str: str = date_to_check.strftime("%m-%d-%Y")

        # Fetch the daily CSV file from JHU GitHub
        cur_download_url: str = download_url_daily + url_date_str + ".csv"
        r: Response = requests.get(cur_download_url, allow_redirects=True)
        # rows: Iterator = r.iter_lines(decode_unicode=True)
        rows = csv.reader(io.StringIO(r.content.decode()))

        # extract header row from iterator
        # headers: List[str] = next(rows).split(",")
        headers: List[str] = next(rows)

        # For each location needed
        row: str = None
        for row in rows:

            # row_values: List[str] = next(csv.reader([row]))
            row_dict: Dict[str, str] = dict()
            header: str = None
            idx: int = None
            for idx, header in enumerate(headers):
                row_dict[header] = row[idx]

            province_state: str = row_dict.get("Province_State", None)
            if province_state in province_names:
                # Get the "confirmed" case value
                # name, date, value place
                datum: Dict[str, Union[str | numeric]] = dict(
                    date=str(date_to_check)
                )
                datum_deaths: Dict[str, Union[str | numeric]] = dict(
                    date=str(date_to_check)
                )
                datum["value"] = get_int_or_none(row_dict, "Confirmed")
                datum_deaths["value"] = get_int_or_none(row_dict, "Deaths")
                datum["name"] = province_state
                datum_deaths["name"] = province_state
                place: Place = get_place_from_name(db, province_state)

                if place is None:
                    missing_place_names.add(province_state)
                else:
                    datum["place"] = place
                    datum_deaths["place"] = place
                    data.append(datum)
                    data_deaths.append(datum_deaths)

    # List missing place names
    if len(missing_place_names) > 0:
        print_missing_place_names(missing_place_names)

    # Return the output
    return (data, data_deaths)


def get_int_or_none(row_dict: dict, key: str) -> int:
    """Returns the value for the defined `key` in `row_dict` as an integer or
    None if it does not exist.

    Args:
        row_dict (dict): A dict of data
        key (str): A key to check

    Returns:
        int: The int-parsed value of the key or None
    """
    val_str: str = row_dict.get(key, None)
    if val_str is not None:
        return int(val_str)
    else:
        return None


@db_session
def jhu_caseload_csv_to_dict(
    download_url: str, db: Database
) -> Dict[str, Any]:
    """Returns a dictionary of COVID-19 case data from the JHU GitHub, given
    the download URL and the database object.

    Args:
        download_url (str): The download URL for a CSV file of COVID-19 case
        data by country by date

        db (Database): The PonyORM database object.

    Returns:
        Dict[str, Any]: A dictionary of COVID-19 case data by country by date
    """

    # get data from download URL
    r: Response = requests.get(download_url, allow_redirects=True)
    rows: Iterator = r.iter_lines(decode_unicode=True)

    # remove the header row from the generator
    headers_raw: List[str] = next(rows).split(",")
    dates_raw: List[str] = headers_raw[4:]
    dates: List[str] = list()
    d: str = None
    for d in dates_raw:
        date_parts: List[str] = d.split("/")
        mm: str = (
            date_parts[0] if len(date_parts[0]) == 2 else "0" + date_parts[0]
        )
        dd: str = (
            date_parts[1] if len(date_parts[1]) == 2 else "0" + date_parts[1]
        )
        yyyy: str = (
            date_parts[2] if len(date_parts[2]) == 4 else "20" + date_parts[2]
        )
        date_str: str = yyyy + "-" + mm + "-" + dd
        dates.append(date_str)

    headers: List[str] = headers_raw[0:4] + dates
    skip: Tuple[str, str, str] = ("Lat", "Long", "Province/State")

    # keep dictionary of special row lists that need to be summed
    # TODO confirm this approach is necessary
    special_country_rows: DefaultDict[str, list] = defaultdict(list)
    row_lists: list = list()
    row: str = None
    for row in rows:
        row_list: List[str] = row.split(",")
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

    missing_place_names: Set[str] = set()
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
            missing_place_names.add(datum["name"])
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

    if len(missing_place_names) > 0:
        print_missing_place_names(missing_place_names)

    # return output
    return data


def print_missing_place_names(missing_place_names):
    logger.warning(
        "These places in the JHU dataset were missing from the COVID AMP "
        "places database:"
    )
    for name in missing_place_names:
        logger.warning(name)
