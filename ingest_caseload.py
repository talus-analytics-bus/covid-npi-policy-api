"""Run caseload data ingest application"""
# standard modules
import argparse
import time
from typing import List, Optional

# local modules
from db_metric import db
from db import db as db_amp
from ingest.plugins import CovidCaseloadPlugin

# 3rd party modules
from pony.orm import db_session

# setup arguments
parser = argparse.ArgumentParser(description="Ingest caseload data")
parser.add_argument(
    "-s",
    "--state",
    default=False,
    action="store_const",
    const=True,
    help="ingest USA state data",
)
parser.add_argument(
    "-c",
    "--county",
    default=False,
    action="store_const",
    const=True,
    help="ingest USA county data",
)
parser.add_argument(
    "-g",
    "--globe",
    default=False,
    action="store_const",
    const=True,
    help="ingest global data",
)
parser.add_argument(
    "-a",
    "--all",
    default=False,
    action="store_const",
    const=True,
    help="ingest all data",
)
parser.add_argument(
    "-mv",
    "--materialized-views",
    default=False,
    action="store_const",
    const=True,
    help="refresh materialized views",
)


@db_session
def refresh_materialized_views():
    """Refresh materialized views that depend on case/deaths data."""

    # get connection to DBAPI
    conn = db.get_connection()
    cur = conn.cursor()

    # single statement to refresh all materialized views relevant
    to_refresh: List[str] = ["74", "77", "94"]
    stmt_list: List[str] = list()
    id: Optional[str]
    for id in to_refresh:
        stmt: str = f"""REFRESH MATERIALIZED VIEW metric_{id}"""
        stmt_list.append(stmt)
    stmt_str = "; ".join(stmt_list)

    print("\nRefreshing materialized views (x" + str(len(to_refresh)) + ")...")
    then = time.perf_counter()
    cur.execute(stmt_str)
    now = time.perf_counter()
    sec = now - then
    print("Refreshed. Time elapsed: " + str(sec))


if __name__ == "__main__":
    # get args
    args = parser.parse_args()
    do_county = args.county or args.all
    do_state = args.state or args.all
    do_global = args.globe or args.all
    do_refresh_materialized_views = (
        args.globe
        or args.state
        or args.county
        or args.materialized_views
        or args.all
    )

    # generate database mapping and ingest data for the COVID-AMP project
    db.generate_mapping(create_tables=False)
    db_amp.generate_mapping(create_tables=False)
    if do_state or do_global or do_county:
        plugin = CovidCaseloadPlugin()
        plugin.upsert_covid_data(
            db,
            db_amp,
            do_state=do_state,
            do_global=do_global,
            do_county=do_county,
        )

    # refresh materialized views that depend on case/deaths data
    if do_refresh_materialized_views:
        refresh_materialized_views()
    print("\nData ingested.")
