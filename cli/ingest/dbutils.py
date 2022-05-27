import time
from typing import List, Optional

from pony.orm import db_session

from db import db


@db_session
def refresh_materialized_views():
    """Refresh materialized views that depend on case/deaths data."""

    # get connection to DBAPI
    conn = db.get_connection()
    cur = conn.cursor()

    # single statement to refresh all materialized views relevant
    to_refresh: List[str] = ["73", "74", "77", "94", "104"]
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
