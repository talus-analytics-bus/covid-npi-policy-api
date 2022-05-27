##
# API configuration file.
##

import os
from typing import Union, List, Tuple

from pony import orm
from pony.orm import db_session
import psycopg2

from . import configtools


# init PonyORM database instance
conn_params = configtools.Config()["db"]
db = orm.Database()

db.bind(
    "postgres",
    user=conn_params["username"],
    password=conn_params["password"],
    host=conn_params["host"],
    database=conn_params["database"],
)


@db_session
def execute_raw_sql(statement: str) -> Union[List[Tuple], None]:
    """Runs the provided statement as raw SQL against the connected database.

    Args:
        statement (str): SQL statement string

    Returns:
        Union[List[Tuple], None]: Result set if any, otherwise None
    """
    cursor = db.execute(statement)
    if cursor.rowcount > -1:
        try:
            return [row for row in cursor]
        except psycopg2.ProgrammingError:
            return None
    else:
        return None
