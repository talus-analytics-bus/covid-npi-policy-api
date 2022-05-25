##
# API configuration file.
##

from typing import Union, List, Tuple

from pony import orm

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


def execute_raw_sql(statement: str) -> Union[List[Tuple], None]:
    """Runs the provided statement as raw SQL against the connected database.

    Args:
        statement (str): SQL statement string

    Returns:
        Union[List[Tuple], None]: Result set if any, otherwise None
    """
    cursor = db.execute(statement)
    if cursor.rowcount > -1:
        return [row for row in cursor]
    else:
        return None
