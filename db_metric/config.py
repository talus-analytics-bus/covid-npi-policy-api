##
# API configuration file.
##

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
    database=conn_params["database_metric"],
)
