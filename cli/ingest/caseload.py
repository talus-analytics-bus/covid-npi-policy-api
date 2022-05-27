import os
from typing import Union

import click

from cli import options


DBNAME_CLOUD_DEFAULT: str = "metric-amp"


@click.command(
    help="Ingest caseload data from NYT and WHO and copy it to the"
    f" cloud database (by default named `{DBNAME_CLOUD_DEFAULT}`)"
)
@click.option(
    "--dbname-cloud",
    "-dc",
    default=os.getenv("DBNAME_CLOUD_METRICS", DBNAME_CLOUD_DEFAULT),
    show_default=True,
    type=str,
    help="Cloud PostgreSQL database name (to copy to). Can also be set with"
    " environment variable `DBNAME_CLOUD_METRICS`.",
)
@click.option(
    "--dbname-local",
    "-d",
    default=os.getenv("database_metric", "metric-amp-lo22cal"),
    help="Local PostgreSQL database name (to copy from). Defaults to"
    " `metric-amp-local`. Can be defined in environment variable `database_metric`.",
)
@options.username_local_op
@options.yes
@options.skip_restore
def caseload(
    dbname_cloud: str,
    username_local: Union[str, None],
    dbname_local: Union[str, None],
    yes: bool,
    skip_restore: bool,
):

    if not yes:
        click.confirm(
            "This will replace all COVID-19 caseload data in your local database"
            f" named `{dbname_local}` and cloud database named `{dbname_cloud}` with"
            " the latest from the data sources."
            "\nDo you want to continue?",
            abort=True,
        )

    # import packages and modules
    from . import dbutils
    from cli.database.restore import do_restore_to_cloud
    from db import db as db_amp
    from db_metric import db
    from ingest.plugins import CovidCaseloadPlugin

    # ingest the data
    covid_caseload_plugin = CovidCaseloadPlugin()

    db.generate_mapping(create_tables=False)
    db_amp.generate_mapping(create_tables=False)
    covid_caseload_plugin.upsert_covid_data(
        db,
        db_amp,
        do_state=True,
        do_global=True,
        do_global_daily=True,
        do_county=True,
    )

    # refresh materialized views that depend on case/deaths data
    dbutils.refresh_materialized_views()

    # restore database to cloud
    if not skip_restore:
        do_restore_to_cloud(
            dbname_cloud, username_local, dbname_local, None, None, yes=yes
        )
