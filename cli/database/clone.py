import os
import subprocess
import logging
from typing import Union

import click

from cli import options

logger = logging.getLogger(__name__)


@click.command(
    "clone-from-cloud",
    help="Clones the cloud database into a local database for development",
)
@click.option(
    "--dbname-cloud",
    "-dc",
    type=str,
    help="Cloud PostgreSQL database name (to copy from)",
)
@click.option(
    "--username-local",
    "-u",
    default=os.getenv("PG_LOCAL_USERNAME"),
    help="Local PostgreSQL database server username",
)
@click.option(
    "--dbname-local",
    "-d",
    default=os.getenv("PG_LOCAL_DBNAME"),
    help="Local PostgreSQL database name (to copy into from cloud)",
)
@options.yes
def clone_from_cloud(
    dbname_cloud: str,
    username_local: Union[str, None],
    dbname_local: Union[str, None],
    yes: bool,
):
    if username_local is None:
        raise ValueError(
            "Define local PostgreSQL username in option --username-local/-u or in"
            " environment variable `PG_LOCAL_USERNAME`"
        )
    if dbname_local is None:
        raise ValueError(
            "Define local PostgreSQL database name (to copy into from cloud) in option"
            " --dbname-local/-d or in environment variable `PG_LOCAL_DBNAME`"
        )
    if dbname_cloud is None:
        raise ValueError(
            "Define cloud PostgreSQL database name (to copy from) in option"
            " --dbname-cloud/-dc"
        )
    if not yes:
        click.confirm(
            "This will copy data from the cloud database"
            f" `{dbname_cloud}` to your local database named `{dbname_local}`,"
            " overwriting it completely.\nDo you want to continue?",
            abort=True,
        )
    logger.info(
        f"Copying data into local database `{dbname_local}` from cloud database"
        f" `{dbname_cloud}`"
    )
    subprocess.run(
        [
            "bash",
            "cli/database/sh/clone.sh",
            username_local,
            dbname_cloud,
            dbname_local,
        ]
    )