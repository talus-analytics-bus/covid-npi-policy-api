import os
import subprocess
import logging
from typing import Union

import click

from cli import options

logger = logging.getLogger(__name__)


@click.command(
    "restore-to-cloud",
    help="Restores the local database to the cloud for use by cloud applications.",
)
@click.option(
    "--dbname-cloud",
    "-dc",
    type=str,
    help="Cloud PostgreSQL database name (to copy to)",
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
    help="Local PostgreSQL database name (to copy from)",
)
@options.awseb_restart
@options.yes
def restore_to_cloud(
    dbname_cloud: str,
    username_local: Union[str, None],
    dbname_local: Union[str, None],
    awseb_environment_name: Union[str, None],
    awseb_environment_region: Union[str, None],
    yes: bool,
):
    do_restore_to_cloud(
        dbname_cloud,
        username_local,
        dbname_local,
        awseb_environment_name,
        awseb_environment_region,
        yes,
    )


def do_restore_to_cloud(
    dbname_cloud,
    username_local,
    dbname_local,
    awseb_environment_name,
    awseb_environment_region,
    yes,
):
    # validate
    options.validate_awseb_restart_ops(awseb_environment_name, awseb_environment_region)

    if username_local is None:
        raise ValueError(
            "Define local PostgreSQL username in option --username-local/-u or in"
            " environment variable `PG_LOCAL_USERNAME`"
        )
    if dbname_local is None:
        raise ValueError(
            "Define local PostgreSQL database name (to copy to cloud) in option"
            " --dbname-local/-d"
        )
    if dbname_cloud is None:
        raise ValueError(
            "Define cloud PostgreSQL database name (to copy to) in option"
            " --dbname-cloud/-dc"
        )
    if not yes:
        click.confirm(
            f"This will copy data from your local `{dbname_local}` to the cloud"
            f" database named `{dbname_cloud}`, overwriting it completely.\nDo"
            " you want to continue?",
            abort=True,
        )
    logger.info(
        f"Restoring data from local database `{dbname_local}` to cloud database"
        f" `{dbname_cloud}`"
    )
    subprocess.run(
        [
            "bash",
            "cli/database/sh/restore.sh",
            username_local,
            dbname_cloud,
            dbname_local,
        ]
    )
