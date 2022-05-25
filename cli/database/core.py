import click

from .clone import clone_from_cloud
from .restore import restore_to_cloud


@click.group(
    help="Refresh database tables, restore to cloud, or clone from cloud"
)
def database():
    pass


@database.command(
    help="Refresh all materialized views, tables, and other entities that depend on"
    " ingested assistance data.\n\nThese entities are refreshed with on demand"
    " because they may take up to 30 minutes to refresh. This command should be run"
    " before viewing data in the frontend or analyzing it in the backend after an"
    " ingest operation has been performed."
    # help="Equivalent to running other refresh-* commands in this group, i.e.,"
    # " refreshes tables, materialized views, flow assignments data."
)
def refresh_all():
    from db import config, utils

    with config.AppSession() as session:
        utils.refresh_database(session)


database.add_command(clone_from_cloud)
database.add_command(restore_to_cloud)
