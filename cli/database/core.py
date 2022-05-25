import click

from .clone import clone_from_cloud
from .restore import restore_to_cloud


@click.group(help="Restore database to cloud or clone database from cloud")
def database():
    pass


database.add_command(clone_from_cloud)
database.add_command(restore_to_cloud)
