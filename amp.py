import click

from cli.ingest import ingest
from cli.database import database
from cli.export import export


@click.group(help="Command line interface (CLI) for COVID AMP data management.")
def amp():
    pass


amp.add_command(ingest)
amp.add_command(database)
amp.add_command(export)


if __name__ == "__main__":
    amp()
