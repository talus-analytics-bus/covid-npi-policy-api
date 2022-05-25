import click
from .caseload import caseload
from .airtable import airtable


@click.group(help="Ingest caseload and Airtable data")
def ingest():
    pass


ingest.add_command(caseload)
ingest.add_command(airtable)
