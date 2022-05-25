import click


@click.command(help="Ingest policy and other data from Airtable")
@options.skip_restore
