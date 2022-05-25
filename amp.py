import click
from cli.ingest import ingest


@click.group(help="Command line interface (CLI) for COVID AMP data management.")
def amp():
    pass


amp.add_command(ingest)


if __name__ == "__main__":
    amp()
