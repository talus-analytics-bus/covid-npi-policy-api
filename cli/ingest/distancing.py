import logging

import click

from cli import options
from cli.decorators import db_mapping

logger = logging.getLogger(__name__)


@click.command(
    help="Ingest distancing levels from S3 bucket `s3://covid-npi-policy-storage`."
)
@options.yes
def distancing(yes: bool):
    do_distancing(yes)

@db_mapping
def do_distancing(yes: bool):
    """Ingest distancing levels from S3 bucket `s3://covid-npi-policy-storage`.

    Args:
        yes (bool): True if no confirmation dialogues should be shown
    """    
    if not yes:
        click.confirm(
            "This will replace all distancing levels in table `observation` of the"
            " database.\nDo you want to continue?",
            abort=True,
        )

    # import modules
    from db import db
    from ingest.distancinglevelgetter.core import DistancingLevelGetter

    # Update observations of distancing levels
    try:
        getter: DistancingLevelGetter = DistancingLevelGetter(
            S3_BUCKET_NAME="covid-npi-policy-storage",
            path="Distancing-Status",
            fn_prefix="distancing_status",
        )
        getter.import_levels(db=db)
    except Exception as e:
        logger.error(
            "\nERROR: Observations not loaded successfully, check for "
            "Amazon S3 errors and then following exception:."
        )
        raise(e)
