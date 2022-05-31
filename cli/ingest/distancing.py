import logging

import click

from cli import options

logger = logging.getLogger(__name__)


@click.command(
    help="Ingest distancing levels from S3 bucket `s3://covid-npi-policy-storage`."
)
@options.yes
def distancing(yes: bool):
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
    except Exception:
        logger.error(
            "\nERROR: Observations not loaded successfully, check for "
            "Amazon S3 errors."
        )
