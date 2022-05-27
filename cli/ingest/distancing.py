import logging

import click

logger = logging.getLogger(__name__)


@click.command(help="Ingest distancing levels from S3 bucket")
def distancing():
    from ingest.distancinglevelgetter.core import DistancingLevelGetter
    from db import db

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
