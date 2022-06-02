import os
import logging
import subprocess
from typing import Union
from cli.ingest.distancing import do_distancing

import click

from cli import options


# constants
logger = logging.getLogger(__name__)

DBNAME_CLOUD_MAIN_TEST_DEFAULT: str = "covid-npi-policy-test"


@click.command(help="Ingest policy and other data from Airtable")
@click.option(
    "--dbname-cloud",
    "-dc",
    default=os.getenv("DBNAME_CLOUD_MAIN_TEST", DBNAME_CLOUD_MAIN_TEST_DEFAULT),
    show_default=True,
    type=str,
    help="Cloud PostgreSQL test site database name (to copy to). Can also be set with"
    " environment variable `DBNAME_CLOUD_MAIN_TEST`.",
)
@options.dbmigration_local
@options.awseb_restart
@click.option(
    "--postprocess-only",
    default=False,
    is_flag=True,
    show_default=True,
    help="If flag set, then only postprocessing is run on the local database, and"
    " no other steps in the workflow will be executed.",
)
@options.yes
@options.skip_restore
def airtable(
    dbname_cloud: str,
    username_local: Union[str, None],
    dbname_local: Union[str, None],
    awseb_environment_name: Union[str, None],
    awseb_environment_region: Union[str, None],
    postprocess_only: bool,
    yes: bool,
    skip_restore: bool,
):
    if postprocess_only:
        do_postprocessing()
        return

    # import modules (delay imports until this point to streamline CLI efficiency)
    from cli import awseb
    from cli.database.restore import do_restore_to_cloud
    from db.config import execute_raw_sql

    # validate
    options.validate_awseb_restart_ops(awseb_environment_name, awseb_environment_region)

    # confirm
    if not yes:
        click.confirm(
            f"This will delete all data from your local database `{dbname_local}`,"
            " replace it with the latest data from Airtable, and overwrite all data"
            f" in the AWS RDS cloud database `{dbname_cloud}`."
            "\nDo you want to continue?",
            abort=True,
        )

    # delete existing data
    execute_raw_sql("TRUNCATE policy, plan, court_challenge, policy_number CASCADE;")

    # ingest all Airtable data
    do_main_ingest(all=True)

    # run post-processing
    do_postprocessing()

    # update test database in cloud
    if not skip_restore:
        do_restore_to_cloud(
            dbname_cloud,
            username_local,
            dbname_local,
            awseb_environment_name,
            awseb_environment_region,
            yes=yes,
        )

    # restart test API server
    if awseb_environment_name is not None and awseb_environment_region is not None:
        awseb.restart_app_server(awseb_environment_name, awseb_environment_region)


def do_main_ingest(
    all: bool,
    metadata: bool = False,
    policies: bool = False,
    group_numbers: bool = False,
    challenges: bool = False,
    distancing_levels: bool = False,
):
    from api import core
    from db import db
    from ingest.distancinglevelgetter.core import DistancingLevelGetter
    from ingest.places.core import (
        add_local_plus_state_places,
        add_missing_usa_local_areas,
        update_tribal_nation_fields,
    )
    from ingest.plugins import CovidPolicyPlugin, assign_policy_group_numbers

    AIRTABLE_BASE_ID: str = "appoXaOlIgpiHK3I2"

    # generate database mapping and ingest data for the COVID-AMP project
    db.generate_mapping(create_tables=False)

    # update core policy data, if appropriate
    plugin = CovidPolicyPlugin()
    client = plugin.load_client(AIRTABLE_BASE_ID)

    # load and process metadata
    if metadata or policies or challenges or all:
        client.load_metadata().process_metadata(db)

    # # Court challenges: Commented out because they are not included in the site
    # # ingest court challenges and matter number info, if appropriate
    # if challenges:
    #     client.load_court_challenge_data().process_court_challenge_data(db)
    #     plugin.post_process_court_challenge_data(db)

    #     if not policies:
    #         plugin.post_process_policies(db, include_court_challenges=True)

    if policies or all:

        # ingest main data
        client.load_data().process_data(db)

        # post-process places
        plugin.post_process_places(db)
        plugin.post_process_policies(db)
        plugin.post_process_policy_numbers(db)
        core.add_search_text()

    if group_numbers or policies or all:
        assign_policy_group_numbers(db)

    # Update observations of lockdown level, if appropriate
    if distancing_levels:
        try:
            do_distancing(yes=True) # CLI command func for ingesting distancing lvls
        except Exception:
            logger.error(
                "\nERROR: Observations not loaded successfully, check for "
                "Amazon S3 errors."
            )

    # # Court challenges: Commented out because they are not included in the site
    # if challenges or all:
    #     # TODO remove this when court challenge complaint categories and
    #     # subcategories are updated circa Nov/Dec 2020
    #     plugin.debug_add_test_complaint_cats(db)

    if policies or all:
        # add missing local area places if needed
        add_missing_usa_local_areas()
        add_local_plus_state_places()

    # update tribal nation place tagging
    update_tribal_nation_fields()
    print("Main data ingest complete")


def do_postprocessing():
    from db.config import execute_raw_sql

    # Remove future policies
    execute_raw_sql("DELETE FROM policy WHERE date_start_effective > CURRENT_DATE;")

    # Remove policies without places
    execute_raw_sql(
        """
        WITH policy_to_delete AS (
            SELECT
                id
            FROM
                "policy" p
                LEFT JOIN place_to_policy p2p ON p2p."policy" = p.id
            WHERE
                p2p."policy" IS NULL
        )
        DELETE FROM
            "policy"
        WHERE
            id IN (
                SELECT
                    id
                FROM
                    policy_to_delete
            );
        """
    )

    # Update `name_and_desc` field based on name and desc of policy/plan
    execute_raw_sql(
        """
        UPDATE
            "policy"
        SET
            name_and_desc = CONCAT(policy_name, ': ', "desc")
        WHERE
            policy_name IS NOT NULL
            AND policy_name != ''
            AND policy_name != 'Unspecified';
        UPDATE
            "policy"
        SET
            name_and_desc = "desc"
        WHERE
            policy_name IS NULL
            OR policy_name = ''
            OR policy_name = 'Unspecified';
        UPDATE
            "plan"
        SET
            name_and_desc = CONCAT("name", ': ', "desc")
        WHERE
            "name" IS NOT NULL
            AND "name" != ''
            AND "name" != 'Unspecified';
        UPDATE
            "plan"
        SET
            name_and_desc = "desc"
        WHERE
            "name" IS NULL
            OR "name" = ''
            OR "name" != 'Unspecified';
        """
    )

    # refresh materialized views
    execute_raw_sql(
        """
        REFRESH MATERIALIZED VIEW "policy_date";
        REFRESH MATERIALIZED VIEW "policy_day_dates";
        REFRESH MATERIALIZED VIEW "policy_by_group_number";
        """
    )

    print("Postprocessing scripts complete")
