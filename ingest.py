"""Run data ingest application"""
# standard modules and packages
import argparse
from ingest.places.core import (
    add_local_plus_state_places,
    add_missing_usa_local_areas,
    update_tribal_nation_fields,
)

# local modules
from api import schema
from db import db
from ingest.plugins import CovidPolicyPlugin, assign_policy_group_numbers

# setup arguments
parser = argparse.ArgumentParser(
    description="Define which data ingest routines to run"
)
parser.add_argument(
    "-a",
    "--all",
    default=False,
    action="store_const",
    const=True,
    help="ingest all data types",
)
parser.add_argument(
    "-p",
    "--policies",
    default=False,
    action="store_const",
    const=True,
    help="ingest policies",
)
parser.add_argument(
    "-g",
    "--group-numbers",
    default=False,
    action="store_const",
    const=True,
    help="assign policy group numbers",
)
parser.add_argument(
    "-c",
    "--challenges",
    default=False,
    action="store_const",
    const=True,
    help="ingest court challenges",
)
parser.add_argument(
    "-d",
    "--distancing-levels",
    default=False,
    action="store_const",
    const=True,
    help="ingest distancing levels",
)
parser.add_argument(
    "-m",
    "--metadata",
    default=False,
    action="store_const",
    const=True,
    help="ingest metadata",
)

if __name__ == "__main__":

    # constants
    # command line arguments
    args = parser.parse_args()

    # define red and green airtable keys and pick the one to use
    green_airtable_key = "appoXaOlIgpiHK3I2"
    airtable_key = green_airtable_key

    # ingest policies?
    ingest_policies = args.policies or args.all

    # ingest court challenges and matter numbers?
    ingest_court = args.challenges or args.all

    # generate database mapping and ingest data for the COVID-AMP project
    ingest_lockdown_levels = args.distancing_levels or args.all

    # generate db mapping
    db.generate_mapping(create_tables=False)

    # update core policy data, if appropriate
    plugin = CovidPolicyPlugin()
    client = plugin.load_client(airtable_key)

    # load and process metadata
    if args.metadata or args.policies or args.challenges or args.all:
        client.load_metadata().process_metadata(db)

    # ingest court challenges and matter number info, if appropriate
    if ingest_court:
        client.load_court_challenge_data().process_court_challenge_data(db)
        plugin.post_process_court_challenge_data(db)

        if not ingest_policies:
            plugin.post_process_policies(db, include_court_challenges=True)

    if ingest_policies:

        # ingest main data
        client.load_data().process_data(db)

        # post-process places
        plugin.post_process_places(db)
        plugin.post_process_policies(db)
        plugin.post_process_policy_numbers(db)
        schema.add_search_text()

    if args.group_numbers or ingest_policies:
        assign_policy_group_numbers(db)

    # Update observations of lockdown level, if appropriate
    if ingest_lockdown_levels:
        try:
            plugin.load_client("appEtzBj5rWEsbcE9").load_observations(db)
        except Exception:
            print(
                "\nERROR: Observations not loaded successfully, check for "
                "Airtable exceptions."
            )

    if ingest_court:
        # TODO remove this when court challenge complaint categories and
        # subcategories are updated circa Nov/Dec 2020
        plugin.debug_add_test_complaint_cats(db)

    if ingest_policies:
        # add missing local area places if needed
        add_missing_usa_local_areas()
        add_local_plus_state_places()

    # update tribal nation place tagging
    update_tribal_nation_fields()
