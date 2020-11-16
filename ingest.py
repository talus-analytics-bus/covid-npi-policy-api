"""Run data ingest application"""
# standard modules
import argparse
from os import sys

# local modules
from api import schema
from db import db
from ingest import CovidPolicyPlugin

# setup arguments
parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('-mo', '--metadata-only', default=False,
                    action='store_const',
                    const=True,
                    help='update metadata only')
parser.add_argument('-sp', '--skip-policies', default=False,
                    action='store_const',
                    const=True,
                    help='skip updating policy and plan data')
parser.add_argument('-sl', '--skip-levels', default=False,
                    action='store_const',
                    const=True,
                    help='skip updating distancing levels')
parser.add_argument('-sc', '--skip-court', default=False,
                    action='store_const',
                    const=True,
                    help='skip updating court challenges')

if __name__ == "__main__":

    args = parser.parse_args()
    ingest_policies = not args.skip_policies
    ingest_court = not args.skip_court
    ingest_lockdown_levels = not args.skip_levels
    metadata_only = args.metadata_only

    # constants
    # define red and green airtable keys and pick the one to use
    red_airtable_key = 'appOtKBVJRyuH83wf'
    green_airtable_key = 'appoXaOlIgpiHK3I2'
    # airtable_key = red_airtable_key
    airtable_key = green_airtable_key

    # generate database mapping and ingest data for the COVID-AMP project
    db.generate_mapping(create_tables=True)
    plugin = CovidPolicyPlugin()

    # update core policy data, if appropriate
    client = plugin.load_client(airtable_key)

    # load and process metadata
    client.load_metadata().process_metadata(db)

    if not metadata_only:
        # ingest court challenges and matter number info, if appropriate
        if ingest_court:
            print('\n\nIngesting court challenges and matter numbers data...')
            client.load_court_challenge_data().process_court_challenge_data(db)
            plugin.post_process_court_challenge_data(db)

            if not ingest_policies:
                plugin.post_process_policies(db, include_court_challenges=True)

        else:
            print('\n\nSkipping court challenges and matter numbers data ingest.\n')

        if ingest_policies:
            client.load_data().process_data(db)

            # post-process places
            plugin.post_process_places(db)
            plugin.post_process_policies(db)
            plugin.post_process_policy_numbers(db)
            schema.add_search_text()
        else:
            print('\n\nSkipping policy ingest.\n')

        # Update observations of lockdown level, if appropriate
        if ingest_lockdown_levels:
            plugin.load_client('appEtzBj5rWEsbcE9').load_observations(db)
        else:
            print('\n\nSkipping distancing level ingest.\n')

    # TODO remove this when court challenge complaint categories and
    # subcategories are updated circa Nov/Dec 2020
    plugin.debug_add_test_complaint_cats(db)
    sys.exit(0)
