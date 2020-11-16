"""Run data ingest application"""
# standard modules
from os import sys

# local modules
from api import schema
from db import db
from ingest import CovidPolicyPlugin

if __name__ == "__main__":
    # constants
    # define red and green airtable keys and pick the one to use
    red_airtable_key = 'appOtKBVJRyuH83wf'
    green_airtable_key = 'appoXaOlIgpiHK3I2'
    # airtable_key = red_airtable_key
    airtable_key = green_airtable_key

    # ingest policies?
    ingest_policies = True

    # ingest court challenges and matter numbers?
    ingest_court = True

    # generate database mapping and ingest data for the COVID-AMP project
    ingest_lockdown_levels = len(sys.argv) > 1 and sys.argv[1] == 'yes'
    db.generate_mapping(create_tables=True)
    plugin = CovidPolicyPlugin()

    # update core policy data, if appropriate
    client = plugin.load_client(airtable_key)

    # load and process metadata
    client.load_metadata().process_metadata(db)

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

    # TODO remove this when court challenge complaint categories and
    # subcategories are updated circa Nov/Dec 2020
    plugin.debug_add_test_complaint_cats(db)

    # Update observations of lockdown level, if appropriate
    if ingest_lockdown_levels:
        plugin.load_client('appEtzBj5rWEsbcE9').load_observations(db)
    else:
        print('\n\nSkipping distancing level ingest.\n')
