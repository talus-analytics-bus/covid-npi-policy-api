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
    plugin.post_process_policies(db)
    plugin.post_process_policy_numbers(db)
    sys.exit(0)


    # update core policy data, if appropriate
    client = plugin.load_client(airtable_key)

    # load and process metadata
    client.load_metadata().process_metadata(db)

    # ingest court challenges and matter number info, if appropriate
    if ingest_court:
        print('\n\nIngesting court challenges and matter numbers data...')
        client.load_court_challenge_data().process_court_challenge_data(db)
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

    sys.exit(0)

    # # Drop all data/tables before ingesting
    # db.generate_mapping(check_tables=False, create_tables=False)
    # db.drop_all_tables(with_all_data=True)
    # db.create_tables()
    # plugin = CovidPolicyPlugin()
    # plugin.load_client().load_data().process_data(db)
    # sys.exit(0)
