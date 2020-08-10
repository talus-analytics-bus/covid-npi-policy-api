"""Run data ingest application"""
# standard modules
from os import sys

# local modules
from api import schema
from db import db
from ingest import CovidPolicyPlugin

if __name__ == "__main__":
    # generate database mapping and ingest data for the COVID-AMP project
    ingest_lockdown_levels = len(sys.argv) > 1 and sys.argv[1] == 'yes'
    db.generate_mapping(create_tables=True)
    plugin = CovidPolicyPlugin()

    # update core policy data
    plugin.load_client('appOtKBVJRyuH83wf').load_data().process_data(db)

    # post-process places
    plugin.post_process_places(db)
    plugin.post_process_policies(db)

    # Update observations of lockdown level
    if ingest_lockdown_levels:
        plugin.load_client('appEtzBj5rWEsbcE9').load_observations(db)

    sys.exit(0)

    # # Drop all data/tables before ingesting
    # db.generate_mapping(check_tables=False, create_tables=False)
    # db.drop_all_tables(with_all_data=True)
    # db.create_tables()
    # plugin = CovidPolicyPlugin()
    # plugin.load_client().load_data().process_data(db)
    # sys.exit(0)
