"""Run data ingest application"""
# standard modules and packages
# import pandas as pd

# local modules
from db import db
from ingest.plugins import CovidPolicyPlugin


if __name__ == "__main__":
    # define red and green airtable keys and pick the one to use
    airtable_key = "appoXaOlIgpiHK3I2"

    # generate db mapping
    db.generate_mapping(create_tables=True)

    # update core policy data, if appropriate
    plugin = CovidPolicyPlugin()
    client = plugin.load_client(airtable_key)
    client.load_data().process_data(db).post_process_policies(
        db
    ).post_process_policy_numbers(db)
    plugin.assign_policy_group_numbers(db)
