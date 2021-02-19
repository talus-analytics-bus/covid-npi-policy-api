"""Run data ingest application"""
# standard modules and packages
import argparse
from os import sys
import pandas as pd

# local modules
from api import schema
from db import db
from ingest import CovidPolicyPlugin

# setup arguments
parser = argparse.ArgumentParser(description='Define which data ingest routines to run')
parser.add_argument('-a', '--all', default=False,
                    action='store_const',
                    const=True,
                    help='ingest all data types')
parser.add_argument('-p', '--policies', default=False,
                    action='store_const',
                    const=True,
                    help='ingest policies')
parser.add_argument('-g', '--group-numbers', default=False,
                    action='store_const',
                    const=True,
                    help='assign policy group numbers')
parser.add_argument('-c', '--challenges', default=False,
                    action='store_const',
                    const=True,
                    help='ingest court challenges')
parser.add_argument('-d', '--distancing-levels', default=False,
                    action='store_const',
                    const=True,
                    help='ingest distancing levels')
parser.add_argument('-m', '--metadata', default=False,
                    action='store_const',
                    const=True,
                    help='ingest metadata')

if __name__ == "__main__":
    # constants
    # command line arguments
    args = parser.parse_args()

    # define red and green airtable keys and pick the one to use
    green_airtable_key = 'appoXaOlIgpiHK3I2'
    airtable_key = green_airtable_key

    # generate db mapping
    db.generate_mapping(create_tables=True)

    # update core policy data, if appropriate
    plugin = CovidPolicyPlugin()
    client = plugin.load_client(airtable_key)
    data: pd.DataFrame = client.client \
            .worksheet(name='Policy Database') \
            .as_dataframe()
    client.run_tests(data)
    
