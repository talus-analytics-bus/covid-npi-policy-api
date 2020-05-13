"""Run data ingest application"""
# standard modules
from os import sys

# local modules
from api import schema
from db import db
from ingest import CovidPolicyPlugin

# generate database mapping and ingest data for the COVID-AMP project
db.generate_mapping()
plugin = CovidPolicyPlugin()
plugin.load_client().load_data().process_data(db)
sys.exit(0)

# # Drop all data/tables before ingesting
# db.generate_mapping(check_tables=False, create_tables=False)
# db.drop_all_tables(with_all_data=True)
# db.create_tables()
# ingest_covid_npi_policy()
# sys.exit(0)
