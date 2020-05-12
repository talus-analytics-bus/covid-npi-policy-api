"""Run data ingest application"""
# standard modules
from os import sys

# local modules
from api import schema
from db import db

db.generate_mapping()
# db.drop_all_tables(with_all_data=True)
# db.create_tables()
schema.ingest_covid_npi_policy()
# db.generate_mapping(check_tables=False, create_tables=False)
sys.exit(0)
