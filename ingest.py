"""Run data ingest application"""
# standard modules
from os import sys

# local modules
from api import schema
from db import db

print('Ingesting data.')
db.generate_mapping(check_tables=False, create_tables=False)
db.drop_all_tables(with_all_data=True)
db.create_tables()
schema.ingest_covid_npi_policy()
# schema.clean_docs()
sys.exit(0)
