"""Run API application"""
# local modules
from api import app, schema
from db import db

DO_INGEST = False
if DO_INGEST:
    print('ingesting data')
    db.generate_mapping(check_tables=False, create_tables=False)
    db.drop_all_tables(with_all_data=True)
    db.create_tables()
    schema.ingest_covid_npi_policy()

else:
    print("generating mapping")
    db.generate_mapping()
