"""Run caseload data ingest application"""
# standard modules
from os import sys

# local modules
from api import schema
from db_metric import db
from db import db as db_amp
from ingest import CovidCaseloadPlugin

# generate database mapping and ingest data for the COVID-AMP project
db.generate_mapping(create_tables=False)
db_amp.generate_mapping(create_tables=False)
plugin = CovidCaseloadPlugin()
plugin.upsert_data(db, db_amp)
print('Success!')

sys.exit(0)
