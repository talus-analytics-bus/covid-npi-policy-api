"""Run caseload data ingest application"""
# standard modules
from os import sys

# local modules
from api import schema
from db_metric import db
from ingest import CovidCaseloadPlugin

# generate database mapping and ingest data for the COVID-AMP project
db.generate_mapping(create_tables=False)
plugin = CovidCaseloadPlugin()
plugin.upsert_data(db)
print('Success!')


# plugin.load_client().load_data().process_data(db)
sys.exit(0)

# # Drop all data/tables before ingesting
# db.generate_mapping(check_tables=False, create_tables=False)
# db.drop_all_tables(with_all_data=True)
# db.create_tables()
# plugin = CovidPolicyPlugin()
# plugin.load_client().load_data().process_data(db)
# sys.exit(0)
