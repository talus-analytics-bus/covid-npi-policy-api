"""Run caseload data ingest application"""
# standard modules
import argparse
from os import sys

# local modules
from api import schema
from db_metric import db
from db import db as db_amp
from ingest import CovidCaseloadPlugin

# setup arguments
parser = argparse.ArgumentParser(description='Ingest caseload data')
parser.add_argument('-s', '--state', default=False,
                    action='store_const',
                    const=True,
                    help='ingest state data')
parser.add_argument('-g', '--globe', default=False,
                    action='store_const',
                    const=True,
                    help='ingest global data')
parser.add_argument('-a', '--all', default=False,
                    action='store_const',
                    const=True,
                    help='ingest all data')

if __name__ == "__main__":
    # get args
    args = parser.parse_args()
    do_state = args.state or args.all
    do_global = args.globe or args.all

    # generate database mapping and ingest data for the COVID-AMP project
    db.generate_mapping(create_tables=False)
    db_amp.generate_mapping(create_tables=False)
    plugin = CovidCaseloadPlugin()
    plugin.upsert_data(db, db_amp, do_state=do_state, do_global=do_global)
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
