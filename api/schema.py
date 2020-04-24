from ingest import CovidPolicyPlugin
from db import db


def ingest_covid_npi_policy():
    plugin = CovidPolicyPlugin()
    plugin.load_client().load_data().process_data(db)
    return []


def test():
    ingest_covid_npi_policy()


test()
