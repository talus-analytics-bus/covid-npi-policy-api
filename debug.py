"""Run data ingest application"""
# standard modules and packages
# import pandas as pd

# local modules
from api.ampresolvers.tests import test_policy_status_counter

if __name__ == "__main__":
    test_policy_status_counter.test_countries()
