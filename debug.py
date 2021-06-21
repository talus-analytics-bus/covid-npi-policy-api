"""Run data ingest application"""
# standard modules and packages
# import pandas as pd

# local modules


from api.ampresolvers.policystatuscounter.tests.test_policy_status_counter import (
    test_counties,
    test_counties_plus_states,
    test_countries,
    test_states,
)
from api.ampresolvers.policystatuscounter.helpers import StaticMaxMinCounter
from db import db

from ingest.places.core import (
    add_local_plus_state_places,
    add_missing_usa_local_areas,
)


if __name__ == "__main__":
    # db.generate_mapping()
    # counter: StaticMaxMinCounter = StaticMaxMinCounter()
    # counter.get_max_min_counts()
    # add_missing_usa_local_areas()
    # add_local_plus_state_places()
    test_counties_plus_states()
