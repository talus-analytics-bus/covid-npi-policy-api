"""Run data ingest application"""
# standard modules and packages
# import pandas as pd

# local modules


from ingest.places.core import (
    add_local_plus_state_places,
    add_missing_usa_local_areas,
)


if __name__ == "__main__":
    add_missing_usa_local_areas()
    add_local_plus_state_places()
