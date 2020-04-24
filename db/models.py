"""Define database models."""
# standard modules
from datetime import date

# 3rd party modules
from pony.orm import PrimaryKey, Optional, Optional, Set, StrArray

# local modules
from .config import db


class Policy(db.Entity):
    """Non-pharmaceutical intervention (NPI) policies."""
    _table_ = "policy"
    id = PrimaryKey(int, auto=False)

    # descriptive information
    desc = Optional(str)
    primary_ph_measure = Optional(str)
    ph_measure_details = Optional(str)
    policy_type = Optional(str)

    # key dates
    date_issued = Optional(date)
    date_start_effective = Optional(date)
    date_end_anticipated = Optional(date)
    date_end_actual = Optional(date)

    # relationships
    auth_entity = Optional('Auth_Entity')


class Auth_Entity(db.Entity):
    """Authorizing entities."""
    _table_ = "auth_entity"
    id = PrimaryKey(int, auto=True)
    level = Optional(str)
    iso3 = Optional(str)
    area1 = Optional(str)
    area2 = Optional(str)
    name = Optional(str)
    office = Optional(str)
    policies = Set('Policy')


if False:
    db.generate_mapping(check_tables=False, create_tables=False)
    db.drop_all_tables(with_all_data=True)
    db.create_tables()
else:
    db.generate_mapping()

# db.generate_mapping(create_tables=True)
