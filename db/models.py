"""Define database models."""
# standard modules
# from datetime import datetime, date

# 3rd party modules
from pony.orm import PrimaryKey, Required, Optional, Set, StrArray

# local modules
from .config import db


class Policy(db.Entity):
    """Non-pharmaceutical intervention (NPI) policies."""
    _table_ = "policy"
    id = PrimaryKey(int, auto=True)
    auth_entity = Required('Auth_Entity')


class Auth_Entity(db.Entity):
    """Authorizing entities."""
    _table_ = "auth_entity"
    id = PrimaryKey(int, auto=True)
    level = Required(str)
    iso3 = Required(str)
    area1 = Required(str)
    area2 = Required(str)
    name = Required(str)
    offices = Required(StrArray)
    policies = Set('Policy')


db.generate_mapping(create_tables=True)
