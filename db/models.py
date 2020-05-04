"""Define database models."""
# standard modules
from datetime import date

# 3rd party modules
from pony.orm import PrimaryKey, Required, Optional, Optional, Set, StrArray
# from enum import Enum
# from pony.orm.dbapiprovider import StrConverter

# local modules
from .config import db

# # Define enum type support
# class State(Enum):
#     mv = 'mv'
#     jk = 'jk'
#     ac = 'ac'
#
#
# # Adapted from:
# # https://stackoverflow.com/questions/31395663/how-can-i-store-a-python-enum-using-pony-orm
# class EnumConverter(StrConverter):
#     def validate(self, val):
#         if not isinstance(val, Enum):
#             raise ValueError('Must be an Enum.  Got {}'.format(type(val)))
#         return val
#
#     def py2sql(self, val):
#         return val.name
#
#     def sql2py(self, value):
#         return self.py_type[value].name
# db.provider.converter_classes.append((Enum, EnumConverter))


class Policy(db.Entity):
    """Non-pharmaceutical intervention (NPI) policies."""
    _table_ = "policy"
    id = PrimaryKey(int, auto=False)

    # descriptive information
    desc = Optional(str)
    primary_ph_measure = Optional(str)
    ph_measure_details = Optional(str)
    policy_type = Optional(str)
    # enum_test = Optional(State, column='enum_test_str')

    # key dates
    date_issued = Optional(date)
    date_start_effective = Optional(date)
    date_end_anticipated = Optional(date)
    date_end_actual = Optional(date)

    # relationships
    doc = Set('Doc')
    auth_entity = Set('Auth_Entity')
    place = Optional('Place')


class Place(db.Entity):
    _table_ = "place"
    id = PrimaryKey(int, auto=True)
    level = Optional(str)
    iso3 = Optional(str)
    area1 = Optional(str)
    area2 = Optional(str)
    loc = Optional(str)

    # relationships
    policies = Set('Policy')
    auth_entities = Set('Auth_Entity')


class Auth_Entity(db.Entity):
    """Authorizing entities."""
    _table_ = "auth_entity"
    id = PrimaryKey(int, auto=True)
    name = Optional(str)
    office = Optional(str)

    # relationships
    policies = Set('Policy')
    place = Optional('Place')


class Doc(db.Entity):
    """Supporting documentation."""
    _table_ = "doc"
    id = PrimaryKey(int, auto=True)
    name = Optional(str)
    type = Required(str)
    url = Optional(str)
    pdf = Optional(str)

    # relationships
    policies = Set('Policy')
