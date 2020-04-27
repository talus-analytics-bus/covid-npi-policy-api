"""Define API entity models."""
# standard modules
from datetime import date

# 3rd party modules
from pydantic import BaseModel
from typing import Dict, List, Set
from enum import Enum


# # Define enum type support
# class State(Enum):
#     mv = 'mv'
#     jk = 'jk'
#     ac = 'ac'


class Response(BaseModel):
    success: bool
    message: str


class Auth_Entity(BaseModel):
    id: int

    # descriptive information
    level: str
    iso3: str
    area1: str
    area2: str
    name: str
    office: str
    desc: str = None


class Policy(BaseModel):
    id: int

    # descriptive information
    desc: str = None
    primary_ph_measure: str = None
    ph_measure_details: str = None
    policy_type: str = None

    # key dates
    date_issued: date = None
    date_start_effective: date = None
    date_end_anticipated: date = None
    date_end_actual: date = None
    # enum_test: State = None

    # relationships
    auth_entity: Auth_Entity = None  # TODO as entity


class PolicyFilters(BaseModel):
    filters: Dict[str, List]


class PolicyList(Response):
    data: List[Policy]


class OptionSetList(Response):
    data: Dict[str, List[dict]]
