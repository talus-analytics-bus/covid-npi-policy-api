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


class Place(BaseModel):
    id: int

    # descriptive information
    level: str
    iso3: str
    area1: str
    area2: str
    loc: str = None


class Auth_Entity(BaseModel):
    id: int

    # descriptive information
    name: str
    office: str
    place: Place = None


class Doc(BaseModel):
    id: int
    type: str
    pdf: str = None
    url: str = None


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
    auth_entity: List[Auth_Entity] = None
    place: Place = None
    policy_docs: List[Doc] = None


class PolicyFilters(BaseModel):
    filters: Dict[str, List]


class PolicyList(Response):
    data: List[Policy]


class OptionSetList(Response):
    data: Dict[str, List[dict]]


class MetadataList(Response):
    data: Dict[str, dict]
