"""Define API entity models."""
# standard modules
from datetime import date

# 3rd party modules
from pydantic import BaseModel
from typing import List


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

    # relationships
    auth_entity: Auth_Entity = None  # TODO as entity


class PolicyList(Response):
    data: List[Policy]
