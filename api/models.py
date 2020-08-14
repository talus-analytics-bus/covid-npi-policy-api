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
    next_page_url: str = None
    n: int = None


class ListResponse(Response):
    data: List[dict]


class Place(BaseModel):
    id: int

    # descriptive information
    level: str = None
    iso3: str = None
    area1: str = None
    area2: str = None
    loc: str = None


class Auth_Entity(BaseModel):
    id: int

    # descriptive information
    name: str
    office: str
    place: Place = None


class File(BaseModel):
    id: int
    type: str = None
    filename: str = None
    data_source: str = None


class Policy(BaseModel):
    id: int

    # descriptive information
    desc: str = None
    primary_ph_measure: str = None
    ph_measure_details: str = None
    policy_type: str = None
    policy_name: str = None
    authority_name: str = None

    # key dates
    date_issued: date = None
    date_start_effective: date = None
    date_end_anticipated: date = None
    date_end_actual: date = None
    # enum_test: State = None

    # relationships
    auth_entity: List[Auth_Entity] = None
    place: List[Place] = None
    file: List = None


class Plan(BaseModel):
    """Plans. Similar to policies but they lack legal authority."""
    id: int
    source_id: str

    # descriptive information
    name: str = None
    primary_loc: str = None
    desc: str = None
    org_name: str = None
    org_type: str = None
    name: str = None

    # dates
    date_issued: date = None
    date_start_effective: date = None
    date_end_effective: date = None

    # standardized fields / tags
    n_phases: int = None
    auth_entity_has_authority: str = None
    reqs_essential: List[str] = None
    reqs_private: List[str] = None
    reqs_school: List[str] = None
    reqs_social: List[str] = None
    reqs_hospital: List[str] = None
    reqs_public: List[str] = None
    reqs_other: List[str] = None

    # university only
    residential: bool = None

    # sourcing and PDFs
    plan_data_source: str = None
    announcement_data_source: str = None
    file: List = None

    # relationships
    place: List[Place] = None
    auth_entity: Auth_Entity = None


class PolicyStatus(BaseModel):
    place_name: str = None
    value: str
    datestamp: date = None


class PolicyFilters(BaseModel):
    filters: Dict[str, List]
    ordering: List[list]


class PolicyList(Response):
    data: List[Policy]


class PlanList(Response):
    data: List[Plan]


class PolicyDict(Response):
    data: Dict[str, List[Policy]]


class PolicyStatusList(Response):
    data: List[PolicyStatus]


class OptionSetList(Response):
    data: Dict[str, List[dict]]


class MetadataList(Response):
    data: Dict[str, dict]
