"""Define API entity models."""
# standard modules
from datetime import date
from enum import Enum

# 3rd party modules
from pydantic import BaseModel, Field
from typing import Dict, List, Set, Optional
from enum import Enum


# # Define enum type support
# class State(Enum):
#     mv = 'mv'
#     jk = 'jk'
#     ac = 'ac'

class FilterFieldsPolicy(str, Enum):
    # TODO use inheritance to create `ClassNameExport` from this class
    primary_ph_measure = 'primary_ph_measure'


class PolicyFields(str, Enum):
    id = 'id'
    policy_name = 'policy_name'
    desc = 'desc'
    name_and_desc = 'name_and_desc'
    primary_ph_measure = 'primary_ph_measure'
    ph_measure_details = 'ph_measure_details'
    policy_type = 'policy_type'
    authority_name = 'authority_name'
    subtarget = 'subtarget'
    date_issued = 'date_issued'
    date_start_effective = 'date_start_effective'
    date_end_anticipated = 'date_end_anticipated'
    date_end_actual = 'date_end_actual'
    auth_entity = 'auth_entity'
    auth_entity_place_level = 'auth_entity.place.level'
    place = 'place'
    file = 'file'
    none = ''


class PlanFields(str, Enum):
    id = 'id'
    source_id = 'source_id'

    # descriptive information
    name = 'name'
    desc = 'desc'
    name_and_desc = 'name_and_desc'
    primary_loc = 'primary_loc'
    org_name = 'org_name'
    org_type = 'org_type'

    # dates
    date_issued = 'date_issued'
    date_start_effective = 'date_start_effective'
    date_end_effective = 'date_end_effective'

    # standardized fields / tags
    n_phases = 'n_phases'
    auth_entity_has_authority = 'auth_entity_has_authority'
    reqs_essential = 'reqs_essential'
    reqs_private = 'reqs_private'
    reqs_school = 'reqs_school'
    reqs_social = 'reqs_social'
    reqs_hospital = 'reqs_hospital'
    reqs_public = 'reqs_public'
    reqs_other = 'reqs_other'

    # university only
    residential = 'residential'

    # sourcing and PDFs
    plan_data_source = 'plan_data_source'
    announcement_data_source = 'announcement_data_source'
    file = 'file'

    # relationships
    place = 'place'
    auth_entity = 'auth_entity'
    none = ''


class CourtChallengeFields(str, Enum):
    id = 'id'
    matter_numbers = 'matter_numbers'
    case_name = 'case_name'
    case_number = 'case_number'
    court = 'court'
    jurisdiction = 'jurisdiction'
    filed_in_state_or_federal_court = 'filed_in_state_or_federal_court'
    government_order_upheld_or_enjoined = 'government_order_upheld_or_enjoined'
    summary_of_action = 'summary_of_action'
    policy_or_law_name = 'policy_or_law_name'
    parties = 'parties'
    legal_citation = 'legal_citation'
    holding = 'holding'
    complaint_category = 'complaint_category'
    data_source_for_complaint = 'data_source_for_complaint'
    data_source_for_decision = 'data_source_for_decision'
    date_of_decision = 'date_of_decision'
    date_of_complaint = 'date_of_complaint'
    parties_or_citation_and_summary_of_action = 'parties_or_citation_and_summary_of_action'
    policy_status = 'policy_status'
    case_status = 'case_status'

    # related entities
    policies = 'policies'
    none = ''


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
    home_rule: str = None
    dillons_rule: str = None


class Auth_Entity(BaseModel):
    id: int

    # descriptive information
    name: str
    office: str
    official: str = None
    place: Place = None


class File(BaseModel):
    id: int
    type: str = None
    filename: str = None
    data_source: str = None


class Policy(BaseModel):
    id: int = None

    # descriptive information
    policy_name: str = None
    desc: str = None
    name_and_desc: str = None
    primary_ph_measure: str = None
    ph_measure_details: str = None
    policy_type: str = None
    authority_name: str = None
    subtarget: str = None

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


class PolicyNumber(BaseModel):
    policy_number: int  # aka. `id`

    # descriptive information
    titles: List[str] = None
    auth_entity_offices: List[str] = None

    # relationships
    policies: List[Policy] = None


class Plan(BaseModel):
    """Plans. Similar to policies but they lack legal authority."""
    id: int = None
    source_id: str = None

    # descriptive information
    name: str = None
    desc: str = None
    name_and_desc: str = None
    primary_loc: str = None
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
    place: List[dict] = None
    auth_entity: List[dict] = None


class PolicyStatus(BaseModel):
    place_name: str = None
    value: str
    datestamp: date = None


class PolicyStatusCount(BaseModel):
    place_name: str = None
    value: int
    datestamp: date = None


examplePolicyFilter = {
    'dates_in_effect': [
        '2019-12-31',
        '2022-12-31',
    ]
}


class ExportFiltersNoOrdering(BaseModel):
    filters: Optional[Dict[str, List]] = Field(
        {}, title="Filters to be applied",
        description="Key: Name of data field on which to filter. Values: List of strings of values the data field may have."
    )


class PolicyFiltersNoOrdering(BaseModel):
    filters: Optional[Dict[str, List]] = Field(
        examplePolicyFilter, title="Filters to be applied",
        description="Key: Name of data field on which to filter. Values: List of strings of values the data field may have."
    )


class PlanFiltersNoOrdering(BaseModel):
    filters: Optional[Dict[str, List]] = Field(
        {"date_issued": ["2019-12-31", "2022-12-31"]}, title="Filters to be applied",
        description="Key: Name of data field on which to filter. Values: List of strings of values the data field may have."
    )


class ChallengeFiltersNoOrdering(BaseModel):
    filters: Optional[Dict[str, List]] = Field(
        {"date_of_complaint": ["2019-12-31", "2022-12-31"]}, title="Filters to be applied",
        description="Key: Name of data field on which to filter. Values: List of strings of values the data field may have."
    )


class PolicyFilters(PolicyFiltersNoOrdering):
    ordering: List[list] = [['id', 'asc']]


class PlanFilters(PlanFiltersNoOrdering):
    ordering: List[list] = [['id', 'asc']]


class ChallengeFilters(ChallengeFiltersNoOrdering):
    ordering: List[list] = [['id', 'asc']]


class PolicyList(Response):
    data: List[Policy]


class PolicyNumberList(Response):
    data: List[PolicyNumber]


class Court_Challenge(BaseModel):
    id: int
    jurisdiction: str = None
    case_name: str = None
    summary_of_action: str = None
    policy_or_law_name: str = None
    parties: str = None
    legal_citation: str = None
    court: str = None
    case_number: str = None
    holding: str = None
    complaint_category: List[str] = None
    data_source_for_complaint: str = None
    data_source_for_decision: str = None
    date_of_decision: date = None
    date_of_complaint: date = None
    government_order_upheld_or_enjoined: str = None
    parties_or_citation_and_summary_of_action: str = None
    policy_status: str = None
    case_status: str = None

    # related entities
    policies: List[Policy] = None


class ChallengeList(Response):
    data: List[Court_Challenge]


class PlanList(Response):
    data: List[Plan]


class PolicyDict(Response):
    data: Dict[str, List[Policy]]


class PolicyStatusList(Response):
    data: List[PolicyStatus]


class PolicyStatusCountList(Response):
    data: List[PolicyStatusCount]


class OptionSetList(Response):
    data: Dict[str, List[dict]]


class MetadataList(Response):
    data: Dict[str, dict] = {}


class Iso3Codes(str, Enum):
    all_countries = 'All countries'
    ARG = 'ARG'
    AUS = 'AUS'
    BOL = 'BOL'
    BRA = 'BRA'
    BWA = 'BWA'
    CAN = 'CAN'
    CHE = 'CHE'
    CHL = 'CHL'
    CHN = 'CHN'
    CIV = 'CIV'
    COD = 'COD'
    COL = 'COL'
    CUB = 'CUB'
    CZE = 'CZE'
    DEU = 'DEU'
    DJI = 'DJI'
    DNK = 'DNK'
    ECU = 'ECU'
    EGY = 'EGY'
    ESP = 'ESP'
    EST = 'EST'
    ETH = 'ETH'
    FRA = 'FRA'
    GBR = 'GBR'
    GHA = 'GHA'
    GIN = 'GIN'
    GUF = 'GUF'
    GUY = 'GUY'
    HKG = 'HKG'
    HND = 'HND'
    IND = 'IND'
    IRL = 'IRL'
    IRN = 'IRN'
    IRQ = 'IRQ'
    ISL = 'ISL'
    ISR = 'ISR'
    ITA = 'ITA'
    JOR = 'JOR'
    JPN = 'JPN'
    KOR = 'KOR'
    LBY = 'LBY'
    MAC = 'MAC'
    MAR = 'MAR'
    MEX = 'MEX'
    MHL = 'MHL'
    MNG = 'MNG'
    MYS = 'MYS'
    NER = 'NER'
    NGA = 'NGA'
    NOR = 'NOR'
    NZL = 'NZL'
    OMN = 'OMN'
    PAN = 'PAN'
    PER = 'PER'
    PHL = 'PHL'
    PRY = 'PRY'
    RUS = 'RUS'
    SAU = 'SAU'
    SEN = 'SEN'
    SLV = 'SLV'
    SOM = 'SOM'
    SUR = 'SUR'
    SWE = 'SWE'
    TCD = 'TCD'
    THA = 'THA'
    UGA = 'UGA'
    UMI = 'UMI'
    URY = 'URY'
    USA = 'USA'
    VEN = 'VEN'
    YEM = 'YEM'
    ZAF = 'ZAF'
    none = ''


StateNames = Enum(
    value='StateNames',
    names=[('All states and territories', 'All states and territories'),
           ('Alabama', 'Alabama'),
           ('Alaska', 'Alaska'),
           ('American Samoa', 'American Samoa'),
           ('Arizona', 'Arizona'),
           ('Arkansas', 'Arkansas'),
           ('California', 'California'),
           ('Colorado', 'Colorado'),
           ('Connecticut', 'Connecticut'),
           ('Delaware', 'Delaware'),
           ('District of Columbia', 'District of Columbia'),
           ('Florida', 'Florida'),
           ('Georgia', 'Georgia'),
           ('Guam', 'Guam'),
           ('Hawaii', 'Hawaii'),
           ('Idaho', 'Idaho'),
           ('Illinois', 'Illinois'),
           ('Indiana', 'Indiana'),
           ('Iowa', 'Iowa'),
           ('Kansas', 'Kansas'),
           ('Kentucky', 'Kentucky'),
           ('Louisiana', 'Louisiana'),
           ('Maine', 'Maine'),
           ('Maryland', 'Maryland'),
           ('Massachusetts', 'Massachusetts'),
           ('Michigan', 'Michigan'),
           ('Minnesota', 'Minnesota'),
           ('Mississippi', 'Mississippi'),
           ('Missouri', 'Missouri'),
           ('Montana', 'Montana'),
           ('Nebraska', 'Nebraska'),
           ('Nevada', 'Nevada'),
           ('New Hampshire', 'New Hampshire'),
           ('New Jersey', 'New Jersey'),
           ('New Mexico', 'New Mexico'),
           ('New York', 'New York'),
           ('North Carolina', 'North Carolina'),
           ('North Dakota', 'North Dakota'),
           ('Northern Mariana Islands', 'Northern Mariana Islands'),
           ('Ohio', 'Ohio'),
           ('Oklahoma', 'Oklahoma'),
           ('Oregon', 'Oregon'),
           ('Pennsylvania', 'Pennsylvania'),
           ('Puerto Rico', 'Puerto Rico'),
           ('Rhode Island', 'Rhode Island'),
           ('South Carolina', 'South Carolina'),
           ('South Dakota', 'South Dakota'),
           ('Tennessee', 'Tennessee'),
           ('Texas', 'Texas'),
           ('US Virgin Islands', 'US Virgin Islands'),
           ('Unspecified', 'Unspecified'),
           ('Utah', 'Utah'),
           ('Vermont', 'Vermont'),
           ('Virginia', 'Virginia'),
           ('Washington', 'Washington'),
           ('West Virginia', 'West Virginia'),
           ('Wisconsin', 'Wisconsin'),
           ('Wyoming', 'Wyoming'),
           ('none', ''),
           ]
)
