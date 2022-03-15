"""
Define API entity models.
TODO reorganize into packages

"""
# standard modules
from datetime import date
from enum import Enum

# 3rd party modules
from pydantic import BaseModel, Field
from typing import Any, Dict, Generic, List, Optional, TypeVar, Union


class FilterFieldsPolicy(str, Enum):
    # TODO use inheritance to create `ClassNameExport` from this class
    primary_ph_measure = "primary_ph_measure"


class PolicyFields(str, Enum):
    id = "id"
    policy_name = "policy_name"
    policy_number = "policy_number"
    desc = "desc"
    name_and_desc = "name_and_desc"
    primary_ph_measure = "primary_ph_measure"
    ph_measure_details = "ph_measure_details"
    policy_type = "policy_type"
    authority_name = "authority_name"
    subtarget = "subtarget"
    date_issued = "date_issued"
    date_start_effective = "date_start_effective"
    date_end_anticipated = "date_end_anticipated"
    date_end_actual = "date_end_actual"
    auth_entity = "auth_entity"
    auth_entity_place_level = "auth_entity.place.level"
    auth_entity_place_loc = "auth_entity.place.loc"
    court_challenges_id = "court_challenges.id"
    place = "place"
    file = "file"
    none = ""


class PlanFields(str, Enum):
    id = "id"
    source_id = "source_id"

    # descriptive information
    name = "name"
    desc = "desc"
    name_and_desc = "name_and_desc"
    primary_loc = "primary_loc"
    org_name = "org_name"
    org_type = "org_type"

    # dates
    date_issued = "date_issued"
    date_start_effective = "date_start_effective"
    date_end_effective = "date_end_effective"

    # standardized fields / tags
    n_phases = "n_phases"
    auth_entity_has_authority = "auth_entity_has_authority"
    reqs_essential = "reqs_essential"
    reqs_private = "reqs_private"
    reqs_school = "reqs_school"
    reqs_social = "reqs_social"
    reqs_hospital = "reqs_hospital"
    reqs_public = "reqs_public"
    reqs_other = "reqs_other"

    # university only
    residential = "residential"

    # sourcing and PDFs
    plan_data_source = "plan_data_source"
    announcement_data_source = "announcement_data_source"
    file = "file"

    # relationships
    place = "place"
    auth_entity = "auth_entity"
    none = ""


class CourtChallengeFields(str, Enum):
    id = "id"
    matter_numbers = "matter_numbers"
    case_name = "case_name"
    case_number = "case_number"
    court = "court"
    jurisdiction = "jurisdiction"
    filed_in_state_or_federal_court = "filed_in_state_or_federal_court"
    government_order_upheld_or_enjoined = "government_order_upheld_or_enjoined"
    summary_of_action = "summary_of_action"
    policy_or_law_name = "policy_or_law_name"
    parties = "parties"
    legal_citation = "legal_citation"
    holding = "holding"
    complaint_category = "complaint_category"
    data_source_for_complaint = "data_source_for_complaint"
    data_source_for_decision = "data_source_for_decision"
    date_of_decision = "date_of_decision"
    date_of_complaint = "date_of_complaint"
    parties_or_citation_and_summary_of_action = (
        "parties_or_citation_and_summary_of_action"
    )
    policy_status = "policy_status"
    case_status = "case_status"

    # related entities
    policies = "policies"
    none = ""


class PlaceFields(str, Enum):
    id = "id"
    level = "level"
    iso3 = "iso3"
    country_name = "country_name"
    area1 = "area1"
    area2 = "area2"
    loc = "loc"
    home_rule = "home_rule"
    dillons_rule = "dillons_rule"
    policies = "policies"
    plans = "plans"
    auth_entities = "auth_entities"
    observations = "observations"
    policy_numbers = "policy_numbers"


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
    ansi_fips: str = None


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


class Court_Challenge(BaseModel):
    id: int = None
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
    policies: List["Policy"] = None


class Policy(BaseModel):
    id: int = None

    # descriptive information
    policy_name: str = None
    policy_number: int = None
    desc: str = None
    name_and_desc: str = None
    primary_ph_measure: str = None
    ph_measure_details: str = None
    policy_type: str = None
    authority_name: str = None
    subtarget: List[str] = None

    # key dates
    date_issued: date = None
    date_start_effective: date = None
    date_end_anticipated: date = None
    date_end_actual: date = None
    # enum_test: State = None

    # relationships
    # court_challenges: List[Court_Challenge] = None
    auth_entity: List[Auth_Entity] = None
    place: List[Place] = None
    file: List = None
    n: int = None


class PolicyResponse(Response):
    data: List[Policy]


T = TypeVar("T")


class EntityResponse(Response, Generic[T]):
    data: List[T]


Court_Challenge.update_forward_refs()


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


class PlaceObs(BaseModel):
    place_id: int = None
    place_name: str = None
    value: int
    datestamp: date = None


class PlaceObsList(Response):
    """Return observations for places as list, along with optional min and max
    data for all time.

    """

    data: List[PlaceObs]
    min_all_time: PlaceObs = None
    max_all_time: PlaceObs = None


examplePolicyFilter: Dict[str, List[str]] = {
    "dates_in_effect": [
        "2019-12-31",
        "2022-12-31",
    ],
    "country_name": ["United States of America (USA)"],
    "area1": ["California"],
    "area2": ["Alameda County, CA"],
    "primary_ph_measure": ["Social distancing"],
    "ph_measure_details": ["Adaptation and mitigation measures"],
    "subtarget": ["All essential businesses"],
    "text": [],
}

examplePlanFilter = {
    "date_issued": [
        "2019-12-31",
        "2022-12-31",
    ],
    "area1": ["California"],
    "area2": ["Office of the Chancellor"],
    "org_type": ["University"],
    "text": [],
}


class ExportFiltersNoOrdering(BaseModel):
    filters: Optional[Dict[str, List]] = Field(
        {},
        title="Filters to be applied",
        description="Key: Name of data field on which to filter. Values: List"
        " of strings of values the data field may have.",
    )


class PolicyFiltersFields(BaseModel):
    dates_in_effect: Optional[List[date]] = list()
    country_name: Optional[List[str]] = list()
    iso3: Optional[List[str]] = list()
    area1: Optional[List[str]] = list()
    area2: Optional[List[str]] = list()
    primary_ph_measure: Optional[List[str]] = list()
    subtarget: Optional[List[str]] = list()
    text: Optional[List[str]] = list()

    class Config:
        fields = {"text": "_text"}


class PlanFiltersFields(BaseModel):
    date_issued: Optional[List[date]] = list()
    area1: Optional[List[str]] = list()
    area2: Optional[List[str]] = list()
    org_type: Optional[List[str]] = list()
    text: Optional[List[str]] = list()

    class Config:
        fields = {"text": "_text"}


class PolicyFilters(BaseModel):
    filters: Optional[PolicyFiltersFields] = Field(
        examplePolicyFilter,
        title="Filters to be applied",
        description="Key: Name of data field on which to filter. Values: List"
        " of strings of values the data field may have.",
    )


class PlanFilters(BaseModel):
    filters: Optional[PlanFiltersFields] = Field(
        examplePlanFilter,
        title="Filters to be applied",
        description="Key: Name of data field on which to filter. Values: List"
        " of strings of values the data field may have.",
    )


class ChallengeFilters(BaseModel):
    filters: Optional[Dict[str, List]] = Field(
        {"date_of_complaint": ["2019-12-31", "2022-12-31"]},
        title="Filters to be applied",
        description="Key: Name of data field on which to filter. Values: List"
        " of strings of values the data field may have.",
    )


class PolicyBody(PolicyFilters):
    ordering: List[list] = [["id", "asc"]]


class PlanBody(PlanFilters):
    ordering: List[list] = [["id", "asc"]]


class ChallengeBody(ChallengeFilters):
    ordering: List[list] = [["id", "asc"]]


class PolicyList(Response):
    data: List[Policy]


class PolicyNumberList(Response):
    data: List[PolicyNumber]


class ChallengeList(Response):
    data: List[Court_Challenge]


class PlanList(Response):
    data: List[Plan]


class PolicyDict(Response):
    data: Dict[str, List[Policy]]


class PolicyStatusList(Response):
    data: List[PolicyStatus]


class PolicyStatusCountList(Response):
    data: List[PlaceObs]


class OptionSetRecord(BaseModel):
    id: int
    value: Any
    label: Any = None
    group: Any = None


OptionSetRecords = Dict[str, List[OptionSetRecord]]


class OptionSetList(Response):
    data: OptionSetRecords


class Metadata(BaseModel):
    class_name: Optional[str] = None
    colgroup: Optional[str] = None
    definition: Optional[str] = None
    display_name: Optional[str] = None
    entity_name: Optional[str] = None
    export: Optional[bool] = None
    field: Optional[str] = None
    ingest_field: Optional[str] = None
    order: Optional[float] = None
    possible_values: Optional[str] = None
    table_name: Optional[str] = None
    tooltip: Optional[str] = None


MetadataRecords = Dict[str, Metadata]


class MetadataList(Response):
    data: MetadataRecords


class Version(BaseModel):
    name: str
    date: Union[date, str]
    last_datum_date: Optional[Union[date, str]] = None
    map_types: List[str]


class VersionResponse(Response):
    data: List[Version]


class CountResponse(Response):
    data: Dict[str, int]


class Iso3Codes(str, Enum):
    all_countries = "All countries"
    ABW = "ABW"
    AFG = "AFG"
    AGO = "AGO"
    AIA = "AIA"
    ALA = "ALA"
    ALB = "ALB"
    AND = "AND"
    ARE = "ARE"
    ARG = "ARG"
    ARM = "ARM"
    ASM = "ASM"
    ATA = "ATA"
    ATF = "ATF"
    ATG = "ATG"
    AUS = "AUS"
    AUT = "AUT"
    AZE = "AZE"
    BDI = "BDI"
    BEL = "BEL"
    BEN = "BEN"
    BES = "BES"
    BFA = "BFA"
    BGD = "BGD"
    BGR = "BGR"
    BHR = "BHR"
    BHS = "BHS"
    BIH = "BIH"
    BLM = "BLM"
    BLR = "BLR"
    BLZ = "BLZ"
    BMU = "BMU"
    BOL = "BOL"
    BRA = "BRA"
    BRB = "BRB"
    BRN = "BRN"
    BTN = "BTN"
    BWA = "BWA"
    CAF = "CAF"
    CAN = "CAN"
    CHE = "CHE"
    CHL = "CHL"
    CHN = "CHN"
    CIV = "CIV"
    CMR = "CMR"
    COD = "COD"
    COG = "COG"
    COK = "COK"
    COL = "COL"
    COM = "COM"
    CPV = "CPV"
    CRI = "CRI"
    CUB = "CUB"
    CUW = "CUW"
    CYM = "CYM"
    CYP = "CYP"
    CZE = "CZE"
    DEU = "DEU"
    DJI = "DJI"
    DMA = "DMA"
    DNK = "DNK"
    DOM = "DOM"
    DZA = "DZA"
    EAZ = "EAZ"
    ECU = "ECU"
    EGY = "EGY"
    ERI = "ERI"
    ESH = "ESH"
    ESP = "ESP"
    EST = "EST"
    ETH = "ETH"
    FIN = "FIN"
    FJI = "FJI"
    FLK = "FLK"
    FRA = "FRA"
    FRO = "FRO"
    FSM = "FSM"
    GAB = "GAB"
    GBR = "GBR"
    GB_NIR = "GB-NIR"
    GB_ENG = "GB-ENG"
    GB_SCT = "GB-SCT"
    GB_WLS = "GB-WLS"
    GEO = "GEO"
    GGY = "GGY"
    GHA = "GHA"
    GIB = "GIB"
    GIN = "GIN"
    GLP = "GLP"
    GMB = "GMB"
    GNB = "GNB"
    GNQ = "GNQ"
    GRC = "GRC"
    GRD = "GRD"
    GRL = "GRL"
    GTM = "GTM"
    GUF = "GUF"
    GUM = "GUM"
    GUY = "GUY"
    HKG = "HKG"
    HMD = "HMD"
    HND = "HND"
    HRV = "HRV"
    HTI = "HTI"
    HUN = "HUN"
    IDN = "IDN"
    IMN = "IMN"
    IND = "IND"
    IOT = "IOT"
    IRL = "IRL"
    IRN = "IRN"
    IRQ = "IRQ"
    ISL = "ISL"
    ISR = "ISR"
    ITA = "ITA"
    JAM = "JAM"
    JEY = "JEY"
    JOR = "JOR"
    JPN = "JPN"
    KAZ = "KAZ"
    KEN = "KEN"
    KGZ = "KGZ"
    KHM = "KHM"
    KIR = "KIR"
    KNA = "KNA"
    KOR = "KOR"
    KWT = "KWT"
    LAO = "LAO"
    LBN = "LBN"
    LBR = "LBR"
    LBY = "LBY"
    LCA = "LCA"
    LIE = "LIE"
    LKA = "LKA"
    LSO = "LSO"
    LTU = "LTU"
    LUX = "LUX"
    LVA = "LVA"
    MAC = "MAC"
    MAF = "MAF"
    MAR = "MAR"
    MCO = "MCO"
    MDA = "MDA"
    MDG = "MDG"
    MDV = "MDV"
    MEX = "MEX"
    MHL = "MHL"
    MKD = "MKD"
    MLI = "MLI"
    MLT = "MLT"
    MMR = "MMR"
    MNE = "MNE"
    MNG = "MNG"
    MNP = "MNP"
    MOZ = "MOZ"
    MRT = "MRT"
    MSR = "MSR"
    MUS = "MUS"
    MWI = "MWI"
    MYS = "MYS"
    NAM = "NAM"
    NCL = "NCL"
    NER = "NER"
    NFK = "NFK"
    NGA = "NGA"
    NIC = "NIC"
    NIU = "NIU"
    NLD = "NLD"
    NOR = "NOR"
    NPL = "NPL"
    NRU = "NRU"
    NZL = "NZL"
    OMN = "OMN"
    PAK = "PAK"
    PAN = "PAN"
    PCN = "PCN"
    PER = "PER"
    PHL = "PHL"
    PLW = "PLW"
    PNG = "PNG"
    POL = "POL"
    PRI = "PRI"
    PRK = "PRK"
    PRT = "PRT"
    PRY = "PRY"
    PSE = "PSE"
    PYF = "PYF"
    QAT = "QAT"
    REU = "REU"
    ROU = "ROU"
    RUS = "RUS"
    RWA = "RWA"
    SAU = "SAU"
    SDN = "SDN"
    SEN = "SEN"
    SGP = "SGP"
    SGS = "SGS"
    SHN = "SHN"
    SLB = "SLB"
    SLE = "SLE"
    SLV = "SLV"
    SMR = "SMR"
    SOM = "SOM"
    SPM = "SPM"
    SRB = "SRB"
    SSD = "SSD"
    STP = "STP"
    SUR = "SUR"
    SVK = "SVK"
    SVN = "SVN"
    SWE = "SWE"
    SWZ = "SWZ"
    SXM = "SXM"
    SYC = "SYC"
    SYR = "SYR"
    TCA = "TCA"
    TCD = "TCD"
    TGO = "TGO"
    THA = "THA"
    TJK = "TJK"
    TKM = "TKM"
    TLS = "TLS"
    TON = "TON"
    TTO = "TTO"
    TUN = "TUN"
    TUR = "TUR"
    TUV = "TUV"
    TWN = "TWN"
    TZA = "TZA"
    UGA = "UGA"
    UKR = "UKR"
    UMI = "UMI"
    URY = "URY"
    USA = "USA"
    UZB = "UZB"
    VAT = "VAT"
    VCT = "VCT"
    VEN = "VEN"
    VGB = "VGB"
    VIR = "VIR"
    VNM = "VNM"
    VUT = "VUT"
    WLF = "WLF"
    WSM = "WSM"
    YEM = "YEM"
    ZAF = "ZAF"
    ZMB = "ZMB"
    ZWE = "ZWE"
    EU = "EU"
    GLOBAL = "GLOBAL"
    none = ""


StateNames = Enum(
    value="StateNames",
    names=[
        ("All states and territories", "All states and territories"),
        ("Alabama", "Alabama"),
        ("Alaska", "Alaska"),
        ("American Samoa", "American Samoa"),
        ("Arizona", "Arizona"),
        ("Arkansas", "Arkansas"),
        ("California", "California"),
        ("Colorado", "Colorado"),
        ("Connecticut", "Connecticut"),
        ("Delaware", "Delaware"),
        ("District of Columbia", "District of Columbia"),
        ("Florida", "Florida"),
        ("Georgia", "Georgia"),
        ("Guam", "Guam"),
        ("Hawaii", "Hawaii"),
        ("Idaho", "Idaho"),
        ("Illinois", "Illinois"),
        ("Indiana", "Indiana"),
        ("Iowa", "Iowa"),
        ("Kansas", "Kansas"),
        ("Kentucky", "Kentucky"),
        ("Louisiana", "Louisiana"),
        ("Maine", "Maine"),
        ("Maryland", "Maryland"),
        ("Massachusetts", "Massachusetts"),
        ("Michigan", "Michigan"),
        ("Minnesota", "Minnesota"),
        ("Mississippi", "Mississippi"),
        ("Missouri", "Missouri"),
        ("Montana", "Montana"),
        ("Nebraska", "Nebraska"),
        ("Nevada", "Nevada"),
        ("New Hampshire", "New Hampshire"),
        ("New Jersey", "New Jersey"),
        ("New Mexico", "New Mexico"),
        ("New York", "New York"),
        ("North Carolina", "North Carolina"),
        ("North Dakota", "North Dakota"),
        ("Northern Mariana Islands", "Northern Mariana Islands"),
        ("Ohio", "Ohio"),
        ("Oklahoma", "Oklahoma"),
        ("Oregon", "Oregon"),
        ("Pennsylvania", "Pennsylvania"),
        ("Puerto Rico", "Puerto Rico"),
        ("Rhode Island", "Rhode Island"),
        ("South Carolina", "South Carolina"),
        ("South Dakota", "South Dakota"),
        ("Tennessee", "Tennessee"),
        ("Texas", "Texas"),
        ("US Virgin Islands", "US Virgin Islands"),
        ("Unspecified", "Unspecified"),
        ("Utah", "Utah"),
        ("Vermont", "Vermont"),
        ("Virginia", "Virginia"),
        ("Washington", "Washington"),
        ("West Virginia", "West Virginia"),
        ("Wisconsin", "Wisconsin"),
        ("Wyoming", "Wyoming"),
        ("none", ""),
    ],
)
