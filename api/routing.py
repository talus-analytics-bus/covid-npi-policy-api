"""Define API endpoints"""
# standard modules
from api.ampresolvers.optionsetgetter.core import OptionSetGetter
from api.types import ClassName, GeoRes, GeoResCountryState
from api.ampresolvers import PolicyStatusCounter
from datetime import date
from enum import Enum

# 3rd party modules
from fastapi import Query, Path, Response
from starlette.responses import RedirectResponse
from typing import List

# local modules
from . import routing_custom  # noqa F401
from . import schema
from .models import (
    EntityResponse,
    Place,
    PlaceObsList,
    Plan,
    Policy,
    PolicyBody,
    PlanBody,
    OptionSetList,
    MetadataList,
    ListResponse,
    PolicyStatusList,
    PolicyFields,
    PlanFields,
    PlaceFields,
    Iso3Codes,
    StateNames,
    ExportFiltersNoOrdering,
    VersionResponse,
)
from . import helpers
from . import app
from db import db  # noqa F401

DOWNLOAD_DESCRIPTION = (
    "**Note:** This endpoint results in a File download "
    "and may only work if you make the API request either (1) in your address"
    " bar or (2) using cURL."
)

ClassNameExport = Enum(
    value="ClassNameExport",
    names=[
        ("all_static", "All_data"),
        ("all_static_summary", "All_data_summary"),
        ("PolicySummary", "PolicySummary"),
        ("Policy", "Policy"),
        ("Plan", "Plan"),
        # ("Court_Challenge", "Court_Challenge"),
        ("All_data_recreate", "All_data_recreate"),
        ("All_data_recreate_summary", "All_data_recreate_summary"),
        ("none", ""),
    ],
)

export_defs: List[list] = [
    [
        "All_data",
        "Returns data of all types including Policies and Plans, and includes"
        " all data fields that can be exported",
    ],
    [
        "All_data_summary",
        "Returns data of all types including Policies and Plans, and includes"
        " a compact subset of data fields",
    ],
    [
        "PolicySummary",
        "Returns data for Policies only, compact subset of fields",
    ],
    [
        "Policy",
        "Returns data for Policies only, all fields",
    ],
    [
        "Plan",
        "Returns data for Plans only, all fields",
    ],
    [
        "All_data_recreate",
        "ADVANCED, do not use: Same as `All_data` option but will not use"
        " cached export",
    ],
    [
        "All_data_recreate_summary",
        "ADVANCED, do not use: Same as `All_data_summary` option but will not"
        " use cached export",
    ],
]

EXCEL_EXPORT_FILTERS_DESCR = "".join(
    [f"<li><strong>{label}</strong>: {val}</li>" for label, val in export_defs]
)


@app.post(
    "/export",
    tags=["Downloads"],
    summary="Return Excel (.xlsx) File containing formatted data for all "
    'records belonging to the provided class, e.g., "Policy" or "Plan"'
    " that match filters.",
    description=DOWNLOAD_DESCRIPTION
    + """ <br/><br/>**Example:** to download all face mask Policies:<br/><br/>
    ```curl -X POST "https://api.covidamp.org/"""
    """post/export?class_name=Policy" -H  "accept: application/json" -H"""
    """  "Content-Type: application/json" -d """
    """"{\"filters\":{\"primary_ph_measure\":[\"Face mask\"]}}```""",
)
@app.post(
    "/post/export",
    tags=["Downloads"],
    summary="Return Excel (.xlsx) File containing formatted data for all "
    'records belonging to the provided class, e.g., "Policy" or "Plan"'
    " that match filters.",
    description=DOWNLOAD_DESCRIPTION
    + """ <br/><br/>**Example:** to download all face mask Policies:<br/><br/>
    ```curl -X POST "https://api.covidamp.org/"""
    """post/export?class_name=Policy" -H  "accept: application/json" -H"""
    """  "Content-Type: application/json" -d """
    """"{\"filters\":{\"primary_ph_measure\":[\"Face mask\"]}}```""",
    include_in_schema=False,
)
async def post_export(
    body: ExportFiltersNoOrdering,
    class_name: ClassNameExport = Query(
        ClassNameExport.all_static,
        description="The name of the data type for which an Excel export "
        "is requested. Use one of the following options:<ul>"
        f"{EXCEL_EXPORT_FILTERS_DESCR}</ul>",
    ),
):
    """Return XLSX data export for Policies with the given filters applied.

    Parameters
    ----------
    filters : dict
        The filters to apply.

    Returns
    -------
    fastapi.responses.Response
        The XLSX data export File.

    """
    if class_name == "All_data":
        class_name = "all_static"
    if class_name == "All_data_summary":
        class_name = "all_static_summary"
    if class_name == ClassNameExport.none or class_name is None:
        raise NotImplementedError(
            "Must provide a `class_name` to /post/export"
        )
    filters = body.filters if bool(body.filters) is True else None
    return schema.export(filters=filters, class_name=class_name.name)


@app.get(
    "/version",
    tags=["Metadata"],
    summary="Return dates different data types were last updated, and the "
    "most recent date appearing in the data",
    response_model=VersionResponse,
)
@app.get(
    "/get/version",
    include_in_schema=False,
)
async def get_version():
    return schema.get_version()


@app.get(
    "/countries_with_lockdown_levels",
    tags=["Metadata"],
    summary="Return ISO 3166-1 alpha-3 codes of countries for which "
    "national-level Policy data are currently available in AMP",
    response_model=EntityResponse[str],
)
@app.get(
    "/get/countries_with_lockdown_levels",
    tags=["Metadata"],
    summary="Return ISO 3166-1 alpha-3 codes of countries for which "
    "national-level Policy data are currently available in AMP",
    include_in_schema=False,
)
async def get_countries_with_lockdown_levels():
    return schema.get_countries_with_lockdown_levels()


@app.get(
    "/count",
    tags=["Metadata"],
    summary="Return the total number of records currently in AMP of the "
    'provided class(es), e.g., "Policy" or "Plan".',
)
@app.get(
    "/get/count",
    tags=["Metadata"],
    summary="Return the total number of records currently in AMP of the "
    'provided class(es), e.g., "Policy" or "Plan".',
    include_in_schema=False,
)
async def get_count(
    class_names: List[ClassName] = Query(
        [ClassName.Policy],
        description="The name(s) of the data type(s) for which record counts "
        "are requested",
    )
):
    class_names = [v.name for v in class_names if v != ClassNameExport.none]
    if len(class_names) == 0:
        raise NotImplementedError("Must provide a `class_name` to /get/count")
    return schema.get_count(class_names=class_names)


@app.get(
    "/metadata",
    response_model=MetadataList,
    response_model_exclude_unset=True,
    tags=["Metadata"],
    summary="Return metadata describing the provided field(s), e.g, "
    '"Policy.policy_name" belonging to the provided class, e.g., "Policy"'
    ' or "Plan".',
)
@app.get(
    "/get/metadata",
    response_model=MetadataList,
    response_model_exclude_unset=True,
    tags=["Metadata"],
    summary="Return metadata describing the provided field(s), e.g, "
    '"Policy.policy_name" belonging to the provided class, e.g., "Policy"'
    ' or "Plan".',
    include_in_schema=False,
)
async def get_metadata(
    entity_class_name: ClassName = Query(
        ClassName.Policy,
        description="The name of the data type for which metadata"
        " are requested",
    ),
    fields: List[str] = Query(
        [
            "Policy.policy_name",
            "Policy.primary_ph_measure",
            "Policy.ph_measure_details",
        ],
        description="A list of fields for which metadata are requested,"
        " prefixed by the data type name and a period",
    ),
):
    entity_class_name = (
        entity_class_name if entity_class_name != ClassName.none else None
    )
    if entity_class_name is None:
        raise NotImplementedError(
            "Must provide a `entity_class_name` to /get/metadata"
        )
    return schema.get_metadata(
        fields=fields, entity_class_name=entity_class_name.name
    )


@app.get("/file/redirect", tags=["Downloads"], include_in_schema=False)
@app.get("/get/file/redirect", include_in_schema=False)
async def get_file_redirect(id: int):
    """Return File from S3 with the matching ID using the provided title.

    Parameters
    ----------
    id : int
        ID of File instance.
    title : str
        Title to give File.

    Returns
    -------
    fastapi.responses.Response
        The File.

    """
    title = schema.get_file_title(id)
    return RedirectResponse(url=f"""/get/file/{title}?id={id}""")


@app.get(
    "/file/{title}",
    tags=["Downloads"],
    summary="Download PDF with the given ID, using the provided title as "
    "the filename",
    include_in_schema=False,
)
@app.get(
    "/get/file/{title}",
    include_in_schema=False,
)
async def get_file_title_required(
    id: int = Query(
        None,
        description="Unique ID of File, as listed in `file` attribute of "
        "Policy records",
    ),
    title: str = Query("Filename", description="Any filename"),
):
    return schema.get_file(id)


@app.get(
    "/file/{id}/{title}",
    tags=["Downloads"],
    summary="Download PDF with the given ID, using the provided title as"
    " the filename",
    description=DOWNLOAD_DESCRIPTION,
)
@app.get(
    "/get/file/{id}/{title}",
    include_in_schema=False,
)
async def get_file(
    id: int = Query(
        None,
        description="Unique ID of File, as listed in `file` attribute of "
        "Policy records",
    ),
    title: str = Query("Filename", description="Any filename"),
):
    return schema.get_file(id)


@app.get(
    "/get/policy",
    response_model=ListResponse,
    response_model_exclude_unset=True,
    tags=["Policies"],
    include_in_schema=False,
)
async def get_policy(
    fields: List[str] = Query(None),
    page: int = None,
    pagesize: int = 100,
    count: bool = False,
):
    """Return Policy data."""
    return schema.get_policy(
        fields=fields, page=page, pagesize=pagesize, count_only=count
    )


@app.post(
    "/policy",
    response_model=EntityResponse[Policy],
    response_model_exclude_unset=True,
    tags=["Policies"],
    summary="Return data for Policies matching filters",
)
@app.post(
    "/post/policy",
    response_model=ListResponse,
    response_model_exclude_unset=True,
    include_in_schema=False,
)
async def post_policy(
    body: PolicyBody,
    fields: List[PolicyFields] = Query(
        [PolicyFields.id],
        description="List of data fields that should be returned for"
        " each policy",
    ),
    page: int = Query(1, description="Page to return"),
    pagesize: int = Query(100, description="Number of records per page"),
    count: bool = Query(
        False,
        description="If true, return number of records only, otherwise return"
        " data for records",
    ),
    merge_like_policies: bool = Query(
        True,
        description="Applies only if `count` is true. If true, more"
        " accurately weights Policy counts by merging like Policies, e.g.,"
        " counting Policies that affected multiple types of commercial"
        " locations only once, etc. If false, counts each row in the Policy"
        " database without merging.",
    ),
    random: bool = Query(
        False,
        description="If true, return a random sampling of `pagesize` records,"
        " otherwise return according to `ordering` in body",
    ),
):
    fields = [
        v
        for v in fields
        if v not in (PolicyFields.none, PolicyFields.court_challenges_id)
    ]
    return schema.get_policy(
        filters=helpers.get_body_attr(body, "filters"),
        fields=fields,
        by_category=None,
        page=page,
        pagesize=pagesize,
        ordering=body.ordering,
        count_only=count,
        random=random,
        merge_like_policies=merge_like_policies,
    )


# @app.get(
#     "/get/challenge",
#     response_model=ListResponse,
#     response_model_exclude_unset=True,
#     tags=["Court challenges"],
#     include_in_schema=False,
#     summary="Return court challenges (to Policies) matching filters",
# )
# async def get_challenge(
#     fields: List[str] = Query(None),
#     page: int = None,
#     pagesize: int = 100,
# ):
#     """Return Court_Challenge data.

#     Parameters
#     ----------
#     fields : List[str]
#         Data fields to return.

#     Returns
#     -------
#     dict
#         Challenge response dictionary.

#     """
#     return schema.get_challenge(fields=fields, page=page, pagesize=pagesize)


class Level(str, Enum):
    Country = "Country"
    For_profit = "For-profit"
    Local = "Local"
    Local_plus_state_province = "Local plus state/province"
    Non_profit = "Non-profit"
    State_Province = "State / Province"
    Tribal_nation = "Tribal nation"
    University = "University"


@app.get(
    "/place",
    response_model=EntityResponse[Place],
    response_model_exclude_unset=True,
    tags=["Places"],
    summary="Return Places matching filters",
)
@app.get(
    "/get/place",
    response_model=ListResponse,
    response_model_exclude_unset=True,
    tags=["Places"],
    summary="Return Places matching filters",
    include_in_schema=False,
)
async def get_place(
    fields: List[PlaceFields] = Query(None),
    iso3: str = "",
    levels: List[Level] = Query(
        [Level.Country],
        description="The level(s) of Place that should be returned",
    ),
    ansi_fips: str = Query(
        "",
        description="The ANSI or FIPS code of the local area to be returned.",
    ),
    include_policy_count: bool = False,
    level: Level = Query(
        None,
        deprecated=True,
        description="The level of Place that should be returned."
        " Please use `levels` parameter instead.",
    ),
):
    if levels is None:
        if level != "" and level is not None:
            levels = [level]
    levels = [s.lower() for s in levels]

    """Return Place data."""
    return schema.get_place(
        fields=[d.name for d in fields] if fields is not None else None,
        iso3=iso3.lower(),
        levels=levels,
        ansi_fips=ansi_fips.strip(),
        include_policy_count=include_policy_count,
    )


@app.get(
    "/plan",
    response_model=ListResponse,
    response_model_exclude_unset=True,
    tags=["Plans"],
    include_in_schema=False,
)
@app.get(
    "/get/plan",
    response_model=ListResponse,
    response_model_exclude_unset=True,
    tags=["Plans"],
    include_in_schema=False,
)
async def get_plan(
    fields: List[str] = Query(None),
    page: int = None,
    pagesize: int = 100,
):
    """Return Plan data.

    Parameters
    ----------
    fields : List[str]
        Data fields to return.

    Returns
    -------
    dict
        Plan response dictionary.

    """
    return schema.get_plan(fields=fields, page=page, pagesize=pagesize)


@app.get(
    "/policy_status/{geo_res}",
    response_model=PolicyStatusList,
    response_model_exclude_unset=True,
    tags=["Policies"],
    include_in_schema=False,
)
@app.get(
    "/get/policy_status/{geo_res}",
    response_model=PolicyStatusList,
    response_model_exclude_unset=True,
    tags=["Policies"],
    include_in_schema=False,
)
async def get_policy_status(geo_res=str):
    """Return Policy data.

    Parameters
    ----------
    fields : List[str]
        Data fields to return.

    Returns
    -------
    dict
        Policy response dictionary.

    """
    return schema.get_policy_status(geo_res=geo_res)


@app.get(
    "/lockdown_level/model/{iso3}/{geo_res}/{name}/{end_date}",
    response_model=PolicyStatusList,
    response_model_exclude_unset=True,
    tags=["Distancing levels"],
    include_in_schema=False,
)
@app.get(
    "/get/lockdown_level/model/{iso3}/{geo_res}/{name}/{end_date}",
    response_model=PolicyStatusList,
    response_model_exclude_unset=True,
    tags=["Distancing levels"],
    include_in_schema=False,
)
async def get_lockdown_level_model(
    iso3=str,
    geo_res=str,
    end_date=str,
    name: str = None,
    deltas_only: bool = False,
):
    """Get lockdown level of a location by date."""
    return schema.get_lockdown_level(
        iso3=iso3,  # all or any ISO3 code
        geo_res=geo_res,  # country or state ?
        name=name,
        end_date=end_date,  # date
        deltas_only=deltas_only,  # bool
    )


@app.get(
    "/lockdown_level/country/{iso3}/{end_date}",
    response_model=PolicyStatusList,
    response_model_exclude_unset=True,
    tags=["Distancing levels"],
    include_in_schema=False,
)
@app.get(
    "/get/lockdown_level/country/{iso3}/{end_date}",
    response_model=PolicyStatusList,
    response_model_exclude_unset=True,
    tags=["Distancing levels"],
    include_in_schema=False,
)
async def get_lockdown_level_country(
    iso3=str, end_date=str, deltas_only: bool = False
):
    """Get lockdown level of a location by date."""
    return schema.get_lockdown_level(
        iso3=iso3,
        geo_res="country",
        end_date=end_date,
        deltas_only=deltas_only,
    )


@app.get(
    "/lockdown_level/map/{iso3}/{geo_res}/{date}",
    include_in_schema=False,
    response_model=PolicyStatusList,
    response_model_exclude_unset=True,
    tags=["Distancing levels"],
)
@app.get(
    "/get/lockdown_level/map/{iso3}/{geo_res}/{date}",
    include_in_schema=False,
    response_model=PolicyStatusList,
    response_model_exclude_unset=True,
    tags=["Distancing levels"],
)
async def get_lockdown_level_map(iso3=str, geo_res=str, date=date):
    return schema.get_lockdown_level(iso3=iso3, geo_res=geo_res, date=date)


@app.post(
    "/policy_status/{geo_res}",
    response_model=PolicyStatusList,
    response_model_exclude_unset=True,
    tags=["Policies"],
    summary="Return whether or not ('t' or 'f') Policies were in effect by"
    " location which match the filters and the provided geographic resolution",
)
@app.post(
    "/post/policy_status/{geo_res}",
    response_model=PolicyStatusList,
    response_model_exclude_unset=True,
    tags=["Policies"],
    summary="Return whether or not ('t' or 'f') Policies were in effect by"
    " location which match the filters and the provided geographic resolution",
    include_in_schema=False,
)
async def post_policy_status(
    body: PolicyBody,
    geo_res: GeoRes = Query(
        GeoRes.state,
        description="The geographic resolution for which to return data",
    ),
):
    return schema.get_policy_status(geo_res=geo_res, filters=body.filters)


policy_status_counter: PolicyStatusCounter = PolicyStatusCounter()


@app.post(
    "/policy_status_counts/{geo_res}",
    response_model=PlaceObsList,
    response_model_exclude_unset=True,
    tags=["Policies"],
    summary="Return number of Policies in effect by location matching filters"
    " and the provided geographic resolution",
)
@app.post(
    "/post/policy_status_counts/{geo_res}",
    response_model=PlaceObsList,
    response_model_exclude_unset=True,
    tags=["Policies"],
    summary="Return number of Policies in effect by location matching filters"
    " and the provided geographic resolution",
    include_in_schema=False,
)
async def post_policy_status_counts(
    body: PolicyBody,
    geo_res: GeoRes = Path(
        GeoRes.state,
        description="The geographic resolution for which to return data",
    ),
    include_zeros: bool = Query(
        False,
        description="If true, include zeros if a Place has Policies but not"
        " for the currently selected filters. If false, these Places will not"
        " be included in results.",
    ),
    include_min_max: bool = Query(
        False,
        description="If true, include which observations represent the minimum"
        " and maximum values of the Policy status counts that have ever"
        " occurred over all dates. If false, do not include.",
    ),
    count_min_max_by_cat: bool = Query(
        False,
        description="If true, computes min/max Policy counts taking into "
        "account and category (`primary_ph_measure`) or subcategory "
        "(`ph_measure_details`) filters provided in the request body. If false"
        ", only computes min/max Policy counts across all categories.",
    ),
    count_sub: bool = Query(
        False,
        description="If true, counts all Policies *beneath* the selected"
        " `geo_res` (geographic resolution). If false, only counts Policies"
        " *at* it.",
    ),
    counted_parent_geos: List[GeoRes] = Query(
        list(),
        description="If defined, adds counts for the defined parent "
        "geographies to the count of the geography defined in `geo_res`. "
        "Otherwise, only counts for `geo_res` are returned.",
    ),
    merge_like_policies: bool = Query(
        True,
        description="If true, more accurately weights Policy counts by"
        " merging like Policies, e.g., counting Policies that affected"
        " multiple types of commercial locations only once, etc. If false, "
        "counts each row in the Policy database without merging.",
    ),
    one: bool = Query(
        False,
        description="If true, return first observation only.",
    ),
):
    res = policy_status_counter.get_policy_status_counts(
        geo_res=geo_res,
        filters=body.filters,
        by_group_number=merge_like_policies,
        filter_by_subgeo=count_sub,
        include_zeros=include_zeros,
        include_min_max=include_min_max,
        count_min_max_by_cat=count_min_max_by_cat,
        one=one,
        counted_parent_geos=counted_parent_geos,
    )
    return res


@app.post(
    "/policy_number",
    response_model=ListResponse,
    response_model_exclude_unset=True,
    include_in_schema=False,
    tags=["Advanced"],
)
@app.post(
    "/post/policy_number",
    response_model=ListResponse,
    response_model_exclude_unset=True,
    include_in_schema=False,
    tags=["Advanced"],
)
async def post_policy_number(
    body: PolicyBody,
    fields: List[str] = Query(None),
    page: int = None,
    pagesize: int = 100,
):
    """Return Policy number metadata."""
    return schema.get_policy_number(
        filters=body.filters,
        fields=fields,
        by_category=None,
        page=page,
        pagesize=pagesize,
        ordering=body.ordering,
    )


# @app.post(
#     "/post/challenge",
#     response_model=ListResponse,
#     response_model_exclude_unset=True,
#     tags=["Court challenges"],
#     summary="Return data for court challenges "
#       "(to Policies) matching filters",
# )
# async def post_challenge(
#     body: ChallengeFilters,
#     fields: List[CourtChallengeFields] = Query(
#         [CourtChallengeFields.id],
#         description="List of data fields that should be returned for "
#           "each court challenge",
#     ),
#     page: int = Query(1, description="Page to return"),
#     pagesize: int = Query(100, description="Number of records per page"),
# ):
#     fields = [v for v in fields if v != CourtChallengeFields.none]
#     return schema.get_challenge(
#         filters=body.filters,
#         fields=fields,
#         by_category=None,
#         page=page,
#         pagesize=pagesize,
#         ordering=body.ordering,
#     )


@app.post(
    "/plan",
    response_model=EntityResponse[Plan],
    response_model_exclude_unset=True,
    tags=["Plans"],
    summary="Return data for Plans matching filters",
)
@app.post(
    "/post/plan",
    response_model=ListResponse,
    response_model_exclude_unset=True,
    tags=["Plans"],
    summary="Return data for Plans matching filters",
    include_in_schema=False,
)
async def post_plan(
    body: PlanBody,
    fields: List[PlanFields] = Query(
        [PlanFields.id],
        description="List of data fields that should be returned for "
        "each Plan",
    ),
    page: int = Query(1, description="Page to return"),
    pagesize: int = Query(100, description="Number of records per page"),
):
    fields = [v for v in fields if v != PlanFields.none]
    return schema.get_plan(
        filters=helpers.get_body_attr(body, "filters"),
        fields=fields,
        by_category=None,
        page=page,
        pagesize=pagesize,
        ordering=body.ordering,
    )


# define standard param sets that are shared across some routes


def geo_res_def(default_val):
    return Query(
        default_val,
        description="The geographic resolution for which to return data",
    )


state_name_def = Query(
    getattr(StateNames, "All states and territories"),
    description='For "state" resolution: Which state(s) or territory(ies) '
    "to return",
)

iso3_def = Query(
    Iso3Codes.all_countries,
    description='For "country" resolution: Which country(ies) to return',
)


@app.get(
    "/optionset",
    response_model=OptionSetList,
    response_model_exclude_unset=True,
    tags=["Metadata"],
    summary="Return all possible values for the provided field(s), e.g, "
    '"Policy.policy_name" belonging to the provided class, e.g., "Policy"'
    ' or "Plan". If you define a geographic resolution and/or specific'
    " location, only optionsets and values for which that location has at"
    " least one corresponding Policy or Plan will be returned.",
)
@app.get(
    "/get/optionset",
    response_model=OptionSetList,
    response_model_exclude_unset=True,
    tags=["Metadata"],
    summary="Return all possible values for the provided field(s), e.g, "
    '"Policy.policy_name" belonging to the provided class, e.g., "Policy"'
    ' or "Plan".',
    include_in_schema=False,
)
async def get_optionset(
    class_name: ClassName = Query(
        ClassName.Policy,
        description="The name of the data type for which optionsets "
        "are requested",
    ),
    fields: List[str] = Query(
        ["Policy.primary_ph_measure", "Policy.ph_measure_details"],
        description="A list of fields for which optionsets are requested,"
        " prefixed by the data type name and a period",
    ),
    geo_res: GeoResCountryState = geo_res_def(None),
    state_name: StateNames = state_name_def,
    iso3: Iso3Codes = iso3_def,
):
    getter: OptionSetGetter = OptionSetGetter()
    return getter.get_optionset(
        fields=fields,
        class_name=class_name.name,
        geo_res=geo_res.name if geo_res is not None else None,
        state_name=state_name.name
        if (
            state_name is not None
            and state_name.name != "All states and territories"
        )
        else None,
        iso3=iso3.name
        if (iso3 is not None and iso3.name != "All countries")
        else None,
    )


##
# Test endpoints
##
@app.get("/test", include_in_schema=False)
async def get_test(test_param: str = "GET successful"):
    """Test GET endpoint.

    Parameters
    ----------
    test_param : str
        A message to be returned in the response if GET was successful.

    Returns
    -------
    list[dict]
        A message containing the value of `test_param` indicating the GET was
        successful.

    """
    return [{"success": True, "message": "GET test", "data": [test_param]}]


@app.get(
    "/distancing_levels",
    response_model=PolicyStatusList,
    response_model_exclude_unset=True,
    tags=["Distancing levels"],
    summary="""Get level of distancing (e.g., "Lockdown", "Partially open")"""
    """ that was in effect in a given US state or a given country on a"""
    """ given date.""",
)
@app.get(
    "/get/distancing_levels",
    response_model=PolicyStatusList,
    response_model_exclude_unset=True,
    tags=["Distancing levels"],
    include_in_schema=False,
)
async def get_distancing_levels(
    geo_res: GeoResCountryState = geo_res_def(GeoResCountryState.state),
    iso3: Iso3Codes = iso3_def,
    state_name: StateNames = state_name_def,
    date: date = Query(
        date.today(),
        description="The date for which data are requested, YYYY-MM-DD,"
        " defaults to today. If no data available, data for most recent date"
        " before this date are returned.",
    ),
    all_dates: bool = Query(
        False,
        description="If true, all dates up to and including `date` are"
        " returned, if false, only data for `date` are returned",
    ),
    deltas_only: bool = Query(
        False,
        description="If true, only dates on which the distancing level"
        " changed are returned, if false, dates returned are determined by"
        " `all_dates`",
    ),
):
    state_name = (
        state_name.name
        if state_name.name != ""
        and state_name.name != "All states and territories"
        else None
    )
    iso3 = (
        iso3.name
        if iso3.name != "" and iso3.name != "all_countries"
        else "all"
    )
    end_date = None if not all_dates else str(date)
    date = None if all_dates else str(date)

    if state_name is not None and geo_res != GeoResCountryState.state:
        return Response(
            "Cannot define `state_name` unless `geo_res` is `state`",
            status_code=400,
            media_type="text/plain",
        )
    elif iso3 is not None and geo_res != GeoResCountryState.country:
        return Response(
            "Cannot define `iso3` unless `geo_res` is `country`",
            status_code=400,
            media_type="text/plain",
        )

    return schema.get_lockdown_level(
        iso3=iso3,
        geo_res=geo_res.name,
        date=date,
        name=state_name,
        end_date=end_date,
        deltas_only=deltas_only,
    )


@app.get("/", include_in_schema=False)
async def get_default():
    return {
        "success": True,
        "data": None,
        "message": "This is the default route of the COVID AMP API that"
        " supports https://covidamp.org/. Visit route `/docs` to learn how to"
        " use COVID AMP's data in your own work.",
    }
