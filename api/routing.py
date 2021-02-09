"""Define API endpoints"""
# standard modules
from datetime import date
from enum import Enum

# 3rd party modules
from fastapi import Query
from starlette.responses import RedirectResponse
from typing import List, Optional
from pydantic import BaseModel

# local modules
from . import schema
from .models import (
    PolicyList, PolicyFilters, PlanFilters, ChallengeFilters, OptionSetList,
    MetadataList,
    ListResponse, PolicyStatusList, PolicyStatusCountList,
    PolicyFiltersNoOrdering, PolicyFields, PlanFields, CourtChallengeFields,
    PlaceFields, Iso3Codes, StateNames, ExportFiltersNoOrdering
)
from .app import app
from db import db

DOWNLOAD_DESCRIPTION = '**Note:** This endpoint results in a file download and may only work if you make the API request either (1) in your address bar or (2) using cURL.'

ClassNameExport = Enum(
    value='ClassNameExport',
    names=[
        ('all_static', 'All_data'),
        ('Policy', 'Policy'),
        ('Plan', 'Plan'),
        ('All_data_recreate', 'All_data_recreate'),
        # ('Court_Challenge', 'Court_Challenge'),
        ('none', ''),
    ]
)


ClassName = Enum(
    value='ClassName',
    names=[
        ('Policy', 'Policy'),
        ('Plan', 'Plan'),
        # ('Court_Challenge', 'Court_Challenge'),
        ('none', ''),
    ]
)


@app.post(
    "/post/export",
    tags=["Downloads"],
    summary="Return Excel (.xlsx) file containing formatted data for all records belonging to the provided class, e.g., \"Policy\" or \"Plan\" that match filters.",
    description=DOWNLOAD_DESCRIPTION + """ <br/><br/>**Example:** to download all face mask policies:<br/><br/>
    ```curl -X POST "https://api.covidamp.org/post/export?class_name=Policy" -H  "accept: application/json" -H  "Content-Type: application/json" -d "{\"filters\":{\"primary_ph_measure\":[\"Face mask\"]}}```"""
)
async def post_export(
    body: ExportFiltersNoOrdering,
    class_name: ClassNameExport = Query(
        ClassNameExport.all_static,
        description='The name of the data type for which an Excel export is requested',
    ),
):
    """Return XLSX data export for policies with the given filters applied.

    Parameters
    ----------
    filters : dict
        The filters to apply.

    Returns
    -------
    fastapi.responses.Response
        The XLSX data export file.

    """
    if class_name == 'All_data':
        class_name = 'all_static'
    if class_name == ClassNameExport.none or class_name is None:
        raise NotImplementedError('Must provide a `class_name` to /post/export')
    filters = body.filters if bool(body.filters) == True else None
    return schema.export(filters=filters, class_name=class_name.name)


@app.get(
    "/get/version",
    tags=["Metadata"],
    summary="Return dates different data types were last updated, and the most recent date appearing in the data",
)
async def get_version():
    return schema.get_version()


@app.get(
    "/get/countries_with_lockdown_levels",
    tags=["Metadata"],
    summary="Return ISO 3166-1 alpha-3 codes of countries for which national-level policy data are currently available in AMP",

)
async def get_countries_with_lockdown_levels():
    return schema.get_countries_with_lockdown_levels()

#
# class Item(BaseModel):
#     name: str
#     description: Optional[str] = Field(
#         None, title="The description of the item", max_length=300
#     )
#     price: float = Field(..., gt=0, description="The price must be greater than zero")
#     tax: Optional[float] = None


@app.get(
    "/get/count",
    tags=["Metadata"],
    summary="Return the total number of records currently in AMP of the provided class(es), e.g., \"Policy\" or \"Plan\".",

)
async def get_count(
    class_names: List[ClassName] = Query(
        [ClassName.Policy],
        description='The name(s) of the data type(s) for which record counts are requested'
    )
):
    class_names = [v.name for v in class_names if v != ClassNameExport.none]
    if len(class_names) == 0:
        raise NotImplementedError('Must provide a `class_name` to /get/count')
    return schema.get_count(class_names=class_names)


@app.get(
    "/get/metadata",
    response_model=MetadataList,
    response_model_exclude_unset=True,
    tags=["Metadata"],
    summary="Return metadata describing the provided field(s), e.g, \"Policy.policy_name\" belonging to the provided class, e.g., \"Policy\" or \"Plan\".",

)
async def get_metadata(
    entity_class_name: ClassName = Query(
        ClassName.Policy,
        description='The name of the data type for which metadata are requested'
    ),
    fields: List[str] = Query(
        ['Policy.policy_name', 'Policy.primary_ph_measure', 'Policy.ph_measure_details'],
        description='A list of fields for which metadata are requested, prefixed by the data type name and a period')
):
    entity_class_name = entity_class_name if entity_class_name != ClassName.none else None
    if entity_class_name is None:
        raise NotImplementedError('Must provide a `entity_class_name` to /get/metadata')
    return schema.get_metadata(
        fields=fields, entity_class_name=entity_class_name.name
    )


@ app.get("/get/file/redirect", tags=["Downloads"], include_in_schema=False)
async def get_file_redirect(id: int):
    """Return file from S3 with the matching ID using the provided title.

    Parameters
    ----------
    id : int
        ID of File instance.
    title : str
        Title to give file.

    Returns
    -------
    fastapi.responses.Response
        The file.

    """
    title = schema.get_file_title(id)
    return RedirectResponse(url=f'''/get/file/{title}?id={id}''')


@app.get(
    "/get/file/{title}",
    tags=["Downloads"],
    summary="Download PDF with the given id, using the provided title as the filename",
    include_in_schema=False
)
async def get_file_title_required(
    id: int = Query(
        None, description="Unique ID of file, as listed in `file` attribute of Policy records"),
    title: str = Query('Filename', description="Any filename")
):
    return schema.get_file(id)


@app.get(
    "/get/file/{id}/{title}",
    tags=["Downloads"],
    summary="Download PDF with the given id, using the provided title as the filename",
    description=DOWNLOAD_DESCRIPTION
)
async def get_file(
    id: int = Query(
        None, description="Unique ID of file, as listed in `file` attribute of Policy records"),
    title: str = Query('Filename', description="Any filename")
):
    return schema.get_file(id)


@ app.get("/get/policy", response_model=ListResponse, response_model_exclude_unset=True, tags=["Policies"], include_in_schema=False)
async def get_policy(
    fields: List[str] = Query(None),
    page: int = None,
    pagesize: int = 100,
    count: bool = False,
):
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
    return schema.get_policy(
        fields=fields, page=page, pagesize=pagesize, count_only=count
    )


@ app.post(
    "/post/policy",
    response_model=ListResponse,
    response_model_exclude_unset=True,
    tags=["Policies"],
    summary="Return data for policies matching filters",
)
async def post_policy(
    body: PolicyFilters,
    fields: List[PolicyFields] = Query(
        [PolicyFields.id],
        description='List of data fields that should be returned for each policy'
    ),
    page: int = Query(1, description='Page to return'),
    pagesize: int = Query(100, description='Number of records per page'),
    count: bool = Query(
        False, description='If true, return number of records only, otherwise return data for records'),
    random: bool = Query(
        False, description='If true, return a random sampling of `pagesize` records, otherwise return according to `ordering` in body'),
):
    fields = [
        v for v in fields if v not in (
            PolicyFields.none,
            PolicyFields.court_challenges_id
        )
    ]
    return schema.get_policy(
        filters=body.filters, fields=fields, by_category=None,
        page=page, pagesize=pagesize, ordering=body.ordering,
        count_only=count, random=random
    )


# @ app.get(
#     "/get/challenge",
#     response_model=ListResponse,
#     response_model_exclude_unset=True,
#     tags=["Court challenges"],
#     include_in_schema=False,
#     summary="Return court challenges (to policies) matching filters",
#
# )
# async def get_challenge(
#     fields: List[str] = Query(None),
#     page: int = None,
#     pagesize: int = 100,
# ):
#     """Return Court_Challenge data.
#
#     Parameters
#     ----------
#     fields : List[str]
#         Data fields to return.
#
#     Returns
#     -------
#     dict
#         Challenge response dictionary.
#
#     """
#     return schema.get_challenge(fields=fields, page=page, pagesize=pagesize)


@ app.get(
    "/get/place",
    response_model=ListResponse,
    response_model_exclude_unset=True,
    tags=["Places"],
    summary="Return places matching filters",

)
async def get_place(
    fields: List[PlaceFields] = Query(None),
    iso3: str = '',
    level: str = '',
    include_policy_count: bool = False,
):
    """Return Place data.

    """
    return schema.get_place(
        fields=[d.name for d in fields], iso3=iso3.lower(), level=level.lower(),
        include_policy_count=include_policy_count
    )


@ app.get("/get/plan", response_model=ListResponse, response_model_exclude_unset=True, tags=["Plans"], include_in_schema=False)
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


@ app.get("/get/policy_status/{geo_res}", response_model=PolicyStatusList, response_model_exclude_unset=True, tags=["Policies"], include_in_schema=False)
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


@ app.get("/get/lockdown_level/model/{iso3}/{geo_res}/{name}/{end_date}", response_model=PolicyStatusList, response_model_exclude_unset=True, tags=["Distancing levels"], include_in_schema=False)
async def get_lockdown_level_model(
    iso3=str,
    geo_res=str,
    end_date=str,
    name: str = None,
    deltas_only: bool = False
):
    """Get lockdown level of a location by date.

    """
    return schema.get_lockdown_level(
        iso3=iso3,  # all or any ISO3 code
        geo_res=geo_res,  # country or state ?
        name=name,
        end_date=end_date,  # date
        deltas_only=deltas_only,  # bool
    )


@ app.get("/get/lockdown_level/country/{iso3}/{end_date}", response_model=PolicyStatusList, response_model_exclude_unset=True, tags=["Distancing levels"], include_in_schema=False)
async def get_lockdown_level_country(
    iso3=str,
    end_date=str,
    deltas_only: bool = False
):
    """Get lockdown level of a location by date.

    """
    return schema.get_lockdown_level(
        iso3=iso3,
        geo_res='country',
        end_date=end_date,
        deltas_only=deltas_only
    )


@ app.get(
    "/get/lockdown_level/map/{iso3}/{geo_res}/{date}", include_in_schema=False,
    response_model=PolicyStatusList,
    response_model_exclude_unset=True,
    tags=["Distancing levels"]
)
async def get_lockdown_level_map(
    iso3=str,
    geo_res=str,
    date=date
):
    return schema.get_lockdown_level(iso3=iso3, geo_res=geo_res, date=date)


# define allowed geo_res values
class GeoRes(str, Enum):
    state = 'state'
    country = 'country'


@app.post(
    "/post/policy_status/{geo_res}",
    response_model=PolicyStatusList,
    response_model_exclude_unset=True,
    tags=["Policies"],
    summary="Return whether or not ('t' or 'f') policies were in effect by location which match the filters and the provided geographic resolution"

)
async def post_policy_status(
    body: PolicyFilters,
    geo_res: GeoRes = Query(GeoRes.state,
                            description='The geographic resolution for which to return data'
                            )
):
    return schema.get_policy_status(
        geo_res=geo_res,
        filters=body.filters
    )


@ app.post(
    "/post/policy_status_counts/{geo_res}",
    response_model=PolicyStatusCountList,
    response_model_exclude_unset=True,
    tags=["Policies"],
    summary="Return number of policies in effect by location matching filters and the provided geographic resolution"
)
async def post_policy_status_counts(
    body: PolicyFilters,
    geo_res: GeoRes = Query(GeoRes.state,
                            description='The geographic resolution for which to return data'
                            ),
    merge_like_policies: bool = Query(
        True,
        description="If true, more accurately weights policy counts by merging like policies, e.g., counting policies that affected multiple types of commercial locations only once, etc. If false, counts each row in the Policy database without merging."
    )
):
    res = schema.get_policy_status_counts(
        geo_res=geo_res, filters=body.filters,
        by_group_number=merge_like_policies)
    return res


@ app.post("/post/policy_number", response_model=ListResponse, response_model_exclude_unset=True, include_in_schema=False, tags=["Advanced"])
async def post_policy_number(
    body: PolicyFilters,
    fields: List[str] = Query(None),
    page: int = None,
    pagesize: int = 100,
):
    """Return Policy number metadata.

    """
    return schema.get_policy_number(
        filters=body.filters, fields=fields, by_category=None,
        page=page, pagesize=pagesize, ordering=body.ordering
    )


# @ app.post(
#     "/post/challenge",
#     response_model=ListResponse,
#     response_model_exclude_unset=True,
#     tags=["Court challenges"],
#     summary="Return data for court challenges (to policies) matching filters",
# )
# async def post_challenge(
#     body: ChallengeFilters,
#     fields: List[CourtChallengeFields] = Query(
#         [CourtChallengeFields.id],
#         description='List of data fields that should be returned for each court challenge'
#     ),
#     page: int = Query(1, description='Page to return'),
#     pagesize: int = Query(100, description='Number of records per page'),
# ):
#     fields = [v for v in fields if v != CourtChallengeFields.none]
#     return schema.get_challenge(
#         filters=body.filters, fields=fields, by_category=None,
#         page=page, pagesize=pagesize, ordering=body.ordering
#     )


@ app.post(
    "/post/plan",
    response_model=ListResponse,
    response_model_exclude_unset=True,
    tags=["Plans"],
    summary="Return data for plans matching filters",
)
async def post_plan(
    body: PlanFilters,
    fields: List[PlanFields] = Query(
        [PlanFields.id],
        description='List of data fields that should be returned for each plan'
    ),
    page: int = Query(1, description='Page to return'),
    pagesize: int = Query(100, description='Number of records per page'),
):
    fields = [v for v in fields if v != PlanFields.none]
    return schema.get_plan(
        filters=body.filters, fields=fields, by_category=None,
        page=page, pagesize=pagesize, ordering=body.ordering
    )

# define standard param sets that are shared across some routes


def geo_res_def(default_val): return Query(default_val,
                                           description='The geographic resolution for which to return data',
                                           )


state_name_def = Query(getattr(StateNames, 'All states and territories'),
                       description='For "state" resolution: Which state(s) or territory(ies) to return'
                       )

iso3_def = Query(Iso3Codes.all_countries,
                 description='For "country" resolution: Which country(ies) to return'
                 )


@ app.get(
    "/get/optionset",
    response_model=OptionSetList,
    tags=["Metadata"],
    summary="Return all possible values for the provided field(s), e.g, \"Policy.policy_name\" belonging to the provided class, e.g., \"Policy\" or \"Plan\".",

)
async def get_optionset(
    class_name: ClassName = Query(
        ClassName.Policy,
        description='The name of the data type for which optionsets are requested'
    ),
    fields: List[str] = Query(
        ['Policy.primary_ph_measure', 'Policy.ph_measure_details'],
        description='A list of fields for which optionsets are requested, prefixed by the data type name and a period'),
    geo_res: GeoRes = geo_res_def(None),
    state_name: StateNames = state_name_def,
    iso3: Iso3Codes = iso3_def,
):
    return schema.get_optionset(
        fields=fields,
        class_name=class_name.name,
        geo_res=geo_res.name if geo_res is not None else None,
        state_name=state_name.name if (
            state_name is not None and state_name.name != 'All states and territories') else None,
        iso3=iso3.name if (iso3 is not None and iso3.name != 'All countries') else None
    )


##
# Test endpoints
##
@ app.get("/test", include_in_schema=False)
async def get_test(test_param: str = 'GET successful'):
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
    return [{'success': True, 'message': 'GET test', 'data': [test_param]}]


@ app.get(
    "/get/distancing_levels",
    response_model=PolicyStatusList,
    response_model_exclude_unset=True,
    tags=["Distancing levels"]
)
async def get_distancing_levels(
    geo_res: GeoRes = geo_res_def(GeoRes.state),
    iso3: Iso3Codes = iso3_def,
    state_name: StateNames = state_name_def,
    date: date = Query(
        date.today(),
        description='The date for which data are requested, YYYY-MM-DD, defaults to today. If no data available, data for most recent date before this date are returned.'
    ),
    all_dates: bool = Query(
        False,
        description='If true, all dates up to and including `date` are returned, if false, only data for `date` are returned'
    ),
    deltas_only: bool = Query(
        False,
        description='If true, only dates on which the distancing level changed are returned, if false, dates returned are determined by `all_dates`'
    )
):
    state_name = state_name.name if state_name.name != '' and state_name.name != 'All states and territories' else None
    iso3 = iso3.name if iso3.name != '' and iso3.name != 'all_countries' else 'all'
    end_date = None if not all_dates else str(date)
    date = None if all_dates else str(date)
    return schema.get_lockdown_level(
        iso3=iso3,
        geo_res=geo_res.name,
        date=date,
        name=state_name,
        end_date=end_date,
        deltas_only=deltas_only
    )

# ##
# # Debug endpoints
# ##
#
#
# @app.get("/add_search_text")
# async def add_search_text(test_param: str = 'GET successful'):
#     """Test GET endpoint.
#
#     Parameters
#     ----------
#     test_param : str
#         A message to be returned in the response if GET was successful.
#
#     Returns
#     -------
#     list[dict]
#         A message containing the value of `test_param` indicating the GET was
#         successful.
#
#     """
#     schema.add_search_text()
#     return 'Done'
