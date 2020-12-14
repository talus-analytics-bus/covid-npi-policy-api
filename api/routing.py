"""Define API endpoints"""
# standard modules
from datetime import date

# 3rd party modules
from fastapi import Query
from starlette.responses import RedirectResponse
from typing import List, Optional

# local modules
from . import schema
from .models import PolicyList, PolicyFilters, OptionSetList, MetadataList, \
    ListResponse, PolicyStatusList, PolicyStatusCountList
from .app import app
from db import db


@app.post("/post/export")
async def export(
    body: PolicyFilters,
    class_name: str,
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
    filters = body.filters if bool(body.filters) == True else None
    return schema.export(filters=filters, class_name=class_name)


@app.get("/get/version")
async def get_version():
    return schema.get_version()


@app.get("/get/countries_with_lockdown_levels")
async def get_countries_with_lockdown_levels():
    return schema.get_countries_with_lockdown_levels()


@app.get("/get/count")
async def get_count(
    class_names: List[str] = Query(None),
):
    return schema.get_count(class_names=class_names)


@app.get("/get/metadata", response_model=MetadataList)
async def get_metadata(
    fields: List[str] = Query(None),
    entity_class_name: str = 'Policy'
):
    """Returns Metadata instance fields for the fields specified.

    Parameters
    ----------
    fields : list
        List of fields as strings with entity name prefixed, e.g.,
        `policy.id`.

    Returns
    -------
    dict
        Response containing metadata information for the fields.

    """
    return schema.get_metadata(
        fields=fields, entity_class_name=entity_class_name
    )


@app.get("/get/file/redirect")
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


@app.get("/get/file/{title}")
async def get_file(id: int, title: str):
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
    return schema.get_file(id)


@app.get("/get/policy", response_model=ListResponse, response_model_exclude_unset=True)
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


@app.get("/get/challenge", response_model=ListResponse, response_model_exclude_unset=True)
async def get_challenge(
    fields: List[str] = Query(None),
    page: int = None,
    pagesize: int = 100,
):
    """Return Court_Challenge data.

    Parameters
    ----------
    fields : List[str]
        Data fields to return.

    Returns
    -------
    dict
        Challenge response dictionary.

    """
    return schema.get_challenge(fields=fields, page=page, pagesize=pagesize)


@app.get("/get/plan", response_model=ListResponse, response_model_exclude_unset=True)
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


@app.get("/get/policy_status/{geo_res}", response_model=PolicyStatusList, response_model_exclude_unset=True)
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


@app.get("/get/lockdown_level/model/{iso3}/{geo_res}/{name}/{end_date}", response_model=PolicyStatusList, response_model_exclude_unset=True)
async def get_lockdown_level_model(
    iso3=str,
    geo_res=str,
    end_date=str,
    name=str,
    deltas_only: bool = False
):
    """Get lockdown level of a location by date.

    """
    return schema.get_lockdown_level(
        iso3=iso3,
        geo_res=geo_res,
        name=name,
        end_date=end_date,
        deltas_only=deltas_only,
    )


@app.get("/get/lockdown_level/country/{iso3}/{end_date}", response_model=PolicyStatusList, response_model_exclude_unset=True)
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


@app.get("/get/lockdown_level/map/{iso3}/{geo_res}/{date}", response_model=PolicyStatusList, response_model_exclude_unset=True)
async def get_lockdown_level_map(iso3=str, geo_res=str, date=date):
    """Get lockdown level of a location by date.

    """
    return schema.get_lockdown_level(iso3=iso3, geo_res=geo_res, date=date)


@app.post("/post/policy_status/{geo_res}", response_model=PolicyStatusList, response_model_exclude_unset=True)
async def post_policy_status(body: PolicyFilters, geo_res=str):
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

    return schema.get_policy_status(geo_res=geo_res, filters=body.filters)


@app.post("/post/policy_status_counts/{geo_res}", response_model=PolicyStatusCountList, response_model_exclude_unset=True)
async def post_policy_status(body: PolicyFilters, geo_res=str):
    """Return Policy status counts.

    Parameters
    ----------
    fields : List[str]
        Data fields to return.

    Returns
    -------
    dict
        Policy response dictionary.

    """
    res = schema.get_policy_status_counts(geo_res=geo_res, filters=body.filters)
    return res


@app.post("/post/policy", response_model=ListResponse, response_model_exclude_unset=True)
async def post_policy(
    body: PolicyFilters,
    by_category: str = None,
    fields: List[str] = Query(None),
    page: int = None,
    pagesize: int = 100,
    count: bool = False,
):
    """Return Policy data with filters applied.

    Parameters
    ----------
    body : PolicyFilters
        Filters to apply.
    fields : List[str]
        Data fields to return.

    Returns
    -------
    dict
        Policy response dictionary

    """
    return schema.get_policy(
        filters=body.filters, fields=fields, by_category=by_category,
        page=page, pagesize=pagesize, ordering=body.ordering,
        count_only=count
    )


@app.post("/post/policy_number", response_model=ListResponse, response_model_exclude_unset=True)
async def post_policy_number(
    body: PolicyFilters,
    by_category: str = None,
    fields: List[str] = Query(None),
    page: int = None,
    pagesize: int = 100,
):
    """Return Policy number metadata.

    """
    return schema.get_policy_number(
        filters=body.filters, fields=fields, by_category=by_category,
        page=page, pagesize=pagesize, ordering=body.ordering
    )


@app.post("/post/challenge", response_model=ListResponse, response_model_exclude_unset=True)
async def post_challenge(
    body: PolicyFilters,
    by_category: str = None,
    fields: List[str] = Query(None),
    page: int = None,
    pagesize: int = 100,
):
    """Return Challenge data with filters applied.

    Parameters
    ----------
    body : PolicyFilters
        Filters to apply.
    fields : List[str]
        Data fields to return.

    Returns
    -------
    dict
        Challenge response dictionaries

    """
    return schema.get_challenge(
        filters=body.filters, fields=fields, by_category=by_category,
        page=page, pagesize=pagesize, ordering=body.ordering
    )


@app.post("/post/plan", response_model=ListResponse, response_model_exclude_unset=True)
async def post_plan(
    body: PolicyFilters,
    by_category: str = None,
    fields: List[str] = Query(None),
    page: int = None,
    pagesize: int = 100,
):
    """Return Plan data with filters applied.

    Parameters
    ----------
    body : PolicyFilters
        Filters to apply.
    fields : List[str]
        Data fields to return.

    Returns
    -------
    dict
        Plan response dictionary

    """
    return schema.get_plan(
        filters=body.filters, fields=fields, by_category=by_category,
        page=page, pagesize=pagesize, ordering=body.ordering
    )


@app.get("/get/optionset", response_model=OptionSetList)
async def get_optionset(
    fields: List[str] = Query(None),
    class_name: str = 'Policy'
):
    """Given a list of data fields and an entity name, returns the possible
    values for those fields based on what data are currently in the database.

    TODO add support for getting possible fields even if they haven't been
    used yet in the data

    Parameters
    ----------
    fields : list
        List of strings of data fields names.
    class_name : str
        The name of the entity for which to check possible values.

    Returns
    -------
    api.models.OptionSetList
        List of possible optionset values for each field.

    """
    return schema.get_optionset(fields=fields, class_name=class_name)


##
# Test endpoints
##
@app.get("/test")
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

##
# Debug endpoints
##


@app.get("/add_search_text")
async def add_search_text(test_param: str = 'GET successful'):
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
    schema.add_search_text()
    return 'Done'
