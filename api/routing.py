"""Define API endpoints"""
# 3rd party modules
from fastapi import Query
from starlette.responses import RedirectResponse
from typing import List

# local modules
from . import schema
from .models import PolicyList, PolicyFilters, OptionSetList, MetadataList, \
    ListResponse
from .app import app
from db import db


@app.post("/post/export")
async def export(body: PolicyFilters):
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
    return schema.export(filters=filters)


@app.get("/get/metadata", response_model=MetadataList)
async def get_metadata(fields: List[str] = Query(None)):
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
    return schema.get_metadata(fields)


@app.get("/get/file/redirect")
async def get_file(id: int):
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
async def get_policy(fields: List[str] = Query(None)):
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
    return schema.get_policy(fields=fields)


@app.post("/post/policy", response_model=ListResponse, response_model_exclude_unset=True)
async def post_policy(body: PolicyFilters, fields: List[str] = Query(None)):
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
    return schema.get_policy(filters=body.filters, fields=fields)


@app.get("/get/optionset", response_model=OptionSetList)
async def get_optionset(fields: List[str] = Query(None), entity_name: str = None):
    """Given a list of data fields and an entity name, returns the possible
    values for those fields based on what data are currently in the database.

    TODO add support for getting possible fields even if they haven't been
    used yet in the data

    Parameters
    ----------
    fields : list
        List of strings of data fields names.
    entity_name : str
        The name of the entity for which to check possible values.

    Returns
    -------
    api.models.OptionSetList
        List of possible optionset values for each field.

    """
    return schema.get_optionset(fields)


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
