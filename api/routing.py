"""Define API endpoints"""
# 3rd party modules
from fastapi import Query
from typing import List

# local modules
from . import schema
from .models import PolicyList, PolicyFilters, OptionSetList
from .app import app
from db import db


@app.post("/post/export")
async def export(body: PolicyFilters):
    """Download XLSX of data.

    Returns
    -------
    def
        Description of returned object.

    """
    filters = body.filters if bool(body.filters) == True else None
    return schema.export(filters=filters)


@app.get("/get/doc")
async def get_doc(id: int):
    return schema.get_doc(id)


@app.get("/get/policy")
async def get_policy():
    return schema.get_policy()


@app.post("/post/policy")
async def get_policy(body: PolicyFilters):
    return schema.get_policy(filters=body.filters)


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


@app.get("/ingest")
async def ingest(project_name: str = None):
    if project_name == 'covid-npi-policy':
        print('Ingesting data...')
        # db.generate_mapping(check_tables=False, create_tables=False)
        db.drop_all_tables(with_all_data=True)
        db.create_tables()
        schema.ingest_covid_npi_policy()
        return 'Ingest completed'
    else:
        raise NotImplementedError(
            'Error: Unknown `project_name`: ' + str(project_name))
        return 'Ingest failed'


##
# Test endpoints
##
@app.get("/test")
async def get(test_param: str = 'GET successful'):
    """Test GET endpoint.

    Parameters
    ----------
    test_param : str
        A message to be returned in the response if GET was successful.

    Returns
    -------
    def
        A message containing the value of `test_param` indicating the GET was
        successful.

    """
    return [{'success': True, 'message': 'GET test', 'data': [test_param]}]
