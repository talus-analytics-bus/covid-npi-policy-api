# standard packages
# from datetime import datetime

# 3rd party modules
from fastapi import FastAPI, Path, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.testclient import TestClient
from pony.orm import db_session
from typing import Set, List

# local modules
from . import schema
from .models import PolicyList


app = FastAPI()


allow_origin_regex = \
    "(http:\/\/localhost:.*|" + \
    "https?:\/\/.*\.cloudfront\.net|" + \
    "https?:\/\/.*\.talusanalytics.*)"


app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=allow_origin_regex,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/get_policy")
async def get_policy():
    return schema.get_policy()


@app.get("/ingest", response_model=PolicyList)
async def ingest(project_name: str = None):
    if project_name == 'covid-npi-policy':
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
