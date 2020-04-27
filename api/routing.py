# standard packages
# from datetime import datetime

# 3rd party modules
from pony.orm import db_session

# local modules
from . import schema
from .models import PolicyList
from .app import app


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
