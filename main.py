# standard packages
# from datetime import datetime

# 3rd party modules
from fastapi import FastAPI, Path, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pony.orm import db_session
from typing import Set, List

# local modules
from ingest import GenericGoogleSheetPlugin


test = GenericGoogleSheetPlugin()
data = test.load_client().load_raw_data()

app = FastAPI()


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
