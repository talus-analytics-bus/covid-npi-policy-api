"""Define API application"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.openapi.utils import get_openapi

# define API app
tags_metadata = [
    {
        "name": "Policies",
        "description": "Operations to get data for policies and counts of policies in certain categories and/or subcategories.",
    },
    {
        "name": "Plans",
        "description": "Operations to get data for plans in certain categories and/or subcategories.",
    },
    {
        "name": "Court challenges",
        "description": "Operations to get data for court challenges (to policies) in certain categories and/or subcategories.",
    },
    {
        "name": "Places",
        "description": "Operations to get data for places that are affected by policies, including name and number of policies affecting (overall).",
    },
    {
        "name": "Distancing levels",
        "description": "Operations to get data describing the level of distancing (e.g., \"Lockdown\", \"Partially open\") that was in effect in a given US state or a given country on a given date.",
    },
    {
        "name": "Metadata",
        "description": "Operations to get metadata including field definitions, optionset values for dropdowns based on data fields, counts of policies, plans, or court challenges in the database, and the current version of different data types (i.e., date last updated).",
        # "externalDocs": {
        #     "description": "Items external docs",
        #     "url": "https://fastapi.tiangolo.com/",
        # },
    },
    {
        "name": "Downloads",
        "description": "Operations to download data (.xlsx or .pdf). Excel-exportable data types can be downloaded with `/post/export` and include policies, plans, and court challenges. Filters may be applied. Individual PDF files associated with those data types can be downloaded using the `/get/file` endpoint.",
    },
]
app = FastAPI()

# set allowed origins
allow_origin_regex = \
    "(http:\/\/localhost:.*|" + \
    "https?:\/\/covidamp\.org|" + \
    "https?:\/\/test\.covidamp\.org|" + \
    "https?:\/\/.*\.cloudfront\.net|" + \
    "http:\/\/covid-amp-tess\.s3-website-us-west-2\.amazonaws\.com|" + \
    "https?:\/\/.*\.talusanalytics.*)"

# add middleware
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=allow_origin_regex,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

new_desc = """## Overview
The <strong>COVID Analysis and Mapping of Policies (AMP)</strong> site provides access to a comprehensive list of policies and plans implemented globally to address the COVID-19 pandemic. This API provides access to some of the underlying data used in the COVID AMP site.

You can visit the site at [https://covidamp.org](https://covidamp.org). Please contact us with comments, questions, or accessibility concerns at [https://covidamp.org/contact](https://covidamp.org/contact).
## Database update schedule
The AMP database is usually updated every 1-2 days on weekdays around 10 AM ET with all the latest information. Data request stalling may occur during a 5-minute period while these updates are being made, but requests will succeed again when updates are completed.
## Filtering data
Filters to view only a subset of data are generally defined in the body of a POST request (see POST endpoints below). Some examples of potentially useful filters follow.
 - See all "Social distancing" policies: `{"filters": "primary_ph_measure": ["Social distancing"]}`
 - See all "Social distancing" >> "Lockdown" policies: `{"filters": "primary_ph_measure": ["Social distancing"], "ph_measure_details": ["Lockdown"]}`
 - See all "Face mask" policies: `{"filters": "primary_ph_measure": ["Face mask"]}`
 - Special filter: See all policies in effect during the month of February 2020: `{"filters": "dates_in_effect": ["2020-02-01", "2020-02-28]}`
 - See all policies affecting the country of Brazil: `{"filters": "iso3": ["BRA"]}`
 - See all policies affecting the state of Alabama: `{"filters": "area1": ["Alabama"]}`
"""


def custom_openapi():
    # if app.openapi_schema:
    # return app.openapi_schema
    openapi_schema = get_openapi(
        title="COVID AMP application programming interface (API) documentation",
        version="1.0.0",
        description=new_desc,
        # description="<p>The <strong>COVID Analysis and Mapping of Policies (AMP)</strong> site provides access to a comprehensive list of policies and plans implemented globally to address the COVID-19 pandemic. This API provides access to some of the underlying data used in the COVID AMP site.</p><p>You can visit the site at <a href=\"https://covidamp.org/\" target=\"_blank\">https://covidamp.org/</a>. Please contact us with comments, questions, or accessibility concerns at <a href=\"https://covidamp.org/contact\" target=\"_blank\">https://covidamp.org/contact</a>.</p><p><strong>Note about data updates:</strong> The AMP database is updated every 1-2 days on weekdays around 10 AM ET with all the latest information. Data request stalling may occur during a 5-minute period while these updates are being made, but requests will succeed again when updates are completed.</p>",
        routes=app.routes
    )
    openapi_schema["info"]["x-logo"] = {
        "url": "https://covidamp.org/static/media/logo.b7a0d643.svg",
        "altText": "COVID AMP logo",
        "href": "https://covidamp.org/"
    }
    openapi_schema["tags"] = tags_metadata
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi
