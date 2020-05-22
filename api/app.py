"""Define API application"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

# define API app
app = FastAPI()

# set allowed origins
allow_origin_regex = \
    "(http:\/\/localhost:.*|" + \
    "https?:\/\/.*\.covidamp\.org|" + \
    "https?:\/\/.*\.cloudfront\.net|" + \
    "https?:\/\/.*\.talusanalytics.*)"

# add middleware
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=allow_origin_regex,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
