"""Define API application"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

app = FastAPI()

app.mount("/pdf", StaticFiles(directory="api/pdf"), name="pdf")

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
