"""Run API application"""
# local modules
from api import app, core
from db import db

db.generate_mapping(create_tables=False)
