"""Run API application"""
# local modules
from api import app, schema
from db import db

db.generate_mapping()
