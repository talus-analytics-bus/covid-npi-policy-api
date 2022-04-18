"""Test local area codes, e.g., USA county FIPS codes"""
import unittest
import re

from pony.orm import db_session, select, BindingError
from db import models, db


class TestLocalCodes(unittest.TestCase):
    mapping_generated: bool = False

    def __init__(self, methodName: str = ...) -> None:
        unittest.TestCase.__init__(self, methodName)
        if not TestLocalCodes.mapping_generated:
            db.generate_mapping(create_tables=False)
            TestLocalCodes.mapping_generated = True

    @db_session
    def test_usa_county_fips(self):
        usa_local_areas = select(
            i for i in models.Place if i.level == "Local" and i.iso3 == "USA"
        )[:][:]
        assert len(usa_local_areas) > 0
        assert all(p.ansi_fips is not None for p in usa_local_areas)

        usa_fips_5_regex = re.compile("\\d{5}")
        assert usa_fips_5_regex.match("20005") is not None
        assert usa_fips_5_regex.match("02005") is not None
        assert usa_fips_5_regex.match("2005") is None
        assert usa_fips_5_regex.match("0205") is None
        for p in usa_local_areas:
            assert p.ansi_fips == "" or usa_fips_5_regex.match(p.ansi_fips) is not None

    @db_session
    def test_phl_area_codes(self):
        phl_local_areas = select(
            i for i in models.Place if i.level == "Local" and i.iso3 == "PHL"
        )[:][:]
        phl_code_regex = re.compile("RP[0-9A-Z][0-9]")
        for p in phl_local_areas:
            assert p.ansi_fips == "" or phl_code_regex.match(p.ansi_fips) is not None
