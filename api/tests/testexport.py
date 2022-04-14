import re
import unittest
from typing import Dict, List

import pytest
import pandas as pd
from pony.orm import db_session, select

from db import db, models
from api import schema


class TestLocalAreaCodes(unittest.TestCase):
    mapping_generated: bool = False

    def __init__(self, methodName: str = ...) -> None:
        unittest.TestCase.__init__(self, methodName)
        if not TestLocalAreaCodes.mapping_generated:
            db.generate_mapping(create_tables=False)
            TestLocalAreaCodes.mapping_generated = True

    @pytest.mark.slow
    def test_usa_al(self):
        self.__test_usa_excel(
            {
                "country_name": ["United States of America (USA)"],
                "area1": ["Alabama"],
            }
        )

    # @pytest.mark.slow
    def test_blackfeet_nation(self):
        self.__test_usa_excel(
            {
                "country_name": ["United States of America (USA)"],
                "area1": ["Blackfeet Nation"],
            }
        )

    @pytest.mark.slow
    def test_usa_ca(self):
        self.__test_usa_excel(
            {
                "country_name": ["United States of America (USA)"],
                "area1": ["California"],
            }
        )

    def test_usa_al_lauderdale_county(self):
        self.__test_usa_excel(
            {
                "country_name": ["United States of America (USA)"],
                "area1": ["Alabama"],
                "area2": ["Lauderdale County, AL"],
            }
        )

    def test_usa_mi_farmington(self):
        self.__test_usa_excel(
            {
                "country_name": ["United States of America (USA)"],
                "area1": ["Michigan"],
                "area2": ["Farmington County, MI"],
            }
        )

    def __test_usa_excel(self, filters: dict):
        # download Excel
        df_row_dicts = self.__get_test_excel(filters=filters)
        usa_fips_regex = re.compile("(\\d{5}|Undefined)")

        for row in df_row_dicts:

            for place_type in ["Affected", "Authorizing"]:
                affected_local_area_locs = row[
                    f"{place_type} local area (e.g., county, city)"
                ]
                affected_local_area_codes = row[
                    f"{place_type} local area (e.g., county, city) code"
                ]
                lookup_ansi_fips_by_place_loc: Dict[
                    str, str
                ] = self.__get_local_place_ansi_fips_by_loc()
                if (
                    affected_local_area_codes != ""
                    and type(affected_local_area_codes) == str
                ):
                    affected_local_area_codes_list = affected_local_area_codes.split(
                        "; "
                    )

                    # USA county FIPS codes should be 5 digits, with leading zero if applicable,
                    # regex /\d{5}/; or they should be "Undefined"
                    assert all(
                        usa_fips_regex.match(code) is not None
                        for code in affected_local_area_codes_list
                    )

                    # The location codes should be listed in the same order as the place names
                    affected_local_area_locs_list = affected_local_area_locs.split("; ")
                    for idx, loc in enumerate(affected_local_area_locs_list):
                        assert loc in lookup_ansi_fips_by_place_loc
                        assert (
                            lookup_ansi_fips_by_place_loc[loc]
                            == affected_local_area_codes_list[idx]
                        )

    def __get_test_excel(self, filters: dict):
        response = schema.export(
            filters=filters,
            class_name="Policy",
        )
        assert response.body is not None
        assert response.status_code == 200

        # read Excel
        df = pd.read_excel(response.body, engine="openpyxl", header=6)
        df_row_dicts: List[dict] = df.to_dict(orient="records")
        assert df is not None
        return df_row_dicts

    @db_session
    def __get_local_place_ansi_fips_by_loc(self) -> Dict[str, str]:
        places = select(p for p in models.Place if p.level == "Local")[:][:]
        return {
            p.area2: p.ansi_fips if p.ansi_fips != "" else "Undefined" for p in places
        }
