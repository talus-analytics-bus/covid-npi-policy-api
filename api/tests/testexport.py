from datetime import date
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

        if not hasattr(self, "__local_place_ansi_fips_by_loc"):
            self.__local_place_ansi_fips_by_loc = None

    def test_usa_al(self):
        self.__test_usa_excel(
            {
                "country_name": ["United States of America (USA)"],
                "area1": ["Alabama"],
            }
        )

    def test_blackfeet_nation(self):
        self.__test_usa_excel(
            {
                "country_name": ["Blackfeet Nation"],
                "area1": ["Blackfeet Nation"],
            }
        )

    def test_usa_ny_localities_2020(self):
        self.__test_usa_excel(
            {
                "country_name": ["United States of America (USA)"],
                "area1": ["New York"],
                "dates_in_effect": [date(2020, 1, 1), date(2020, 8, 1)],
            }
        )

    def test_usa_ca(self):
        self.__test_usa_excel(
            {
                "country_name": ["United States of America (USA)"],
                "area1": ["California"],
            }
        )

    def test_usa_al_jefferson_county(self):
        self.__test_usa_excel(
            {
                "country_name": ["United States of America (USA)"],
                "area1": ["Alabama"],
                "area2": ["Jefferson County, AL"],
            }
        )

    def test_usa_az_pima(self):
        self.__test_usa_excel(
            {
                "country_name": ["United States of America (USA)"],
                "area1": ["Arizona"],
                "area2": ["Pima County, AZ"],
            }
        )

    def __test_usa_excel(self, filters: dict):
        # download Excel
        df_row_dicts = self.__get_test_excel(filters=filters)
        usa_fips_regex = re.compile("^(\\d{5}|\\d{7}|Undefined)$")
        lookup_ansi_fips_by_place_loc: Dict[
            str, str
        ] = self.__get_local_place_ansi_fips_by_loc()

        # raise exception if no results since there should be some
        if len(df_row_dicts) == 0:
            raise ValueError(
                "Expected at least some rows, but found zero. Check test filters."
            )

        for row in df_row_dicts:

            for place_type in ["Affected", "Authorizing"]:
                affected_local_area_locs = row[
                    f"{place_type} local area (e.g., county, city)"
                ]
                affected_local_area_codes = row[
                    f"{place_type} local area (e.g., county, city) code"
                ]

                if (
                    affected_local_area_codes != ""
                    and type(affected_local_area_codes) == str
                ):
                    affected_local_area_codes_list = affected_local_area_codes.split(
                        "; "
                    )

                    # Local area code should be correctly formed
                    level: str = row[f"{place_type} level of government"]
                    assert all(
                        (
                            (
                                level == "Local"
                                and usa_fips_regex.match(code) is not None
                            )
                            or (level != "Local" and code == "N/A")
                        )
                        for code in affected_local_area_codes_list
                    )

                    # The location codes should be listed in the same order as the
                    # place names
                    affected_local_area_locs_list = affected_local_area_locs.split("; ")
                    for idx, loc in enumerate(affected_local_area_locs_list):
                        if loc == "N/A" and level != "Local":
                            continue
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
        df = pd.read_excel(
            response.body, engine="openpyxl", header=6, keep_default_na=False
        )
        df_row_dicts: List[dict] = df.to_dict(orient="records")
        assert df is not None
        return df_row_dicts

    @db_session
    def __get_local_place_ansi_fips_by_loc(self) -> Dict[str, str]:
        if self.__local_place_ansi_fips_by_loc is None:
            places = select(p for p in models.Place if p.level == "Local")[:][:]
            lookup_table = {
                p.area2: p.ansi_fips if p.ansi_fips != "" else "Undefined"
                for p in places
            }
            self.__local_place_ansi_fips_by_loc = lookup_table

        return self.__local_place_ansi_fips_by_loc


if __name__ == "__main__":
    tc = TestLocalAreaCodes("test_blackfeet_nation")
    tc()
