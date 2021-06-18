from datetime import date
from enum import Enum
from pony.orm.core import commit, db_session
from ingest.util import upsert
from db.models import MaxMinPolicyCount, Place
from api.types import GeoRes
from db import db
from os import getcwd
from api.models import PlaceObs
from typing import Dict, List, Tuple


def get_map_type_from_level(level: str) -> str:
    if level == "Local":
        return "us-county"
    elif level == "Local plus state/province":
        return "us-county-plus-state"
    elif level == "State / Province":
        return "us"
    elif level == "Country":
        return "global"
    else:
        raise ValueError("Unexpected level: " + str(level))


class PolicyCountType(Enum):
    MIN = 1
    MAX = 2


class StaticMaxMinCounter:
    """
    Calculate and store the maximum number of policies in effect on any
    given date in any given location for each geographic resolution
    (map type).

    """

    def __init__(self):
        return None

    @db_session
    def get_max_min_counts(
        self,
    ) -> Dict[GeoRes, Tuple[PlaceObs, PlaceObs]]:
        """Computes the maximum number of policies in effect in any given
        location on any given date for each map type.

        Returns:
            Dict[GeoRes, Tuple[PlaceObs, PlaceObs]]: The maximum number of
            policies in effect in any given location on any given date for each
            map type. The maximum is first.
        """
        # For each map type (geographic resolution)
        geo_res: GeoRes = None
        max_min_by_geo_res: Dict[GeoRes, Tuple[PlaceObs, PlaceObs]] = dict()
        for geo_res in GeoRes:
            # Get the applicable level
            level: str = geo_res.get_level()
            res: tuple = None

            # Execute SQL query to count maximum number of policies for the map
            # type on any given date in any given place
            with open(
                getcwd()
                + "/api/sql_templates/template_get_max_policies_pg.sql",
                "r",
            ) as file:
                sql: str = file.read()
                with db.get_connection().cursor() as curs:
                    place_filters_sql: str = self.__get_place_filters_sql(
                        [level]
                    )
                    curs.execute(
                        sql
                        % {
                            "place_filters_sql": place_filters_sql,
                        }
                    )
                    res = curs.fetchone()
            # Assume 1 is the minimum number of policies
            min_obs: PlaceObs = PlaceObs(value=1)

            # define max value based on query result
            loc_field: str = geo_res.get_loc_field()
            max_date: date = res[0]
            max_place_id: int = res[1]
            max_value: int = res[2]
            max_place: Place = Place[max_place_id]
            max_obs: PlaceObs = PlaceObs(
                place_name=getattr(max_place, loc_field),
                datestamp=max_date,
                value=max_value,
            )

            # Return the maximum and minimum values
            max_min_counts: Tuple[PlaceObs, PlaceObs] = (
                min_obs,
                max_obs,
            )
            max_min_by_geo_res[geo_res] = max_min_counts

            # update database values
            map_type: str = geo_res.get_map_type()
            upsert(
                MaxMinPolicyCount,
                {"map_type": map_type},
                {
                    "max_value": max_obs.value,
                    "max_date": max_obs.datestamp,
                    "max_place": max_place,
                    "min_value": min_obs.value,
                },
            )
            commit()

        return max_min_by_geo_res

    def __get_place_filters_sql(self, levels: List[str]) -> str:
        """Given the levels for which max policy status counts are needed,
        return the correct filtering code.

        Args:
            levels (List[str]): The levels for which max policy status counts
            are needed.

        Raises:
            NotImplementedError: Only one level can be requested.

        Returns:
            str: The correct filtering SQL statement(s).
        """

        # Apply level filters
        if len(levels) > 1:
            raise NotImplementedError(
                "Expected only one level but found: " + levels
            )
        level: str = levels[0]
        place_filters_sql: str = f"""pl.level = '{level}' """

        if level != "Country":
            place_filters_sql = place_filters_sql + "and pl.iso3 = 'USA'"

        return place_filters_sql
