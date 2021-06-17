from os import getcwd

# from ingest.util import get_fips_with_zeros
from api.types import GeoRes
from queryresolver.core import QueryResolver
import api
from api.models import PlaceObs, PlaceObsList
from api.util import cached
from db import db
from db.models import (
    Place,
    Policy_By_Group_Number,
)
from typing import Any, List, Tuple, Set
from datetime import date
from pony.orm.core import (
    JOIN,
    Query,
    count,
    db_session,
    left_join,
    select,
)


class PolicyStatusCounter(QueryResolver):
    # TODO update all docs
    def __init__(self):
        return None

    @cached
    @db_session
    def get_policy_status_counts(
        self,
        geo_res: GeoRes,
        filters: dict = dict(),
        by_group_number: bool = True,
        filter_by_subgeo: bool = False,
        include_zeros: bool = True,
        include_min_max: bool = True,
        one: bool = False,
        counted_parent_geos: List[GeoRes] = list(),
    ) -> PlaceObsList:
        """Returns the number of active policies matching the provided filters
        affecting locatiioins that match the provided geographic resolution.

        Args:
            geo_res (str): The geographic resolution to count.

            filters (dict, optional): Filters to apply to policies. Defaults to
            dict(), representing no filters.

            by_group_number (bool, optional): If True, counts only the first
            policy with each group number, helping to correct for over-counting
            similar policies, and providing a fairer comparision of active
            policy counts between different locations. Defaults to True.

            filter_by_subgeo (bool, optional): If True, counts policies active
            in locations *beneath* locations with the provided geographic
            resolution, e.g., if `geo_res` is "Country" then all policies
            affecting sub-country locations, like states or provinces or
            counties, are counted. If False, only policies affecting locations
            *at* the provided geographic resolution are counted. Defaults
            to False.

            include_zeros (bool, optional): If True, returns zero-valued data
            entries for locations at the provided geographic resolution that
            had no active policies matching the provided filters, but which
            had some active policies at some point in the database. Defaults
            to True.

            include_min_max (bool, optional): If True, returns the observations
            that represent the min/max number of active policies in any
            location on any date, for comparison and baselining purpose.
            Defaults to True.

            one (bool, optional): If True, return the first observation only.

        Returns:
            PlaceObsList: A list of policy status counts by location.
        """

        # validate arguments and raise exceptions if errors
        self._QueryResolver__validate_args(
            geo_res=geo_res,
            filter_by_subgeo=filter_by_subgeo,
        )

        # get correct location field and level value for filtering
        loc_field: str = geo_res.get_loc_field()
        levels: List[str] = [geo_res.get_level()]

        # if geo res is state or county, filter by USA only
        for_usa_only: bool = geo_res in (
            GeoRes.state,
            GeoRes.county,
            GeoRes.county_plus_state,
        )
        if for_usa_only:
            filters["iso3"] = ["USA"]

        # GET POLICIES FROM DATABASE # -------------------------------------- #
        # filter by level = [geo], unless counting sub-[geo] only; if so, then
        # filter by level != [geo or higher]
        if not filter_by_subgeo:
            filters["level"] = levels

        # define query to get policies from database
        q: Query = select(i for i in db.Policy)

        # if zeros requested, initialize query to get all locations with any
        # data (before filters)
        data_all_time: Query = None

        # if counting only sub-[geo] policies, filter policies by
        # correct levels
        if filter_by_subgeo:
            q = api.schema.apply_subgeo_filter(q, geo_res)

        # initialize output data
        data: list = None

        # apply filters, if any
        if filters is not None:

            # apply filters to standard policy data query
            q = api.schema.apply_entity_filters(
                q,
                db.Policy,
                filters,
            )

        # GET POLICY COUNTS BY LOCATION # ----------------------------------- #
        # if requested, only count the first policy with each group number,
        # otherwise count each policy
        q_policies_by_loc: Query = None
        subquery: Query = None
        if not by_group_number:
            subquery = q
        else:
            subquery = self.__get_distinct_groups_in_policy_q(q)

        q_policies_by_loc = left_join(
            (
                getattr(p, loc_field),
                count(i),
                p.level,
                p.area1,
                p.iso3,
            )
            for i in subquery
            for p in i.place
        )

        # initialize core response data
        data_tmp = dict()
        place_loc_val: str = None
        place_level: str = None
        place_area1: str = None
        _place_iso3: str = None
        value: int = None
        # value_idx: int = 1
        for (
            place_loc_val,
            value,
            place_level,
            place_area1,
            _place_iso3,
        ) in q_policies_by_loc:
            if place_level not in filters["level"]:
                continue
            if place_loc_val in (None, "Unspecified"):
                continue
            if place_loc_val not in data_tmp:
                place_loc_val_final: str = (
                    "0" + place_loc_val
                    if len(place_loc_val) == 4 and loc_field == "ansi_fips"
                    else place_loc_val
                )
                data_tmp[place_loc_val_final] = PlaceObs(
                    place_name=place_loc_val_final, value=value
                )
        data = list(data_tmp.values())

        # add "zeros" to the data, if requested
        if include_zeros:
            data_all_time: List[tuple] = self.__get_zero_count_data(
                filters, loc_field, for_usa_only
            )

            # add a "zero" observation for each of these places if the place is
            # not already present in the response data
            iso3: str = None
            place_area1: str = None
            ansi_fips: str = None
            _level: str = None
            for iso3, place_area1, ansi_fips, _level in data_all_time:
                if geo_res == GeoRes.country:
                    if iso3 not in data_tmp:
                        zero_obs: PlaceObs = PlaceObs(place_name=iso3, value=0)
                        data.append(zero_obs)
                elif geo_res == GeoRes.state:
                    if iso3 == "USA" and place_area1 not in data_tmp:
                        zero_obs: PlaceObs = PlaceObs(
                            place_name=place_area1, value=0
                        )
                        data.append(zero_obs)
                elif geo_res in (GeoRes.county, GeoRes.county_plus_state):
                    if ansi_fips is None:
                        continue
                    if iso3 == "USA" and ansi_fips not in data_tmp:
                        ansi_fips_final: str = (
                            "0" + ansi_fips
                            if len(ansi_fips) == 4
                            else ansi_fips
                        )
                        zero_obs: PlaceObs = PlaceObs(
                            place_name=ansi_fips_final, value=0
                        )
                        data.append(zero_obs)
                else:
                    raise ValueError("Unknown geo_res: " + geo_res)

        # order by value
        data.sort(key=lambda x: x.value, reverse=True)

        # if one record requested, only return one record
        if one and len(data) > 0:
            if loc_field in filters:
                match: PlaceObs = [
                    x for x in data if x.place_name == filters[loc_field][0]
                ]
                if len(match) > 0:
                    data = [match[0]]
                else:
                    data = [data[0]]
            else:
                data = [data[0]]

        # prepare basic response
        res_counted: str = (
            geo_res if not filter_by_subgeo else "sub-" + geo_res
        )

        # parent resolutions counted, if any
        show_parent_res_counted: bool = len(counted_parent_geos) > 0
        parent_res_counted: str = (
            ", including parent geographies at resolution(s) "
            + ", ".join(f"""'{x}'""" for x in counted_parent_geos)
            + ", "
            if show_parent_res_counted
            else ""
        )

        res = api.models.PlaceObsList(
            data=data,
            success=True,
            message=f"""Found {str(len(data))} values """
            f"""counting {res_counted} """
            f"""policies{parent_res_counted} grouped by {geo_res}""",
        )

        # ADD EXTRA REQUESTED DATA TO RESPONSE # ---------------------------- #
        # get min and max values of policies matching the filters for any date,
        # not just the defined date (if defined)
        if include_min_max:
            # get filtered policies, skipping any date filters
            filters_no_dates: dict = dict()
            k: str = None
            v: Any = None
            for k, v in filters.items():
                if k not in ("dates_in_effect", "iso3", "area1", "ansi_fips"):
                    filters_no_dates[k] = v

            # get min/max for all time
            min_max_counts: Tuple[
                PlaceObs, PlaceObs
            ] = self.__get_max_min_counts(
                filters_no_dates=filters_no_dates,
                levels=levels,
                loc_field=loc_field,
            )

            # define min/max for all time
            res.min_all_time = min_max_counts[0]
            res.max_all_time = min_max_counts[1]

        # return response data
        return res

    def __get_all_levels(
        self, geo_res: GeoRes, counted_parent_geos: List[GeoRes]
    ) -> List[str]:
        # get level implied by geographic resolution of interest
        geo_res_level: str = geo_res.get_level()

        # get level(s) implied by parent geographic resolution(s) that will be
        # counted in addition to the geo. res. of interest, if any
        parent_levels: List[str] = [x.get_level() for x in counted_parent_geos]
        # combine and return
        levels: List[str] = [geo_res_level] + parent_levels
        return levels

    def __get_distinct_groups_in_policy_q(self, q: Query) -> Query:
        """For the given query selecting Policy records, returns a modified
        query that keeps only the first Policy record with each group number,
        as defined in the field `Policy.group_number`.

        Args:
            q (Query): The original query selecting Policy records.

        Returns:
            Query: The modified query.
        """
        q = select(
            i
            for i in q
            for pbgn in Policy_By_Group_Number
            if JOIN(i == pbgn.fk_policy_id)
        )
        return q

    @cached
    def __get_zero_count_data(
        self, filters: dict, loc_field: str, for_usa_only: bool
    ):
        is_one_place: bool = (
            loc_field in filters
            and len(filters[loc_field]) > 0
            and "level" in filters
        )
        if is_one_place:
            q: Query = select(
                (p.iso3, p.area1, p.ansi_fips, p.level)
                for p in Place
                if (
                    p.level in filters["level"]
                    and getattr(p, loc_field) == filters[loc_field][0]
                    and count(p.policies) > 0
                )
            )
            return q[:][:]
        else:
            q: Query = select(i for i in db.Policy)
            zero_filters: dict = dict()
            if "level" in filters:
                zero_filters["level"] = filters["level"]
            if is_one_place:
                zero_filters[loc_field] = filters[loc_field]
            if for_usa_only:
                zero_filters["iso3"] = ["USA"]
            q = left_join(
                (
                    p.iso3,
                    p.area1,
                    p.ansi_fips,
                    p.level,
                )
                for i in q
                for p in i.place
                if p.level in zero_filters["level"]
                and (p.iso3 == "USA" or not for_usa_only)
            )
            return q[:][:]

    @cached
    def __get_max_min_counts(
        self,
        filters_no_dates: dict,
        levels: List[str],
        loc_field: str,
    ) -> Tuple[PlaceObs, PlaceObs]:
        # try to recapture SQL in PonyORM
        res: tuple = None
        with open(
            getcwd() + "/api/sql_templates/template_get_max_policies_pg.sql",
            "r",
        ) as file:
            sql: str = file.read()
            with db.get_connection().cursor() as curs:
                place_filters_sql: str = self.__get_place_filters_sql(levels)
                policy_filters_sql: str = (
                    self.__get_cat_and_subcat_filters_sql(filters_no_dates)
                )
                curs.execute(
                    sql
                    % {
                        "place_filters_sql": place_filters_sql,
                        "policy_filters_sql": policy_filters_sql,
                    }
                )
                res = curs.fetchone()
        min_obs: Query = PlaceObs(place_name="n/a", value=1)
        max_obs = self.__get_obs_from_q_result(res, loc_field)
        max_min_counts: Tuple[PlaceObs, PlaceObs] = (
            min_obs,
            max_obs,
        )
        return max_min_counts

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

    def __get_cat_and_subcat_filters_sql(self, filters: dict) -> str:
        """Given the filters dictionary, return SQL "where" clause capturing
        category and subcategory filters, or true if no such filters

        Args:
            filters (dict): The filters

        Returns:
            str: The where clause
        """
        cat_and_subcat_filters_sql = ""
        fields: Set[str] = {"primary_ph_measure", "ph_measure_details"}
        field: str = None
        for field in fields:
            if field in filters:
                vals: List[str] = filters[field]
                if len(vals) > 0:
                    cat_and_subcat_filters_sql += " and "
                    vals_str: str = ", ".join(["'" + x + "'" for x in vals])
                    cat_and_subcat_filters_sql += (
                        f"""pol.{field} in ({vals_str})"""
                    )
        if cat_and_subcat_filters_sql != "":
            return cat_and_subcat_filters_sql
        else:
            return "and true"

    def __get_obs_from_q_result(self, res: tuple, loc_field: str) -> PlaceObs:
        """Returns a place observation corresponding to the result of the query
        that selects a single record with the data fields required by a place
        observation: the datestamp, place name, and value.

        Args:
            q (Query): The query selecting a single record with data fields
            corresponding to and required by a place observation.

            loc_field(str): The field in which location information that should
            be returned is stored.

        Raises:
            ValueError: Query result has more than 1 result row.
            ValueError: Query result row has other than 3 column values.

        Returns:
            PlaceObs: The place observation.
        """
        if res is None:
            return None
        else:
            place_obs: PlaceObs = None
            datestamp: date = None
            place_id: int = None
            value: int = None
            q_vals: Tuple[date, int, int] = res
            if len(q_vals) != 3:
                raise ValueError(
                    "Expected query result row to have 3 column values,"
                    " but found " + str(len(q_vals))
                )
            datestamp, place_id, value = q_vals
            place_obs: PlaceObs = PlaceObs(
                datestamp=datestamp,
                place_name=getattr(Place[place_id], loc_field),
                value=value,
            )
            return place_obs

    def _QueryResolver__validate_args(
        self,
        geo_res: GeoRes,
        filter_by_subgeo: bool,
    ):
        """Validate input arguments."""
        if geo_res == "county" and filter_by_subgeo is True:
            raise NotImplementedError(
                "Cannot count sub-geography policies for counties."
            )
