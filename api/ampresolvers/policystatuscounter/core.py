# standard packages
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, List, Tuple, Set, Union
from pony.orm.core import (
    JOIN,
    Query,
    count,
    db_session,
    get,
    left_join,
    select,
)

# local packages
import api
from api.types import GeoRes
from api.models import PlaceObs, PlaceObsList
from api.utils import cached
from db import db
from db.models import (
    MaxMinPolicyCount,
    Place,
    Policy_By_Group_Number,
    Policy,
)
from .helpers import PolicyCountType, get_map_type_from_level


class QueryResolver(ABC):
    def __init__(self):
        return None

    @abstractmethod
    def __validate_args(self, **kwargs):
        """Validate input arguments and raise exception if error found"""
        pass


class PolicyStatusCounter(QueryResolver):
    """Counts the number of policies in effect in a given location on a given
    date, and potentially matching certain filters.

    """

    def __init__(self) -> None:
        """Create new PolicyStatusCounter

        Returns:
            NoneType: None.
        """
        return None

    @cached
    @db_session
    def get_policy_status_counts_for_map(
        self,
        geo_res: GeoRes,
        cats: List[str],
        subcats: List[str],
        subtargets: List[str],
        date: datetime.date,
        sort: bool = False,
    ) -> PlaceObsList:
        """Returns a list of place observations defining the number of policies
        in effect in locations on a given date, at a given geographic
        resolution, and optionally with certain categories and/or
        subcategories. The min and max observation for all time are
        also returned.

        Args:
            geo_res (GeoRes): The geographic resolution of interest.

            cats (List[str]): Optional list of categories to filter by.

            subcats (List[str]): Optional list of subcategories to filter by.

            subtargets (List[str]): Optional list of subtargets to filter by.

            date (datetime.date): The date of interest.

            sort (bool, optional): Whether to sort the observation list by
            descending value. Defaults to False.

        Returns:
            PlaceObsList: A list of place observations.
        """
        # get data fields specific to this geographic resolution for query
        level: str = geo_res.get_level()
        loc_field: str = geo_res.get_loc_field()
        usa_only: bool = geo_res != GeoRes.country

        # define query and get results
        # pre-filter by subtargets if needed
        q_subtargets: Query = select(p for p in Policy)
        for st in subtargets:
            q_subtargets = q_subtargets.filter(lambda p: st in p.subtarget)

        q: Query = select(
            (getattr(pl, loc_field), count(pbgn))
            for p in q_subtargets
            for pbgn in p._policy_by_group_number
            for pl in p.place
            for pdd in p._policy_day_dates
            if pdd.day_date == date
            and pl.level == level
            and (len(cats) == 0 or p.primary_ph_measure in cats)
            and (len(subcats) == 0 or p.ph_measure_details in subcats)
            and (not usa_only or pl.iso3 == "USA")
        )
        q_result: List[Tuple[str, int]] = q[:][:]

        # define response's place observation list
        response: PlaceObsList = PlaceObsList(
            data=[
                PlaceObs(place_name=r[0], value=r[1]) for r in q_result if r[0] != ""
            ],
            success=True,
            message="Message",
        )

        # add missing zero values
        zero_val_loc_names: List[str] = self.__get_place_loc_vals_of_level(
            loc_field=loc_field, level=level, usa_only=usa_only
        )
        nonzero_loc_vals: List[str] = set([t[0] for t in q_result])
        loc_val: str = None
        for loc_val in zero_val_loc_names:
            if loc_val not in nonzero_loc_vals:
                response.data.append(PlaceObs(place_name=loc_val, value=0))

        # sort if requested
        if sort:
            response.data.sort(key=lambda x: x.value, reverse=True)

        # define min/max observation values
        min_max: Tuple[PlaceObs, PlaceObs] = self.__fetch_static_max_min_counts(level)
        response.min_all_time = min_max[0]
        response.max_all_time = min_max[1]

        # return response
        return response

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
        count_min_max_by_cat: bool = False,
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

            count_min_max_by_cat (bool, optional): If True, a min/max value
            specific to any categories and/or subcategories defined in
            `filters` will be returned. Otherwise, the overall min/max value
            irrespective of cats./subcats. will be returned.

            one (bool, optional): If True, return the first observation only.

            counted_parent_geos (List[GeoRes], optional): A list of parent
            geographic resolutions whose policies should also be counted in
            addition to policies in effect at the level of the defined
            `geo_res`. If none provided, only policies at the level of the
            defined `geo_res` are counted.

        Returns:
            PlaceObsList: A list of policy status counts by location.
        """

        # DEBUG Profile code time. Uncomment code below and at end of file
        # to profile code time.

        # import cProfile
        # import pstats
        # import io
        # from pstats import SortKey

        # pr = cProfile.Profile()
        # pr.enable()

        # validate arguments and raise exceptions if errors
        self._QueryResolver__validate_args(
            geo_res=geo_res,
            filter_by_subgeo=filter_by_subgeo,
        )

        # get location field and level value for SQL query filtering
        loc_field: str = geo_res.get_loc_field()
        levels: List[str] = [geo_res.get_level()]

        # if geo res is state or county, count USA places only
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
            q = api.core.apply_subgeo_filter(q, geo_res)

        # initialize output data
        data: list = None

        # apply filters, if any
        if filters is not None:

            # apply filters to standard policy data query
            q = api.core.apply_entity_filters(
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
                filters=filters, loc_field=loc_field, for_usa_only=for_usa_only
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
                        zero_obs: PlaceObs = PlaceObs(place_name=place_area1, value=0)
                        data.append(zero_obs)
                elif geo_res in (GeoRes.county, GeoRes.county_plus_state):
                    if ansi_fips is None:
                        continue
                    if iso3 == "USA" and ansi_fips not in data_tmp:
                        ansi_fips_final: str = (
                            "0" + ansi_fips if len(ansi_fips) == 4 else ansi_fips
                        )
                        zero_obs: PlaceObs = PlaceObs(
                            place_name=ansi_fips_final, value=0
                        )
                        data.append(zero_obs)
                else:
                    raise ValueError("Unknown geo_res: " + geo_res)

        # order by descending value
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
        res_counted: str = geo_res if not filter_by_subgeo else "sub-" + geo_res

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
            filters_to_skip: Set[str] = {
                "dates_in_effect",
                "iso3",
                "area1",
                "ansi_fips",
            }

            # if policy count min/max should not be computed on a cat/subcat
            # basis, skip those filters if they're present
            if not count_min_max_by_cat:
                filters_to_skip |= {"primary_ph_measure", "ph_measure_details"}
            filters_no_dates: dict = dict()
            field: str = None
            field_val: Any = None
            for field, field_val in filters.items():
                if field not in filters_to_skip:
                    filters_no_dates[field] = field_val

            # get min/max for all time
            min_max_counts: Tuple[
                PlaceObs, PlaceObs
            ] = self.__fetch_static_max_min_counts(
                level=levels[0],
            )

            # define min/max for all time
            res.min_all_time = min_max_counts[0]
            res.max_all_time = min_max_counts[1]

        # # DEBUG Write profiling results to text file
        # pr.disable()
        # s = io.StringIO()
        # sortby = SortKey.CUMULATIVE
        # ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
        # ps.print_stats()
        # with open("debug.txt", "w") as file:
        #     file.write(s.getvalue())

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
    def __get_zero_count_data(self, filters: dict, loc_field: str, for_usa_only: bool):
        is_one_place: bool = (
            loc_field in filters and len(filters[loc_field]) > 0 and "level" in filters
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
    def __fetch_static_max_min_counts(
        self,
        level: str,
    ) -> Tuple[PlaceObs, PlaceObs]:
        """Given a place level and location field, returns the corresponding
        highest number of policies in effect on any date in any location at
        that level.

        Args:
            level (str): The level of place being considered.
            loc_field (str): The field of the Place entity that identifies the
            place at `level`.

        Returns:
            Tuple[PlaceObs, PlaceObs]: The maximum and minimum observed counts
            of policies on any date in any location at that level.
        """

        # get map type from place level
        map_type: str = get_map_type_from_level(level)

        # retrieve counts
        instance: Query = get(i for i in MaxMinPolicyCount if i.map_type == map_type)
        min_obs: PlaceObs = self.__get_policy_count_obs_from_inst(
            instance, PolicyCountType.MIN
        )
        max_obs: PlaceObs = self.__get_policy_count_obs_from_inst(
            instance, PolicyCountType.MAX
        )
        max_min_counts: Tuple[PlaceObs, PlaceObs] = (
            min_obs,
            max_obs,
        )
        return max_min_counts

    def __get_policy_count_obs_from_inst(
        self, instance: MaxMinPolicyCount, type: PolicyCountType
    ) -> PlaceObs:
        """Given the instance of the MaxMinPolicyCount entity and the policy
        count type (min or max), returns the PlaceObs that corresponds to it.

        Args:
            instance (MaxMinPolicyCount): The instance of the MaxMinPolicyCount
            entity containing the max/min policy count for the map of interest.

            type (PolicyCountType): Max or min

        Raises:
            ValueError: For unexpected values of `type`

        Returns:
            PlaceObs: The place observation corresponding to the max/min count
        """
        key_suffixes: List[str] = ["_place", "_value", "_date"]
        keys: List[str] = None
        if type == PolicyCountType.MIN:
            keys = ["min" + suffix for suffix in key_suffixes]

        elif type == PolicyCountType.MAX:
            keys = ["max" + suffix for suffix in key_suffixes]
        else:
            raise ValueError("Unexpected policy count type: " + str(PolicyCountType))

        place_field: str = keys[0]
        value_field: str = keys[1]
        date_field: str = keys[2]
        place_id: str = (
            getattr(instance, place_field).id
            if getattr(instance, place_field) is not None
            else None
        )
        return PlaceObs(
            place_id=place_id,
            value=getattr(instance, value_field),
            datestamp=getattr(instance, date_field),
        )

    def __get_obs_from_q_result(
        self, res: MaxMinPolicyCount, loc_field: str
    ) -> Union[PlaceObs, None]:
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
            Union[PlaceObs, None]: The place observation, or None if no data.
        """
        if res is None:
            return None
        else:
            return PlaceObs(
                datestamp=None,
                place_name=None,
                value=res.max_value,
            )

            # The code below is necessary if the PlaceObs is being determined
            # from a tuple from a SQL query result rather than from a simple
            # instance value, as is currently done.

            # place_obs: PlaceObs = None
            # datestamp: date = None
            # place_id: int = None
            # value: int = None
            # q_vals: Tuple[date, int, int] = res
            # if len(q_vals) != 3:
            #     raise ValueError(
            #         "Expected query result row to have 3 column values,"
            #         " but found " + str(len(q_vals))
            #     )
            # datestamp, place_id, value = q_vals
            # place_obs: PlaceObs = PlaceObs(
            #     datestamp=datestamp,
            #     place_name=getattr(Place[place_id], loc_field),
            #     value=value,
            # )
            # return place_obs

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

    # The method below returns SQL WHERE clauses that filter max policy counts
    # by category and/or subcategory.
    #
    # It is not currently used because the global max policy counts is used in
    # the COVID AMP map page, rather than the cat./subcat.-specific one.

    # def __get_cat_and_subcat_filters_sql(self, filters: dict) -> str:
    #     """Given the filters dictionary, return SQL "where" clause capturing
    #     category and subcategory filters, or true if no such filters

    #     Args:
    #         filters (dict): The filters

    #     Returns:
    #         str: The where clause
    #     """
    #     cat_and_subcat_filters_sql = ""
    #     fields: Set[str] = {"primary_ph_measure", "ph_measure_details"}
    #     field: str = None
    #     for field in fields:
    #         if field in filters:
    #             vals: List[str] = filters[field]
    #             if len(vals) > 0:
    #                 cat_and_subcat_filters_sql += " and "
    #                 vals_str: str = ", ".join(["'" + x + "'" for x in vals])
    #                 cat_and_subcat_filters_sql += (
    #                     f"""pol.{field} in ({vals_str})"""
    #                 )
    #     if cat_and_subcat_filters_sql != "":
    #         return cat_and_subcat_filters_sql
    #     else:
    #         return "and true"

    @cached
    @db_session
    def __get_place_loc_vals_of_level(
        self, loc_field: str, level: str, usa_only: bool
    ) -> List[str]:
        """Returns location values for locations contained in the COVID AMP
        place relation with the specified level and possibly for USA only. Only
        returns places that have at least one policy affecting them.

        Args:
            loc_field (str): The Place field to use for the location value.
            level (str): The level of Place of interest.
            usa_only (bool): True if only USA places of interest.

        Returns:
            List[str]: List of place location values.
        """
        q: Query = select(
            getattr(pl, loc_field)
            for pl in Place
            if (not usa_only or pl.iso3 == "USA")
            and pl.level == level
            and count(pl.policies) > 0
        )
        q_result: List[str] = q[:][:]
        return q_result
