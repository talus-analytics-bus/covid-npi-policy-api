from queryresolver.core import QueryResolver
import api
from api.models import PlaceObs, PlaceObsList
from api.util import cached, get_first
from db import db
from db.models import (
    DayDate,
    Place,
    Policy,
    Policy_By_Group_Number,
    Policy_Date,
)
from typing import Any, Tuple
from datetime import date
from pony.orm.core import JOIN, Query, count, db_session, select
from pony.orm.ormtypes import raw_sql
from pony.orm.core import min as pony_min


class PolicyStatusCounter(QueryResolver):
    def __init__(self):
        return None

    @cached
    @db_session
    def get_policy_status_counts(
        self,
        geo_res: str,
        filters: dict = dict(),
        by_group_number: bool = True,
        filter_by_subgeo: bool = False,
        include_zeros: bool = True,
        include_min_max: bool = True,
        one: bool = False,
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
        # TODO errors for illogical count_parent_geo vals
        # TODO error if filter_by_subgeo AND count_parent_geo
        self._QueryResolver__validate_args(
            geo_res=geo_res, filter_by_subgeo=filter_by_subgeo
        )

        # get correct location field and level value for filtering
        # TODO add support for multiple levels
        loc_field: str = api.helpers.get_loc_field_from_geo_res(geo_res)
        level: str = api.helpers.get_level_from_geo_res(geo_res)

        # if geo res is state or county, filter by USA only
        for_usa_only: bool = geo_res in ("state", "county")
        if for_usa_only:
            filters["iso3"] = ["USA"]

        # GET POLICIES FROM DATABASE # -------------------------------------- #
        # filter by level = [geo], unless counting sub-[geo] only; if so, then
        # filter by level != [geo or higher]
        # TODO add support for multiple levels
        if not filter_by_subgeo:
            filters["level"] = [level]

        # define query to get policies from database
        q: Query = select(i for i in db.Policy)

        # if zeros requested, initialize query to get all locations with any
        # data (before filters)
        q_all_time = select(i for i in db.Policy) if include_zeros else None

        # if counting only sub-[geo] policies, filter policies by
        # correct levels
        if filter_by_subgeo:
            q = api.schema.apply_subgeo_filter(q, geo_res)
            if include_zeros:
                q_all_time = api.schema.apply_subgeo_filter(
                    q_all_time, geo_res
                )

        # initialize output data
        data: list = None

        # apply filters, if any
        if filters is not None:

            # apply filters to standard policy data query
            q = api.schema.apply_entity_filters(q, db.Policy, filters)

            # if counting zeros, apply filters to query that counts zeros
            if include_zeros and "level" in filters:
                zero_filters: dict = dict(level=filters["level"])
                if loc_field in filters:
                    zero_filters[loc_field] = filters[loc_field]
                q_all_time = api.schema.apply_entity_filters(
                    q_all_time, db.Policy, zero_filters
                )

        # GET POLICY COUNTS BY LOCATION # ----------------------------------- #
        # if requested, only count the first policy with each group number,
        # otherwise count each policy
        # TODO put `counter` under if-else, move up code that uses it to here
        q_policies_by_loc: Query = None
        counter: PolicyStatusCounter = (
            self if (by_group_number or include_min_max) else None
        )
        if not by_group_number:
            q_policies_by_loc = select(
                (getattr(i.place, loc_field), count(i)) for i in q
            )
        else:
            q_distinct_group_nums: Query = (
                counter.__get_distinct_groups_in_policy_q(q)
            )
            q_policies_by_loc = select(
                (getattr(i.place, loc_field), count(i))
                for i in q_distinct_group_nums
            )

        # initialize core response data
        data_tmp = dict()
        place_name: str = None
        value: int = None
        for place_name, value in q_policies_by_loc:
            if place_name not in data_tmp:
                data_tmp[place_name] = PlaceObs(
                    place_name=place_name, value=value
                )
        data = list(data_tmp.values())

        # add "zeros" to the data, if requested
        if include_zeros:

            # get result of query showing which places had policies ever
            q_all_time = select(
                (i.place.iso3, i.place.area1, i.place.ansi_fips, i.place.level)
                for i in q_all_time
            )[:][:]

            # add a "zero" observation for each of these places if the place is
            # not already present in the response data
            iso3: str = None
            area1: str = None
            ansi_fips: str = None
            level: str = None
            for iso3, area1, ansi_fips, level in q_all_time:
                if geo_res == api.routing.GeoRes.country:
                    if iso3 not in data_tmp:
                        zero_obs: PlaceObs = PlaceObs(place_name=iso3, value=0)
                        data.append(zero_obs)
                elif geo_res == api.routing.GeoRes.state:
                    if iso3 == "USA" and area1 not in data_tmp:
                        zero_obs: PlaceObs = PlaceObs(
                            place_name=area1, value=0
                        )
                        data.append(zero_obs)
                elif geo_res == api.routing.GeoRes.county:
                    if iso3 == "USA" and ansi_fips not in data_tmp:
                        zero_obs: PlaceObs = PlaceObs(
                            place_name=ansi_fips, value=0
                        )
                        data.append(zero_obs)
                else:
                    raise ValueError("Unknown geo_res: " + geo_res)

        # order by value
        # TODO change to desc=True instead of negative value
        data.sort(key=lambda x: -x.value)

        # if one record requested, only return one record
        if one and len(data) > 0:
            data = [data[0]]

        # prepare basic response
        res_counted: str = (
            geo_res if not filter_by_subgeo else "sub-" + geo_res
        )

        # TODO mention parent geos if count_parent_geo enabled
        res = api.models.PlaceObsList(
            data=data,
            success=True,
            message=f"""Found {str(len(data))} values """
            f"""counting {res_counted} """
            f"""policies, grouped by {geo_res}""",
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
            ] = counter.__get_max_min_counts(
                geo_res=geo_res,
                filters_no_dates=filters_no_dates,
                level=level,  # TODO add support for multiple levels
                loc_field=loc_field,
                by_group_number=by_group_number,
                filter_by_subgeo=filter_by_subgeo,
            )

            # define min/max for all time
            res.min_all_time = min_max_counts[0]
            res.max_all_time = min_max_counts[1]

        # return response data
        return res

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
    def __get_max_min_counts(
        self,
        geo_res: str,
        filters_no_dates: dict,
        level: str,  # TODO add support for multiple levels
        loc_field: str,
        by_group_number: bool = False,
        filter_by_subgeo: bool = False,
    ) -> Tuple[PlaceObs, PlaceObs]:
        """Return place observations corresponding to the max- and min-valued
        policy status counts matching the provided arguments across all dates
        for which data are available.

        Args:
            geo_res (str): The geographic resolution of the policies to query.
            Must be one of "country", "state", or "county.

            filters_no_dates (dict): The filters to apply to policies, but not
            including any filters on date fields.

            level (str): The level of the place affected by the policy. Must be
            one of "Country", "State / Province", or "Local".

            loc_field (str): The data field from which to obtain the name of
            the location affected by the policy.

            by_group_number (bool, optional): True if only the first policy
            with each group number should be counted. This corrects for similar
            policies by avoiding overcounting them. Defaults to False.

            filter_by_subgeo (bool, optional): If True, counts all policies
            *beneath* the selected `geo_res` (geographic resolution). If false,
            only counts policies *at* it. Defaults to False.

        Returns:
            Tuple[PlaceObs, PlaceObs]: The min- and max-valued place
            observations, respectively.
        """
        q_filtered_policies: Query = select(i for i in Policy)

        # if counting policies beneath the geographic level defined by `level`,
        # add them to the filters
        if filter_by_subgeo:
            q_filtered_policies = api.schema.apply_subgeo_filter(
                q_filtered_policies, geo_res
            )

        # apply filters, if any
        if filters_no_dates is not None:
            q_filtered_policies = api.schema.apply_entity_filters(
                q_filtered_policies, Policy, filters_no_dates
            )

        # get number of active filtered policies by date and location active
        q: Query = None
        if by_group_number:
            q = select(
                (
                    dd.day_date,
                    getattr(pl, loc_field),
                    count(pd),
                )
                for pd in Policy_Date
                for dd in DayDate
                for pl in Place
                for p in q_filtered_policies
                for pbgn in Policy_By_Group_Number
                if date(2019, 1, 1) <= dd.day_date
                and dd.day_date <= raw_sql(f"""DATE '{str(date.today())}'""")
                and pd.start_date <= dd.day_date
                and pd.end_date >= dd.day_date
                and JOIN(pd.fk_policy_id == p.id)
                and JOIN(pl in p.place)
                and (pl.level == level or filter_by_subgeo)
                and (pl.iso3 == "USA" or level == "Country")
                and JOIN(pbgn.fk_policy_id == p)
            )
        else:
            q = select(
                (
                    dd.day_date,
                    getattr(pl, loc_field),
                    count(pd),
                )
                for pd in Policy_Date
                for dd in DayDate
                for pl in Place
                for p in q_filtered_policies
                if date(2019, 1, 1) <= dd.day_date
                and dd.day_date <= raw_sql(f"""DATE '{str(date.today())}'""")
                and pd.start_date <= dd.day_date
                and pd.end_date >= dd.day_date
                and JOIN(pd.fk_policy_id == p.id)
                and JOIN(pl in p.place)
                and (pl.level == level or filter_by_subgeo)
                and (pl.iso3 == "USA" or level == "Country")
            )

        # return first records for min and max number of active policies
        all_res = q.order_by(lambda i, j, k: (k, i, j))[:][:]
        q_min: Query = get_first(all_res, as_list=True)
        q_max: Query = [all_res[len(all_res) - 1]] if len(all_res) > 0 else []
        min_obs = self.__get_obs_from_q_result(q_min)
        max_obs = self.__get_obs_from_q_result(q_max)
        max_min_counts: Tuple[PlaceObs, PlaceObs] = (
            min_obs,
            max_obs,
        )
        return max_min_counts

    def __get_policies_with_distinct_groups(self) -> Query:
        """Returns a query selectinig the `id` of the first Policy record with
        each group number, as defined in the field `Policy.group_number`.

        Returns:
            Query: The query.
        """
        return select(
            (pony_min(i.id), i.group_number) for i in Policy
        ).order_by(lambda id, group_number: (group_number, id))

    def __get_obs_from_q_result(self, q: Query) -> PlaceObs:
        """Returns a place observation corresponding to the result of the query
        that selects a single record with the data fields required by a place
        observation: the datestamp, place name, and value.

        Args:
            q (Query): The query selecting a single record with data fields
            corresponding to and required by a place observation.

        Raises:
            ValueError: Query result has more than 1 result row.
            ValueError: Query result row has other than 3 column values.

        Returns:
            PlaceObs: The place observation.
        """
        place_obs: PlaceObs = None
        if len(q) == 1:
            datestamp: date = None
            place_name: str = None
            value: int = None
            q_vals: Tuple[date, str, int] = q[0]
            if len(q_vals) != 3:
                raise ValueError(
                    "Expected query result row to have 3 column values,"
                    " but found " + str(len(q_vals))
                )
            datestamp, place_name, value = q_vals
            place_obs: PlaceObs = PlaceObs(
                datestamp=datestamp,
                place_name=place_name,
                value=value,
            )
        else:
            return None
        return place_obs

    def _QueryResolver__validate_args(
        self, geo_res: str, filter_by_subgeo: bool
    ):
        """Validate input arguments."""
        # TODO errors for illogical count_parent_geo vals
        # TODO error if filter_by_subgeo AND count_parent_geo
        if geo_res == "county" and filter_by_subgeo is True:
            raise NotImplementedError(
                "Cannot count sub-geography policies for counties."
            )
