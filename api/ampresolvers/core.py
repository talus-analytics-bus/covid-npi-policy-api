from queryresolver.core import QueryResolver
import api
from api.models import PlaceObs
from api.util import cached
from db import db
from db.models import DayDate, Place, Policy, Policy_Date
from typing import Any, Tuple
from datetime import date
from pony.orm.core import Query, count, db_session, select
from pony.orm.ormtypes import raw_sql
from pony.orm.core import min as pony_min


class PolicyStatusCounter(QueryResolver):
    def __init__(self):
        return None

    @cached
    @db_session
    def get_policy_status_counts(
        self,
        geo_res: str = None,
        name: str = None,
        filters: dict = dict(),
        by_group_number: bool = True,
        filter_by_subgeo: bool = False,
        include_zeros: bool = True,
        include_min_max: bool = True,
    ):
        """Return number of policies that match the filters for each geography
        on the date defined in the filters."""

        self._QueryResolver__validate_args(
            geo_res=geo_res, filter_by_subgeo=filter_by_subgeo
        )

        # get correct location field and level for filtering
        loc_field: str = api.helpers.get_loc_field_from_geo_res(geo_res)
        level = api.helpers.get_level_from_geo_res(geo_res)

        # for state/county level: filter by USA only
        for_usa_only: bool = geo_res in ("state", "county")
        if for_usa_only:
            filters["iso3"] = ["USA"]

        # get ordered policies from database
        # if not counting sub-[geo] only, then filter by level = [sub_geo]
        # otherwise, below filter by level != [geo or higher]
        if not filter_by_subgeo:
            filters["level"] = [level]

        # get policies
        q: Query = select(i for i in db.Policy)

        # get all locations with any data (before filters) for counting zeros,
        # if zeros requested
        q_all_time = select(i for i in db.Policy) if include_zeros else None

        # filter policies by correct levels if counting only sub-[geo] policies
        if filter_by_subgeo:
            q = api.schema.apply_subgeo_filter(q, geo_res)
            if include_zeros:
                q_all_time = api.schema.apply_subgeo_filter(
                    q_all_time, geo_res
                )

        # initialize output data
        data: list = None

        # apply filters if any
        if filters is not None:
            q = api.schema.apply_entity_filters(q, db.Policy, filters)
            if include_zeros and "level" in filters:
                q_all_time = api.schema.apply_entity_filters(
                    q_all_time, db.Policy, dict(level=filters["level"])
                )

        # get policy counts by location
        # if requested, only count the first policy with each group number
        q_loc: Query = None
        counter: PolicyStatusCounter = (
            self if (by_group_number or include_min_max) else None
        )
        if not by_group_number:
            q_loc = select((getattr(i.place, loc_field), count(i)) for i in q)
        else:
            q_distinct_groups: Query = (
                counter.__get_distinct_groups_in_policy_q(q)
            )
            q_loc = select(
                (getattr(i.place, loc_field), count(i))
                for i in q_distinct_groups
            )

        data_tmp = dict()
        for name, num in q_loc:
            if name not in data_tmp:
                data_tmp[name] = PlaceObs(place_name=name, value=num)
        data = list(data_tmp.values())

        # add "zeros" to the data if requested
        if include_zeros:
            q_all_time = select(
                (i.place.iso3, i.place.area1, i.place.level)
                for i in q_all_time
            )[:][:]
            iso3: str = None
            area1: str = None
            level: str = None
            for iso3, area1, level in q_all_time:
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

        # order by value
        data.sort(key=lambda x: -x.value)

        # prepare basic response
        res_counted: str = (
            geo_res if not filter_by_subgeo else "sub-" + geo_res
        )
        res = api.models.PlaceObsList(
            data=data,
            success=True,
            message=f"""Found {str(len(data))} values """
            f"""counting {res_counted} """
            f"""policies, grouped by {geo_res}""",
        )

        # add extra requested data to response # ---------------------------- #

        # get min and max values of policies matching the filters for any date,
        # not just the defined date (if defined)
        if include_min_max:
            # get filtered policies, skipping any date filters
            filters_no_dates: dict = dict()
            k: str = None
            v: Any = None
            for k, v in filters.items():
                if k != "dates_in_effect":
                    filters_no_dates[k] = v

            # get min/max for all time
            min_max_counts: Tuple[
                PlaceObs, PlaceObs
            ] = counter.__get_max_min_counts(
                geo_res=geo_res,
                filters_no_dates=filters_no_dates,
                level=level,
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
        q_group_numbers: Query = self.__get_policies_with_distinct_groups()
        q = select(i for i in q for j, _ in q_group_numbers if i.id == j)
        return q

    @cached
    def __get_max_min_counts(
        self,
        geo_res: str,
        filters_no_dates: dict,
        level: str,
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

        # if requested, only count the first policy with each group number
        if by_group_number:
            q_filtered_policies = self.__get_distinct_groups_in_policy_q(
                q_filtered_policies
            )

        # get number of active filtered policies by date and location active
        q: Query = select(
            (
                dd.day_date,
                getattr(pl, loc_field),
                count(pd),
            )
            for pd in Policy_Date
            for dd in DayDate
            for pl in Place
            for p in q_filtered_policies
            for p_pl in p.place
            if date(2019, 1, 1) <= dd.day_date
            and dd.day_date <= raw_sql(f"""DATE '{str(date.today())}'""")
            and pd.start_date <= dd.day_date
            and pd.end_date >= dd.day_date
            and pd.fk_policy_id == p.id
            and pl == p_pl
            and (pl.level == level or filter_by_subgeo)
        )

        # return first records for min and max number of active policies
        q_min: Query = q.order_by(lambda i, j, k: (k, i, j)).limit(1)
        q_max: Query = q.order_by(lambda i, j, k: (-k, i, j)).limit(1)
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
        ).order_by(lambda id, group_number: (id, group_number))

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
            raise ValueError(
                "Expected query to have 1 result row, but found " + str(len(q))
            )
        return place_obs

    def _QueryResolver__validate_args(
        self, geo_res: str, filter_by_subgeo: bool
    ):
        """Validate input arguments."""

        if geo_res == "county" and filter_by_subgeo is True:
            raise NotImplementedError(
                "Cannot count sub-geography policies for counties."
            )
