from queryresolver.core import QueryResolver
from typing import Tuple
from api import schema
from datetime import date
from db.models import DayDate, Place, Policy, Policy_Date
from pony.orm.core import Query, count, select
from pony.orm.ormtypes import raw_sql
from pony.orm.core import min as pony_min
from api.models import PlaceObs
from api.util import cached


class PolicyStatusCounter(QueryResolver):
    def __init__(self):
        return None

    @cached
    def get_max_min_counts(
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
            q_filtered_policies = schema.apply_subgeo_filter(
                q_filtered_policies, geo_res
            )

        # apply filters, if any
        if filters_no_dates is not None:
            q_filtered_policies = schema.apply_entity_filters(
                q_filtered_policies, Policy, filters_no_dates
            )

        # if requested, only count the first policy with each group number
        if by_group_number:
            q_filtered_policies = self.get_distinct_groups_in_policy_q(
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

    def get_distinct_groups_in_policy_q(self, q: Query) -> Query:
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

    def validate_args(self, **kwargs):
        """Validate input arguments."""
        super().validate_args(**kwargs)
