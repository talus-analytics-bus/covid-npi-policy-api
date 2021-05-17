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
        q_filtered_policies: Query = select(i for i in Policy)

        # if counting policies beneath the geographic level defined by `level`,
        # add them to the filters
        if filter_by_subgeo:
            q_filtered_policies = schema.apply_subgeo_filter(
                q_filtered_policies, geo_res
            )

        if filters_no_dates is not None:
            q_filtered_policies = schema.apply_entity_filters(
                q_filtered_policies, Policy, filters_no_dates
            )
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
        min_obs = self.get_min_max_obs_from_q_result(q_min)
        max_obs = self.get_min_max_obs_from_q_result(q_max)
        max_min_counts: Tuple[PlaceObs, PlaceObs] = (
            min_obs,
            max_obs,
        )
        return max_min_counts

    def get_distinct_groups_in_policy_q(self, q: Query) -> Query:
        q_group_numbers: Query = self.__get_policies_with_distinct_groups()
        q = select(i for i in q for j, _ in q_group_numbers if i.id == j)
        return q

    def __get_policies_with_distinct_groups(self) -> Query:
        return select(
            (pony_min(i.id), i.group_number) for i in Policy
        ).order_by(lambda id, group_number: (id, group_number))

    def get_min_max_obs_from_q_result(self, q) -> PlaceObs:
        obs: PlaceObs = None
        if len(q) == 1:
            datestamp: date = None
            place_name: str = None
            value: int = None
            datestamp, place_name, value = q[0]
            obs: PlaceObs = PlaceObs(
                datestamp=datestamp,
                place_name=place_name,
                value=value,
            )
        return obs

    def validate_args(self, **kwargs):
        super().validate_args(**kwargs)
