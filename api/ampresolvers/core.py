from queryresolver.core import QueryResolver
from typing import Tuple
from api import schema
from datetime import date
from db.models import DayDate, Place, Policy, Policy_Date
import pony
from pony.orm.core import Query, count, distinct, select
from pony.orm.ormtypes import raw_sql
from api.models import PlaceObs
from api.util import cached


class PolicyStatusCounter(QueryResolver):
    def __init__(self):
        return None

    @cached
    def get_max_min_counts(
        self,
        filters_no_dates: dict,
        level: str,
        loc_field: str,
        by_group_number: bool = False,
    ) -> PlaceObs:
        q_filtered_policies: Query = select(i for i in Policy)
        if filters_no_dates is not None:
            q_filtered_policies = schema.apply_entity_filters(
                q_filtered_policies, Policy, filters_no_dates
            )
        if by_group_number:
            q_group_numbers = select(
                (min(i.id), i.group_number) for i in Policy
            )
            q_filtered_policies = select(
                i
                for i in q_filtered_policies
                for j, _ in q_group_numbers
                if i.id == j
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
            and pl.level == level
        )

        # return first records for min and max number of active policies
        q_max: Query = q.order_by(lambda i, j, k: (-k, i, j)).limit(1)
        q_min: Query = q.order_by(lambda i, j, k: (k, i, j)).limit(1)
        max_obs = self.create_min_max_obs_from_q_result(q_max)
        min_obs = self.create_min_max_obs_from_q_result(q_min)
        max_min_counts: Tuple[PlaceObs, PlaceObs] = (
            max_obs,
            min_obs,
        )
        return max_min_counts

    def create_min_max_obs_from_q_result(self, q_max):
        max_obs: PlaceObs = None
        if len(q_max) == 1:
            datestamp: date = None
            place_name: str = None
            value: int = None
            datestamp, place_name, value = q_max[0]
            max_obs: PlaceObs = PlaceObs(
                datestamp=datestamp,
                place_name=place_name,
                value=value,
            )
        return max_obs

    def validate_args(self, **kwargs):
        super().validate_args(**kwargs)
