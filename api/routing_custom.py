import datetime
from typing import List
from api.ampresolvers.policystatuscounter.core import PolicyStatusCounter
from api.types import GeoRes
from api.models import PlaceObsList
from . import app
from fastapi import Query, Path


counter: PolicyStatusCounter = PolicyStatusCounter()


@app.get(
    "/get/policy_status_counts_for_map/{geo_res}",
    response_model=PlaceObsList,
    response_model_exclude_unset=True,
    include_in_schema=False,
    tags=["Policies -- Custom"],
    summary="Return number of policies in effect by location matching filters"
    " and the provided geographic resolution",
)
async def get_policy_status_counts_for_map(
    geo_res: GeoRes = Path(
        GeoRes.state,
        description="The geographic resolution for which to return data",
    ),
    categories: List[str] = Query(list()),
    subcategories: List[str] = Query(list()),
    date: datetime.date = Query(None),
) -> PlaceObsList:
    """Return number of policies in effect by location matching filters and the
    provided geographic resolution

    """
    response: dict = None
    try:
        # validate args
        assert date is not None

        # get response
        response: PlaceObsList = counter.get_policy_status_counts_for_map(
            geo_res=geo_res,
            cats=categories,
            subcats=subcategories,
            date=date,
            sort=True,
        )
    except AssertionError:
        response = {"message": "Parameter error", "data": [], "success": False}
    return response
