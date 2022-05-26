# Custom optimized routes for certain pages in the AMP website.

from api.ampresolvers.optionsetgetter.core import OptionSetGetter
import datetime
from typing import List
from api.ampresolvers.policystatuscounter.core import PolicyStatusCounter
from api.types import ClassName, GeoRes
from api.models import OptionSetList, PlaceObsList
from . import app
from fastapi import Query, Path

# policy status counter
counter: PolicyStatusCounter = PolicyStatusCounter()


@app.get(
    "/policy_status_counts_for_map/{geo_res}",
    response_model=PlaceObsList,
    response_model_exclude_unset=True,
    include_in_schema=False,
    tags=["Policies -- Custom"],
    summary="Return number of policies in effect by location matching filters"
    " and the provided geographic resolution",
)
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
    subtargets: List[str] = Query(list()),
    date: datetime.date = Query(None),
    sort: bool = False,
) -> PlaceObsList:
    """Return number of policies in effect by location matching filters and the
    provided geographic resolution

    """
    response: dict = None
    # validate args
    assert date is not None

    # get response
    response: PlaceObsList = counter.get_policy_status_counts_for_map(
        geo_res=geo_res,
        cats=categories,
        subcats=subcategories,
        subtargets=subtargets,
        date=date,
        sort=sort,
    )
    return response


getter: OptionSetGetter = OptionSetGetter()


@app.get(
    "/optionset_for_data",
    response_model=OptionSetList,
    response_model_exclude_unset=True,
    include_in_schema=False,
    tags=["Metadata -- Custom"],
    summary="Return all possible values for the provided field(s), e.g, "
    '"Policy.policy_name" belonging to the provided class, e.g., "Policy"'
    ' or "Plan".',
)
@app.get(
    "/get/optionset_for_data",
    response_model=OptionSetList,
    response_model_exclude_unset=True,
    include_in_schema=False,
    tags=["Metadata -- Custom"],
    summary="Return all possible values for the provided field(s), e.g, "
    '"Policy.policy_name" belonging to the provided class, e.g., "Policy"'
    ' or "Plan".',
)
async def get_optionset_for_data(
    class_name: ClassName = Query(
        ClassName.Policy,
        description="The name of the data type for which optionsets "
        "are requested",
    )
):
    response: OptionSetList = None
    try:
        assert class_name is not None
        response = getter.get_optionset_for_data(entity_name=class_name)
    except AssertionError:
        response = OptionSetList(
            success=False, message="Parameter error", data=None
        )
    return response
