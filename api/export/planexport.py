from api import schema
from typing import List, Set, Tuple
from pony.orm.core import Query, desc, group_concat, select


def get_export_data(
    filters: dict = dict(),
) -> Tuple[Query, List[str], Set[str], dict]:
    """Returns instances, export fields, and custom fields for plan data
    export operations, optionally filtered.

    Args:
        filters (dict, optional): The filters for plans. Defaults to dict().

    Returns:
        Tuple[Query, List[str], Set[str]]: The instances, export fields, and
        custom fields for the plan data given the filters provided.
    """

    # fields to export, ordered
    export_fields: List[str] = [
        "Plan.id",
        "Plan.org_name",
        "Plan.org_type",
        "Plan.name",
        "Plan.desc",
        "Plan.primary_loc",
        "Plan.date_issued",
        "Auth_Entity.Place.iso3",
        "Auth_Entity.Place.area2",
        "Plan.auth_entity_has_authority",
        "Plan.n_phases",
        "Plan.reqs_essential",
        "Plan.reqs_private",
        "Plan.reqs_school",
        "Plan.reqs_social",
        "Plan.reqs_hospital",
        "Plan.reqs_other",
        "Plan.plan_data_source",
        "Plan.attachment_for_plan",
        "Plan.announcement_data_source",
        "Plan.attachment_for_plan_announcement",
        "Plan.policy",
        "Plan.reqs_public",
        "Plan.residential",
    ]

    # define custom fields to be handled specially
    custom_fields: Set[str] = {
        "Plan.attachment_for_plan",
        "Plan.attachment_for_plan_announcement",
    }

    # get filtered plan instances
    instances_tmp: Query = schema.get_plan(
        filters=filters, return_db_instances=True
    )

    # get data to export to Excel
    instances: Query = select(
        (
            i.id,
            i.org_name,
            i.org_type,
            i.name,
            i.desc,
            i.primary_loc,
            i.date_issued,
            group_concat(ae.place.iso3, "; ", distinct=True),
            group_concat(ae.place.area2, "; ", distinct=True),
            i.auth_entity_has_authority,
            i.n_phases,
            i.reqs_essential,
            i.reqs_private,
            i.reqs_school,
            i.reqs_social,
            i.reqs_hospital,
            i.reqs_other,
            i.plan_data_source,
            group_concat(
                (f.permalink for f in i.file if f.type == "plan"), "; "
            ),
            i.announcement_data_source,
            group_concat(
                (f.permalink for f in i.file if f.type == "plan_announcement"),
                "; ",
            ),
            i.policy,
            i.reqs_public,
            i.residential,
        )
        for i in instances_tmp
        for ae in i.auth_entity
    ).order_by(desc(7))
    return (instances, export_fields, custom_fields, {})
