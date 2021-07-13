from api import schema
from typing import List, Set, Tuple
from pony.orm.core import Query, desc, group_concat, select


def get_export_data(
    filters: dict = dict(),
) -> Tuple[Query, List[str], Set[str]]:
    """Returns instances, export fields, and custom fields for policy data
    export operations, optionally filtered.

    Args:
        filters (dict, optional): The filters for policies. Defaults to dict().

    Returns:
        Tuple[Query, List[str], Set[str]]: The instances, export fields, and
        custom fields for the policy data given the filters provided.
    """

    # fields to export, ordered
    export_fields: List[str] = [
        "Policy.id",
        "Auth_Entity.Place.level",
        "Auth_Entity.Place.country_name",
        "Auth_Entity.Place.iso3",
        "Auth_Entity.Place.area1",
        "Auth_Entity.Place.area2",
        "Auth_Entity.name",
        "Auth_Entity.office",
        "Auth_Entity.official",
        "Place.level",
        "Place.country_name",
        "Place.iso3",
        "Place.area1",
        "Place.area2",
        "Policy.relaxing_or_restricting",
        "Policy.primary_ph_measure",
        "Policy.ph_measure_details",
        "Policy.subtarget",
        "Policy.desc",
        "Policy.date_issued",
        "Policy.date_start_effective",
        "Policy.date_end_anticipated",
        "Policy.date_end_actual",
        "Policy.intended_duration",
        "Policy.prior_policy",
        "Policy.announcement_data_source",
        "Policy.policy_name",
        "Policy.policy_type",
        "Policy.policy_data_source",
        "File.filename",
        "Policy.attachment_for_policy",
        "Policy.policy_number",
        "Policy.auth_entity_has_authority",
        "Policy.authority_name",
        "Policy.auth_entity_authority_data_source",
        "Place.home_rule",
        "Place.dillons_rule",
    ]

    # custom fields handled in special ways
    custom_fields: Set[str] = {"File.permalink"}

    # get filtered instances
    instances_tmp: Query = schema.get_policy(
        filters=filters, return_db_instances=True
    )

    # get instances (policies and related info)
    instances: Query = select(
        (
            i.id,
            group_concat(
                ae.place.level,
                "; ",
                distinct=True,
            ),
            group_concat(ae.place.country_name, "; ", distinct=True),
            group_concat(ae.place.iso3, "; ", distinct=True),
            group_concat(ae.place.area1, "; ", distinct=True),
            group_concat(ae.place.area2, "; ", distinct=True),
            group_concat(ae.name, "; ", distinct=True),
            group_concat(ae.office, "; ", distinct=True),
            group_concat(ae.official, "; ", distinct=True),
            group_concat(pl.level, "; ", distinct=True),
            group_concat(pl.country_name, "; ", distinct=True),
            group_concat(pl.iso3, "; ", distinct=True),
            group_concat(pl.area1, "; ", distinct=True),
            group_concat(pl.area2, "; ", distinct=True),
            i.relaxing_or_restricting,
            i.primary_ph_measure,
            i.ph_measure_details,
            i.subtarget,
            i.desc,
            i.date_issued,
            i.date_start_effective,
            i.date_end_anticipated,
            i.date_end_actual,
            i.intended_duration,
            group_concat((i_prior.id for i_prior in i.prior_policy), "; "),
            i.announcement_data_source,
            i.policy_name,
            i.policy_type,
            i.policy_data_source,
            group_concat((f.filename for f in i.file), "; ", distinct=True),
            group_concat((f.permalink for f in i.file), "; ", distinct=True),
            i.policy_number,
            i.auth_entity_has_authority,
            i.authority_name,
            i.auth_entity_authority_data_source,
            group_concat(pl.home_rule, "; ", distinct=True),
            group_concat(pl.dillons_rule, "; ", distinct=True),
        )
        for i in instances_tmp
        for pl in i.place
        for ae in i.auth_entity
        if pl.level != "Local plus state/province"
        and ae.place.level != "Local plus state/province"
    ).order_by(desc(21))
    return (instances, export_fields, custom_fields)
