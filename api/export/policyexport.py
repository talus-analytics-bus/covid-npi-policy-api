from typing import Callable, Dict, List, Set, Tuple

from pony.orm.core import Query, desc, group_concat, select

from api import schema
from db.models import Policy


def get_export_data(
    filters: dict = dict(),
) -> Tuple[Query, List[str], Set[str], dict]:
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
    return (instances, export_fields, custom_fields, {})


def get_export_data_summary(
    filters: dict = dict(),
) -> Tuple[Query, List[str], Set[str], dict]:
    """Returns instances, export fields, and custom fields for policy data
    export operations, optionally filtered, in a simplified format.

    Args:
        filters (dict, optional): The filters for policies. Defaults to dict().

    Returns:
        Tuple[Query, List[str], Set[str]]: The instances, export fields, and
        custom fields for the policy data given the filters provided.
    """

    # fields to export, ordered
    export_fields: List[str] = [
        "Policy.id",
        "Auth_Entity.Place.loc",
        # "Auth_Entity.Place.level",
        "Place.loc",
        # "Place.level",
        "Policy.policy_name",
        # "Policy.desc",
        "Policy.primary_ph_measure",
        "Policy.ph_measure_details",
        "Policy.subtarget",
        "Policy.date_start_effective",
        "Policy.authority_name",
        "File.permalink",
    ]

    # custom fields handled in special ways
    custom_fields: Set[str] = {
        "Policy.id",
        "Auth_Entity.Place.loc",
        # "Auth_Entity.Place.level",
        "Place.loc",
        # "Place.level",
        "Policy.policy_name",
        # "Policy.desc",
        "Policy.primary_ph_measure",
        "Policy.ph_measure_details",
        "Policy.subtarget",
        "File.permalink",
    }

    delim: str = ", "
    custom_value_getters: Dict[str, Callable] = {
        "Policy.id": None,
        "Auth_Entity.Place.loc": lambda inst: f"""{inst[1]}""",
        # "Auth_Entity.Place.level": None,
        "Place.loc": lambda inst: f"""{inst[2]}""",
        # "Place.level": None,
        "Policy.policy_name": lambda inst: f"""{inst[3]}""",
        # "Policy.desc": None,
        "Policy.primary_ph_measure": lambda inst: f"""Category: {inst[4]}"""
        f"""\n\nSubcategory: {inst[5]}"""
        + (
            f"""\n\nTargets: {delim.join(inst[6])}"""
            if len(inst[6]) > 0
            else ""
        ),
        "Policy.ph_measure_details": None,
        "Policy.subtarget": None,
    }

    # get filtered instances
    instances_tmp: Query = schema.get_policy(
        filters=filters, return_db_instances=True
    )

    # get instances (policies and related info)
    instances: Query = select(
        (
            i.id,
            group_concat(
                f"{ae.place.loc} ({ae.place.level})", "; ", distinct=True
            ),
            group_concat(f"{pl.loc} ({pl.level})", "; ", distinct=True),
            f"{i.policy_name}:\n{i.desc}",
            i.primary_ph_measure,
            i.ph_measure_details,
            i.subtarget,
            i.date_start_effective,
            i.authority_name,
            group_concat((f.permalink for f in i.file), "; ", distinct=True),
        )
        for i in instances_tmp
        for pl in i.place
        for ae in i.auth_entity
        if pl.level != "Local plus state/province"
        and ae.place.level != "Local plus state/province"
    )
    # .order_by(lambda a, b, c, d, e, f, g, h, i, j, k, l, m: "1 " + str(k))
    return (instances, export_fields, custom_fields, custom_value_getters)


# Custom metadata used in simple policy Excels
policy_simple_custom_metadata: List[dict] = [
    {
        "field": "id",
        "display_name": "Unique ID",
        "colgroup": "Unique ID",
        "definition": "A unique identifier associated with data in each row. The data is captured so that each row represents a single policy, per date issued, per authority and per area affected.",
        "possible_values": "Numeric: Any unique integer value",
        "entity_name": "Policy",
        "export": False,
    },
    {
        "field": "loc",
        "display_name": "Affected location",
        "colgroup": "Locations involved",
        "definition": "The location affected by the policy, including its level, e.g., country, state / province, or local",
        "possible_values": "Text: Any location name and level",
        "entity_name": "Place",
        "export": True,
    },
    {
        "field": "loc",
        "display_name": "Authorizing location",
        "colgroup": "Locations involved",
        "definition": "The location authorizing (i.e., making) the policy, including its level, e.g., country, state / province, or local",
        "possible_values": "Text: Any location name and level",
        "entity_name": "Auth_Entity.Place",
        "export": True,
    },
    {
        "field": "policy_name",
        "display_name": "Policy name and description",
        "colgroup": "Policy information",
        "definition": "The complete title of the law or policy, including any relevant numerical information, and a written description of the policy or law and who it impacts. This summary is chosen from a policy researcher, taken from the policy itself.",
        "possible_values": "Text: Any text value",
        "entity_name": "Policy",
        "export": True,
    },
    {
        "field": "primary_ph_measure",
        "display_name": "Policy category, subcategory, and targets",
        "colgroup": "Policy information",
        "definition": "Categorization of the overall intention of the policy, more detailed information about the intention of the policy or law, and (if available) the primary population, location or entities impacted by the policy or law",
        "possible_values": """Categories: \nOne of:
Social distancing
Emergency declarations
Travel restrictions
Enabling and relief measures
Support for public health and clinical capacity
Contact tracing/Testing
Military mobilization
Face mask

Subcategories and targets: See all possible values, with corresponding definitions, in glossary of data dictionary""",
        "entity_name": "Policy",
        "export": True,
    },
    {
        "field": "authority_name",
        "display_name": "Relevant authority",
        "colgroup": "Policy information",
        "definition": "Title of legal authority for entity to enact the policy",
        "possible_values": "Text: Any text value",
        "entity_name": "Policy",
        "export": True,
    },
    {
        "field": "date_start_effective",
        "display_name": "Effective start date",
        "colgroup": "Policy information",
        "definition": "Date on which policy was enacted",
        "possible_values": "Date: format mm/dd/yyyy",
        "entity_name": "Policy",
        "export": True,
    },
    {
        "field": "permalink",
        "display_name": "PDF / Link",
        "colgroup": "Files",
        "definition": "URL of permanently hosted PDF document(s) for "
        "the policy",
        "possible_values": "Any URL(s)",
        "entity_name": "File",
        "export": True,
    },
]
