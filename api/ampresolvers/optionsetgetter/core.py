from collections import defaultdict
from itertools import groupby
from typing import Any, DefaultDict, Dict, List
from db.models import Glossary, Place
from pony.orm.core import count, db_session, select

from db import db


class OptionSetGetter:
    def __init__(self):
        return None

    @db_session
    # @cached
    def get_optionset(
        self,
        fields: list = list(),
        class_name: str = "Policy",
        geo_res: str = None,
        state_name: str = None,
        iso3: str = None,
    ):
        """Given a list of data fields and an entity name, returns the possible
        values for those fields based on what data are currently in
        the database.

        TODO add support for getting possible fields even if they haven't been
        used yet in the data

        TODO list unspecified last

        TODO remove bottleneck in AWS deployed version

        Parameters
        ----------
        fields : list
            List of strings of data fields names.
        entity_name : str
            The name of the entity for which to check possible values.

        Returns
        -------
        dict
            List of possible optionset values for each field, contained in a
            response dictionary

        """

        # define which data fields use groups
        # TODO dynamically
        fields_using_groups = (
            "Policy.ph_measure_details",
            "Court_Challenge.complaint_subcategory_new",
        )
        fields_using_geo_groups = ("Place.area1", "Place.area2")

        # define output data dict
        data = dict()

        # get all glossary terms if needed
        need_glossary_terms = any(
            d_str in fields_using_groups for d_str in fields
        )
        glossary_terms = (
            select(i for i in db.Glossary)[:][:]
            if need_glossary_terms
            else list()
        )

        # check places relevant only for the entity of `class_name`
        class_name_field = "policies" if class_name == "Policy" else "plans"

        # get all places if needed
        need_places = any(d_str in fields_using_geo_groups for d_str in fields)
        place_tuples: List[tuple] = (
            select(
                (i.area1, i.area2, i.country_name)
                for i in db.Place
                if len(getattr(i, class_name_field)) > 0
            )[:][:]
            if need_places
            else list()
        )

        # for each field to get optionset values for:
        entity_name_and_field: str = None
        for entity_name_and_field in fields:

            # split into entity class name and field
            entity_name, field = entity_name_and_field.split(".")
            entity_class = getattr(db, entity_name)

            # get all possible values for the field in the database, and sort
            # them such that "Unspecified" is last
            # TODO handle other special values like "Unspecified" as needed
            options = None
            if field == "level":
                if (
                    iso3 is not None or state_name is not None
                ) and geo_res is not None:
                    raise NotImplementedError(
                        f"""Cannot request optionset for `{field}` """
                        f"""when filtering by `{geo_res}`"""
                    )
                options = (
                    select(
                        getattr(i, field)
                        for i in entity_class
                        if count(getattr(i, class_name_field)) > 0
                    )
                    .filter(lambda x: x is not None)
                    .filter(lambda x: x != "Local plus state/province")
                )
            elif field == "country_name":
                if (
                    iso3 is not None or state_name is not None
                ) and geo_res is not None:
                    raise NotImplementedError(
                        f"""Cannot request optionset for `{field}` """
                        f"""when filtering by `{geo_res}`"""
                    )
                options = select(
                    (getattr(i, field), i.level)
                    for i in entity_class
                    if count(getattr(i, class_name_field)) > 0
                ).filter(
                    lambda field_val, level: field_val is not None
                    and (level == "Country" or level == "Tribal nation")
                )
            else:
                if entity_name not in ("Policy", "Plan"):
                    if entity_name == "Place":

                        # select values only from places that have policies
                        # and do not have hybrid levels
                        options = select(
                            getattr(i, field)
                            for i in Place
                            if len(getattr(i, class_name_field)) > 0
                            and i.level != "Local plus state/province"
                        ).filter(lambda x: x is not None)
                    else:
                        options = select(
                            getattr(i, field) for i in entity_class
                        ).filter(lambda x: x is not None)
                else:

                    options = select(
                        getattr(i, field)
                        for i in entity_class
                        if (
                            iso3 in i.place.iso3
                            or iso3 is None
                            or geo_res != "country"
                        )
                        and (
                            state_name in i.place.area1
                            or state_name is None
                            or geo_res != "state"
                        )
                    ).filter(lambda x: x is not None)

            # get objects
            options_tmp: List[Any] = options[:][:]

            # if list of tuples, store tuples indexed by first element
            options: List[str] = None
            groups_by_option: Dict[str, str] = None
            if len(options_tmp) > 0:
                first: Any = options_tmp[0]
                if type(first) == tuple:
                    groups_by_option = dict()
                    options = list()
                    cur_option_tuple: tuple = None
                    for cur_option_tuple in options_tmp:
                        groups_by_option[
                            cur_option_tuple[0]
                        ] = cur_option_tuple[1]
                        options.append(cur_option_tuple[0])
                elif isinstance(first, list):
                    options = list(
                        set(
                            [
                                item
                                for sublist in options_tmp
                                for item in sublist
                            ]
                        )
                    )
                else:
                    options = options_tmp

            options.sort()
            options.sort(key=lambda x: x != "Face mask")
            options.sort(key=lambda x: x != "Social distancing")
            options.sort(key=lambda x: x == "Other")
            options.sort(key=lambda x: x in ("Unspecified", "Local"))

            # skip blank strings
            options = list(filter(lambda x: x.strip() != "", options))

            # assign groups, if applicable
            uses_custom_groups: bool = (
                entity_name_and_field == "Place.country_name"
            )
            uses_nongeo_groups = entity_name_and_field in fields_using_groups
            uses_geo_groups = entity_name_and_field in fields_using_geo_groups
            uses_groups = (
                uses_nongeo_groups or uses_geo_groups or uses_custom_groups
            )
            if uses_custom_groups:
                options = options_tmp
            elif uses_nongeo_groups:
                options_with_groups = list()

                # index glossary terms by info needed to identify parent terms
                glossary_terms_grouped: DefaultDict = defaultdict(
                    lambda: defaultdict(dict)
                )
                term: Glossary = None
                for term in glossary_terms:
                    glossary_terms_grouped[term.entity_name][term.field][
                        term.subterm
                    ] = term

                option: Any = None
                for option in options:
                    # get group from glossary data
                    parent = glossary_terms_grouped[entity_name][field].get(
                        option, None
                    )

                    # if a parent was found use its term as the group,
                    # otherwise specify "Other" as the group
                    if parent:
                        options_with_groups.append([option, parent.term])
                    else:
                        # TODO decide best way to handle "Other" cases
                        options_with_groups.append([option, "Other"])
                options = options_with_groups
            elif uses_geo_groups:
                options_with_groups = list()

                if field == "area1":
                    places_by_area1: dict = {}
                    p: tuple = None
                    for p in place_tuples:
                        if p[2] == "N/A":
                            continue
                        else:
                            places_by_area1[p[0]] = p
                    for option in options:
                        # get group from glossary data
                        parent = places_by_area1.get(option, None)

                        # if a parent was found use its term as the group,
                        # otherwise specify "Other" as the group
                        if parent:
                            options_with_groups.append([option, parent[2]])
                        else:
                            continue
                elif field == "area2":
                    places_by_area1: dict = {}
                    p: tuple = None
                    for p in place_tuples:
                        if p[2] == "N/A":
                            continue
                        else:
                            places_by_area1[p[1]] = p
                    option: Any = None
                    for option in options:
                        # get group from glossary data
                        parent = places_by_area1.get(option, None)

                        # if a parent was found use its term as the group,
                        # otherwise specify "Other" as the group
                        if parent:
                            options_with_groups.append([option, parent[0]])
                        else:
                            continue

                options = options_with_groups

            # return values and labels, etc. for each option
            id = 0

            # init list of optionset values for the field
            data[field] = []

            # for each possible option currently in the data
            for dd in options:

                # append an optionset entry
                value = dd if not uses_groups else dd[0]

                # skip unspecified values
                if value == "Unspecified":
                    continue

                group = None if not uses_groups else dd[1]
                datum = {
                    "id": id,
                    "value": value,
                }
                if uses_groups:
                    datum["group"] = group
                data[field].append(datum)
                id = id + 1

            if (
                entity_name_and_field
                == "Court_Challenge.government_order_upheld_or_enjoined"
            ):
                data["government_order_upheld_or_enjoined"].append(
                    {"id": -1, "value": "Pending", "label": "Pending"},
                )

        # apply special ordering
        if "ph_measure_details" in data:
            data["ph_measure_details"].sort(
                key=lambda x: "other" in x["value"].lower()
            )

        return {
            "data": data,
            "success": True,
            "message": f"""Returned {len(fields)} optionset lists""",
        }
