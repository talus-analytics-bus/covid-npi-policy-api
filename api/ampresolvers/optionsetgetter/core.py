from collections import defaultdict
from typing import DefaultDict, List
from db.models import Glossary
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
            if field == "country_name" or field == "level":
                if (
                    iso3 is not None or state_name is not None
                ) and geo_res is not None:
                    raise NotImplementedError(
                        f"""Cannot request optionset for `{field}` """
                        f"""when filtering by `{geo_res}`"""
                    )
                options = select(
                    getattr(i, field)
                    for i in entity_class
                    if count(getattr(i, class_name_field)) > 0
                ).filter(lambda x: x is not None)
                if field == "level":
                    options = options.filter(
                        lambda x: x != "Local plus state/province"
                    )
            else:
                if entity_name not in ("Policy", "Plan"):
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
            options = options[:][:]
            if len(options) > 0 and isinstance(options[0], list):
                options = list(
                    set([item for sublist in options for item in sublist])
                )

            options.sort()
            options.sort(key=lambda x: x != "Face mask")
            options.sort(key=lambda x: x != "Social distancing")
            options.sort(key=lambda x: x == "Other")
            options.sort(key=lambda x: x in ("Unspecified", "Local"))

            # skip blank strings
            options = list(filter(lambda x: x.strip() != "", options))

            # assign groups, if applicable
            uses_nongeo_groups = entity_name_and_field in fields_using_groups
            uses_geo_groups = entity_name_and_field in fields_using_geo_groups
            uses_groups = uses_nongeo_groups or uses_geo_groups
            if uses_nongeo_groups:
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
                        # TODO fioptiogure out best way to handle "Other" cases
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
                    "label": value,
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
