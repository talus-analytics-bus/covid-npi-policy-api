from fastapi.param_functions import Query
from api.util import cached
from api.types import ClassName
from api.models import OptionSetList, OptionSetRecord, OptionSetRecords
from collections import defaultdict
from typing import Any, DefaultDict, Dict, List, Set, Tuple
from db.models import Glossary, Place, Policy
from pony.orm.core import count, db_session, select

from db import db


class OptionSetGetter:
    def __init__(self) -> None:
        """Define new OptionSetGetter"""
        return None

    @db_session
    def get_optionset_for_data(self, entity_name: ClassName) -> OptionSetList:
        """Return optionsets for use in Data page of COVID AMP website.

        This function is optimized for the Data page.

        Returns:
            OptionSetList: List of optionset values for the Data page.
        """
        # prepare and send optionset lists based on class needed
        # location optionsets (used for both classes)
        data: OptionSetRecords = dict()
        area2_vals: Set[str] = None
        if entity_name == ClassName.Policy:
            area2_vals = {"Local"}
            # cat and subcat optionsets
            cat_and_subcat_optionsets: dict = (
                self.__get_cat_and_subcat_optionsets(entity_name=entity_name)
            )

            data.update(cat_and_subcat_optionsets)

            # get other optionsets
            list_field_name: str = None
            for list_field_name in ("subtarget",):
                data[list_field_name] = self.__get_field_optionset(
                    entity_name=entity_name,
                    field=list_field_name,
                    is_list_field=True,
                )

        elif entity_name == ClassName.Plan:
            area2_vals = {
                "Government",
                "University",
                "For-profit",
                "Non-profit",
            }
            # get other optionsets
            field_name: str = None
            for field_name in ("org_type",):
                data[field_name] = self.__get_field_optionset(
                    entity_name=entity_name, field=field_name
                )

        # add location optionsets
        location_optionsets: OptionSetRecords = self.__get_location_optionsets(
            entity_name=entity_name, area2_vals=area2_vals
        )
        data.update(location_optionsets)

        # sort optionset records A-Z
        field: str = None
        for field in data:
            data[field].sort(key=self.__sort_optionset_by_value)

        # return all optionsets
        return OptionSetList(success=True, message="Message", data=data)

    @cached
    @db_session
    def __get_field_optionset(
        self, entity_name: ClassName, field: str, is_list_field: bool = False
    ) -> List[OptionSetRecord]:
        q: Query = select(
            (getattr(p, field)) for p in entity_name.get_db_model()
        )

        q_result: List[Tuple[Any]] = q[:][:]
        value: str = None
        id: int = 0
        optionset: OptionSetRecords = list()
        checked_vals: Set[str] = set()

        # add each possible value of the field to the optionset
        # if it's a list field, add each subvalue
        if is_list_field:
            for value in q_result:
                sub_value: str = None
                for sub_value in value:
                    if sub_value in checked_vals:
                        continue
                    else:
                        checked_vals.add(sub_value)
                        optionset.append(
                            OptionSetRecord(id=id, value=sub_value)
                        )
                        id = id + 1

        # if field is not a list of values, take them directly
        else:
            for value in q_result:
                if value in checked_vals:
                    continue
                else:
                    optionset.append(OptionSetRecord(id=id, value=value))
                    id = id + 1

        return optionset

    @cached
    @db_session
    def __get_location_optionsets(
        self, entity_name: ClassName, area2_vals: Set[str] = {"Local"}
    ) -> OptionSetRecords:
        """Returns the optionsets for location fields that apply to the given
        entity name.

        Args:
            entity_name (ClassName): The entity name of interest.

            area2_vals (Set[str], optional): The levels of place that should be
            considered `area2` for this entity. Defaults to {"Local"}.

        Returns:
            OptionSetRecords: [description]
        """

        # get the field on the place model that corresponds to this entity
        place_field: str = entity_name.get_place_field_name()

        # retrieve location information for the entity based on its places,
        # keeping only those places that are linked to instances of the entity
        q: Query = select(
            (pl.country_name, pl.area1, pl.area2, pl.level)
            for pl in Place
            if pl.level != "Local plus state/province"
            and count(getattr(pl, place_field)) > 0
        )
        q_result: List[Tuple[str, str, str]] = q[:][:]

        # define fields to unpack from query result
        country_name: str = None
        area1: str = None
        area2: str = None
        level: str = None

        # define unique ID seequences for each type of optionset
        id_country_name: int = 0
        id_area1: int = 0
        id_area2: int = 0
        # id_level: int = 0

        # define variable to hold optionsets and to state whether or not they
        # have been defined yet
        optionsets: OptionSetRecords = dict(
            country_name=list(),
            area1=list(),
            area2=list()
            # country_name=list(), area1=list(), area2=list(), level=list()
        )
        checked: dict = dict(
            country_name=dict(),
            area1=dict(),
            area2=dict()
            # country_name=dict(), area1=dict(), area2=dict(), level=dict()
        )

        # Add optionset entries for each level of place
        for country_name, area1, area2, level in q_result:
            # if level not in checked["level"]:
            #     checked["level"][level] = True
            #     optionsets["level"].append(
            #         OptionSetRecord(id=id_level, value=level)
            #     )
            #     id_level += 1

            if (
                level == "Country"
                and country_name not in checked["country_name"]
            ):
                checked["country_name"][country_name] = True
                optionsets["country_name"].append(
                    OptionSetRecord(
                        id=id_country_name, value=country_name, group=level
                    )
                )
                id_country_name += 1

            # NOTE Tribal nations are tagged as "intermediate areas" (`area1`)
            # but are rendered in optionsets as countries
            elif (
                level == "Tribal nation"
                and area1 not in checked["country_name"]
            ):
                checked["country_name"][area1] = True
                optionsets["country_name"].append(
                    OptionSetRecord(
                        id=id_country_name, value=area1, group=level
                    )
                )
                id_country_name += 1
            elif level == "State / Province" and area1 not in checked["area1"]:
                checked["area1"][area1] = True
                optionsets["area1"].append(
                    OptionSetRecord(
                        id=id_area1, value=area1, group=country_name
                    )
                )
                id_area1 += 1
            elif level in area2_vals and area2 not in checked["area2"]:
                checked["area2"][area2] = True
                optionsets["area2"].append(
                    OptionSetRecord(id=id_area2, value=area2, group=area1)
                )
                id_area2 += 1

        # return optionsets
        return optionsets

    @cached
    @db_session
    def __get_cat_and_subcat_optionsets(
        self, entity_name: ClassName
    ) -> OptionSetRecords:
        """Return optionsets for categories and subcategories of policies that
        are in the COVID AMP database.

        Returns:
            [type]: [description]
        """

        # validation
        if entity_name != ClassName.Policy:
            raise ValueError("Unexpected class name: " + entity_name.name)

        # get field data
        q: Query = select(
            (i.primary_ph_measure, i.ph_measure_details) for i in Policy
        )
        cat_subcat_optionsets: dict = dict(
            primary_ph_measure=list(), ph_measure_details=list()
        )
        checked: dict = dict(
            primary_ph_measure=dict(), ph_measure_details=dict()
        )
        cat_id: int = 0
        subcat_id: int = 0
        cat: str = None
        subcat: str = None
        for cat, subcat in q:
            if cat not in checked["primary_ph_measure"]:
                checked["primary_ph_measure"][cat] = True
                cat_subcat_optionsets["primary_ph_measure"].append(
                    OptionSetRecord(id=cat_id, value=cat)
                )
                cat_id = cat_id + 1
            if subcat not in checked["ph_measure_details"]:
                checked["ph_measure_details"][subcat] = True
                cat_subcat_optionsets["ph_measure_details"].append(
                    OptionSetRecord(id=subcat_id, value=subcat, group=cat)
                )
                subcat_id = subcat_id + 1

        return cat_subcat_optionsets

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
                for i in Place
                if count(getattr(i, class_name_field)) > 0
                and i.level != "Local plus state/province"
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

            # Commented out below code because court challenges disabled
            # if (
            #     entity_name_and_field
            #     == "Court_Challenge.government_order_upheld_or_enjoined"
            # ):
            #     data["government_order_upheld_or_enjoined"].append(
            #         {"id": -1, "value": "Pending", "label": "Pending"},
            #     )

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

    def __sort_optionset_by_value(self, o: OptionSetRecord) -> str:
        """Return string with which the optionset record should be sorted,
        accounting for desired custom ordering behavior.

        Args:
            o (OptionSetRecord): The optionset record.

        Returns:
            str: The string determining its sort order. Note that "AAA"
            represents the top-sorted option and "ZZZ" the lowest.
        """
        if o.value in ("Social distancing", "United States of America (USA)"):
            return "AAA"
        elif o.value == "Face mask":
            return "AAB"
        elif o.value in ("Other", "Unspecified", "Local"):
            return "ZZZ"
        else:
            return o.value
