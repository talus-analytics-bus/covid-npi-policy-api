"""Project-specific plugins for export module"""
# standard modules
from io import BytesIO
from datetime import date
from collections import defaultdict
import types

# 3rd party modules
from pony.orm import db_session, select
from openpyxl import load_workbook
import pandas as pd
import pprint

# local modules
from .formats import WorkbookFormats
from .export import ExcelExport, WorkbookTab
from api import schema

# constants
pp = pprint.PrettyPrinter(indent=4)


class CovidPolicyTab(WorkbookTab):
    """Add a specific parameter denoting whether a tab for court challenges
    is part of a workbook containing court challenges only. Note: Workbooks
    containing court challenges only will contain any court challenges that
    pass filters provided, whereas workbooks containing policies as well will
    containing any court challenges associated with those policies.

    """

    def __init__(self, challenges_only=False, **kwargs):
        # assign project-specific parameter `challenges_only`
        self.challenges_only = challenges_only

        # inherit superclass WorkbookTab
        super().__init__(**kwargs)


class CovidPolicyExportPlugin(ExcelExport):
    """Covid Policy Tracker-specific ExcelExport-style class that writes data
    to an XLSX file when the instance method `build` is called.

    Parameters
    ----------
    db : type
        Description of parameter `db`.

    Attributes
    ----------
    data : type
        Description of attribute `data`.
    init_irow : type
        Description of attribute `init_irow`.
    sheet_settings : type
        Description of attribute `sheet_settings`.
    default_data_getter : type
        Description of attribute `default_data_getter`.
    db

    """

    def __init__(self, db, filters, class_name):
        self.db = db
        self.data = None
        self.init_irow = {
            "logo": 0,
            "title": 1,
            "subtitle": 2,
            "intro_text": 3,
            "gap": 4,
            "colgroups": 5,
            "colnames": 6,
            "data": 7,
        }
        self.filters = filters

        # Define a sheet settings instance for each tab of the XLSX
        # If class_name is all, then export policies and plans, otherwise
        # export whichever is defined in `class_name`
        export_policies_and_plans = class_name == "All_data_recreate"
        tabs = None
        if not export_policies_and_plans:
            if class_name == "Policy":
                tabs = [
                    {"s": "Policy", "p": "Policies"},
                    # {
                    #     's': 'Court_Challenge',
                    #     'p': 'Court challenges'
                    # }
                ]
            elif class_name == "Plan":
                tabs = [{"s": "Plan", "p": "Plans"}]
            # elif class_name == 'Court_Challenge':
            #     tabs = [{
            #         's': 'Court_Challenge',
            #         'p': 'Court challenges'
            #     }]
        else:
            # export all data
            tabs = (
                {"s": "Policy", "p": "Policies"},
                {"s": "Plan", "p": "Plans"},
                # {
                #     's': 'Court_Challenge',
                #     'p': 'Court challenges'
                # }
            )

        self.sheet_settings = []
        for tab in tabs:
            self.sheet_settings += [
                CovidPolicyTab(
                    name=tab["p"],
                    type="data",
                    intro_text=f"""The table below lists {tab['p'].lower()}{'' if tab['s'] != 'Court_Challenge' else ' for policies'} implemented to address the COVID-19 pandemic as downloaded from the COVID AMP website.""",
                    init_irow={
                        "logo": 0,
                        "title": 1,
                        "subtitle": 2,
                        "intro_text": 3,
                        "gap": 4,
                        "colgroups": 5,
                        "colnames": 6,
                        "data": 7,
                    },
                    data_getter=self.default_data_getter,
                    class_name=tab["s"],
                    # Does this workbook contain court challenges only?
                    challenges_only=tab["s"] == "Court_Challenge"
                    and len(tabs) == 1,
                ),
                CovidPolicyTab(
                    name="Legend - " + tab["p"],
                    type="legend",
                    intro_text=f"""A description for each data column in the "{tab['p']}" tab and its possible values is provided below.""",
                    init_irow={
                        "logo": 0,
                        "title": 1,
                        "subtitle": 2,
                        "intro_text": 3,
                        "gap": 4,
                        "colgroups": 5,
                        "colnames": 6,
                        "data": 7,
                    },
                    data_getter=self.default_data_getter_legend,
                    class_name=tab["s"],
                ),
            ]

    def add_content(self, workbook):
        """Add content, e.g., the tab containing the exported data.

        Parameters
        ----------
        workbook : type
            Description of parameter `workbook`.

        Returns
        -------
        type
            Description of returned object.

        """
        # track which sheets were skipped so their legends can be omitted
        skipped = set()
        for settings in self.sheet_settings:
            # Skip empty tabs
            if len(settings.data) == 0 or any(
                settings.name.endswith(name) for name in skipped
            ):
                skipped.add(settings.name)
                continue

            worksheet = workbook.add_worksheet(settings.name)

            # hide gridlines
            worksheet.hide_gridlines(2)

            # define formats
            settings.formats = WorkbookFormats(workbook)

            settings.write_header(
                worksheet,
                logo_fn="./api/assets/images/logo.png",
                logo_offset={
                    "x_offset": 5,
                    "y_offset": 25,
                },
                title=settings.name,
                intro_text=settings.intro_text,
            )

            data = settings.data
            settings.write_colgroups(worksheet, data)
            settings.write_colnames(worksheet, data)
            settings.write_rows(worksheet, data)

            if settings.type == "legend":
                settings.write_legend_labels(worksheet)
                worksheet.set_row(settings.init_irow["data"], 220)
            elif settings.type == "data":
                worksheet.freeze_panes(settings.init_irow["data"], 0)
                worksheet.autofilter(
                    settings.init_irow["colnames"],
                    0,
                    settings.init_irow["colnames"],
                    settings.num_cols - 1,
                )
            worksheet.set_column(0, 0, 25)

        return self

    def default_data_getter(self, tab, class_name: str = "Policy"):
        def get_joined_entity(main_entity, joined_entity_string):
            """Given a main entity class and a string of joined entities like
            'Entity2' or 'Entity2.Entity3', performs joins and returns the
            final entity listed, if it is available.

            Parameters
            ----------
            main_entity : type
                Description of parameter `main_entity`.
            joined_entity_string : type
                Description of parameter `joined_entity_string`.

            Returns
            -------
            type
                Description of returned object.

            """
            joined_entity_list = joined_entity_string.split(".")
            joined_entity = main_entity

            for d in joined_entity_list:
                joined_entity = getattr(joined_entity, d.lower())
            return joined_entity

        # get all metadata
        db = self.db
        metadata = select(
            i
            for i in db.Metadata
            if i.export == True
            # and i.ingest_field != ''
            and i.class_name == class_name
        ).order_by(db.Metadata.order)

        # get all policies (one policy per row exported)
        # TODO use generic var names
        policies = None
        if class_name == "Policy":
            policies = schema.get_policy(
                filters=self.filters, return_db_instances=True
            )
            policies = policies.order_by(db.Policy.date_start_effective)

        elif class_name == "Plan":
            policies = schema.get_plan(
                filters=self.filters, return_db_instances=True
            )
            policies = policies.order_by(db.Plan.date_issued)

        elif class_name == "Court_Challenge":
            if tab.challenges_only:
                policies = schema.get_challenge(
                    filters=self.filters, return_db_instances=True
                )
            else:
                policies_with_challenges = schema.get_policy(
                    filters=self.filters, return_db_instances=True
                )
                n_all_policies = db.Policy.select().count()
                filter_challenges = (
                    policies_with_challenges.count() != n_all_policies
                )
                if filter_challenges:
                    challenge_ids = set()
                    for d in policies_with_challenges:
                        if len(d.court_challenges) > 0:
                            for dd in d.court_challenges:
                                challenge_ids.add(dd.id)
                    policies = select(
                        i for i in db.Court_Challenge if i.id in challenge_ids
                    )
                else:
                    policies = schema.get_challenge(
                        filters=self.filters, return_db_instances=True
                    )
            policies = policies.order_by(db.Court_Challenge.date_of_complaint)

        # init export data list
        rows = list()

        def iterable(obj):
            try:
                iter(obj)
            except Exception:
                return False
            else:
                return True

        formatters = {
            "area1": lambda instance, value: value
            if instance.level != "Country"
            else "N/A",
            "area2": lambda instance, value: value
            if instance.level not in ("Country", "State / Province")
            and value != ""
            and value != "Unspecified"
            else "N/A",
        }

        # for each policy (i.e., row)
        for d in policies:

            # create dict to store row information
            row = defaultdict(dict)

            # for each metadatum (i.e., column in the spreadsheet)
            for dd in metadata:

                # if it's the PDF permalink column: handle specially
                # TODO reduce repeated code
                if dd.display_name == "Attachment for policy":
                    permalinks = list()
                    for file in d.file:
                        permalinks.append(
                            "https://api.covidamp.org/get/file/redirect?id="
                            + str(file.id)
                        )
                    row[dd.colgroup][
                        "Permalink for policy PDF(s)"
                    ] = "\n".join(permalinks)
                    continue
                elif dd.display_name == "Plan PDF":
                    permalinks = list()
                    for file in d.file:
                        permalinks.append(
                            "https://api.covidamp.org/get/file/redirect?id="
                            + str(file.id)
                        )
                    row[dd.colgroup]["Permalink for plan PDF(s)"] = "\n".join(
                        permalinks
                    )
                    continue
                elif dd.display_name == "Plan announcement PDF":
                    permalinks = list()
                    for file in d.file:
                        permalinks.append(
                            "https://api.covidamp.org/get/file/redirect?id="
                            + str(file.id)
                        )
                    row[dd.colgroup][
                        "Permalink for plan announcement PDF(s)"
                    ] = "\n".join(permalinks)
                    continue

                # check whether it is a policy or a joined entity
                join = (
                    dd.entity_name != "Policy"
                    and dd.entity_name != "Plan"
                    and dd.entity_name != "Court_Challenge"
                )

                # if it is not a join (data field entity is Policy)
                if not join:

                    # get value of data field
                    value = getattr(d, dd.field)

                    # format date values
                    # DATES #--------------------------------------------------#
                    # YYYY-MM-DD
                    if type(value) == date:
                        row[dd.colgroup][dd.display_name] = str(value)

                    # SETS / LISTS #-------------------------------------------#
                    # semicolon-delimited list of values
                    elif type(value) != str and iterable(value):
                        value_list = []
                        for v in value:
                            if type(v) == db.Policy:
                                value_list.append(
                                    v.policy_name + " (ID = " + str(v.id) + ")"
                                )
                            else:
                                value_list.append(str(v))
                        row[dd.colgroup][dd.display_name] = "; ".join(
                            value_list
                        )

                    # STRINGS AND NUMBERS #------------------------------------#
                    # run through formatters
                    else:
                        if dd.field in formatters:
                            row[dd.colgroup][dd.display_name] = formatters[
                                dd.field
                            ](d, value)
                        else:
                            row[dd.colgroup][dd.display_name] = value

                # otherwise, if the data field is on an entity other than Policy
                else:

                    # specially handle location fields
                    is_location_field = dd.field in ("area1", "area2", "iso3")

                    # get the joined entity
                    joined_entity = get_joined_entity(d, dd.entity_name)

                    if joined_entity is None:
                        row[dd.colgroup][dd.display_name] = ""
                        continue
                    else:

                        # check if the joined entity is a set or single
                        is_set = (
                            iterable(joined_entity)
                            and type(joined_entity) != str
                        )

                        # SET OF ENTITIES #------------------------------------#
                        # iterate over them and return a semicolon-delimited
                        # list of values, formatting if necessary
                        # TODO generalize to reuse above code
                        if is_set:
                            values = list()
                            if dd.field not in formatters:
                                values = "; ".join(
                                    set(
                                        [
                                            getattr(ddd, dd.field)
                                            for ddd in joined_entity
                                            if getattr(ddd, dd.field)
                                            is not None
                                        ]
                                    )
                                )
                            else:
                                func = formatters[dd.field]
                                values = "; ".join(
                                    set(
                                        [
                                            func(ddd, getattr(ddd, dd.field))
                                            for ddd in joined_entity
                                        ]
                                    )
                                )
                            row[dd.colgroup][dd.display_name] = values
                            continue
                        # SINGLE ENTITY #--------------------------------------#
                        # run through formatters
                        # TODO generalize to reuse above code
                        else:
                            value = getattr(joined_entity, dd.field)
                            if dd.field in formatters:
                                row[dd.colgroup][dd.display_name] = formatters[
                                    dd.field
                                ](joined_entity, value)
                            else:
                                row[dd.colgroup][dd.display_name] = value

            # append row data to overall row list
            rows.append(row)
        # return list of rows
        return rows

    def default_data_getter_legend(self, tab, class_name: str = "Policy"):
        # get all metadata
        db = self.db
        metadata = select(
            i
            for i in db.Metadata
            if i.export == True and i.class_name == class_name
        ).order_by(db.Metadata.order)

        # init export data list
        rows = list()

        # for each metadatum
        for row_type in ("definition", "possible_values"):

            # append rows containing the field's definition and possible values
            row = defaultdict(dict)
            for d in metadata:
                if d.display_name == "Attachment for policy":
                    if row_type == "definition":
                        row[d.colgroup][
                            "Permalink for policy PDF(s)"
                        ] = "URL of permanently hosted PDF document(s) for the policy"
                    elif row_type == "possible_values":
                        row[d.colgroup][
                            "Permalink for policy PDF(s)"
                        ] = "Any URL(s)"
                else:
                    row[d.colgroup][d.display_name] = getattr(d, row_type)
            rows.append(row)
        return rows
