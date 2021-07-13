"""Project-specific plugins for export module"""
# standard modules
import datetime
from collections import defaultdict

from typing import Any, DefaultDict, Dict, List, Set

# 3rd party modules
import pprint
from alive_progress import alive_bar
from pony.orm import select
from pony.orm.core import (
    Query,
    db_session,
)

# local modules
from . import policyexport, planexport
from .formats import WorkbookFormats
from .export import ExcelExport, WorkbookTab
from db.models import Metadata
from api.util import date_to_str, is_listlike

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
            preposition: str = (
                "" if tab["s"] != "Court_Challenge" else " for policies"
            )
            self.sheet_settings += [
                CovidPolicyTab(
                    name=tab["p"],
                    type="data",
                    intro_text=f"""The table below lists {tab['p'].lower()}"""
                    f"""{preposition} implemented to address the COVID-19"""
                    " pandemic as downloaded from the COVID AMP website.",
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
                    intro_text="A description for each data column in the"
                    f""" "{tab['p']}" tab and its possible values is """
                    "provided below.",
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

    @db_session
    def default_data_getter(self, tab, class_name: str = "Policy"):
        # get all metadata
        db = self.db
        metadata = select(
            i
            for i in db.Metadata
            if i.export == True and i.class_name == class_name  # noqa: E712
        ).order_by(db.Metadata.order)[:][:]

        # get all instances (one instance per row exported)
        custom_fields: Set[str] = None
        export_fields: List[str] = None
        instances: Query = None
        if class_name == "Policy":
            (
                instances,
                export_fields,
                custom_fields,
            ) = policyexport.get_export_data(self.filters)

        elif class_name == "Plan":
            (
                instances,
                export_fields,
                custom_fields,
            ) = planexport.get_export_data(self.filters)

        else:
            raise NotImplementedError(
                "Unexpected class name, should be Policy or Plan: "
                + class_name
            )

        # init export data list
        rows = list()

        # get metadata by field name
        metadata_by_field: DefaultDict[Dict[str, Metadata]] = defaultdict(dict)
        m: Metadata = None
        for m in metadata:
            metadata_by_field[m.entity_name][m.field] = m

        # for each policy (i.e., row)
        with alive_bar(
            instances.count(), title="Processing instances for Excel"
        ) as bar:
            raw_vals: tuple = None
            for raw_vals in instances:
                bar()

                # create dict to store row information
                row = defaultdict(dict)
                cell_vals_by_field: Dict[str, Any] = dict()

                # add values to row
                idx: int = -1
                table_and_field: str = None
                for table_and_field in export_fields:
                    idx += 1
                    field_arr: List[str] = table_and_field.split(".")
                    field: str = field_arr[-1]
                    table_name: str = ".".join(field_arr[0:-1])
                    raw_val: Any = raw_vals[idx]
                    cell_val: str = raw_val
                    raw_val_type: Any = type(raw_val)
                    level_field_name: str = table_name + ".level"

                    # handle custom fields specially
                    if table_and_field in custom_fields:
                        meta: Metadata = metadata_by_field[table_name][field]
                        self.__set_multiline_cell_val(
                            row,
                            cell_vals_by_field,
                            table_and_field,
                            raw_val,
                            raw_val_type,
                            meta,
                        )
                        continue

                    # set to N/A if field is for geo. area that doesn't apply
                    elif (
                        field == "area1"
                        and cell_vals_by_field.get(level_field_name)
                        in ("Country", "Tribal nation")
                        and cell_val in ("Unspecified", None, "")
                    ):
                        cell_val = "N/A"
                    elif field == "area2" and (
                        cell_vals_by_field.get(level_field_name)
                        in ("Country", "State / Province", "Tribal nation")
                        and cell_val in ("Unspecified", None, "")
                    ):
                        cell_val = "N/A"
                    elif (
                        field == "iso3"
                        and cell_vals_by_field.get(level_field_name)
                        == "Tribal nation"
                    ):
                        cell_val = "N/A"

                    # if no val or blank string, make "Unspecified"
                    elif raw_val is None:
                        cell_val = "Unspecified"
                    elif raw_val_type == str:
                        if raw_val.strip() == "":
                            cell_val = "Unspecified"
                        elif "; " in raw_val:
                            cell_val = "; ".join(
                                [v for v in raw_val.split("; ") if v != ""]
                            )

                    # if date, convert to YYYY-MM-DD
                    elif raw_val_type == datetime.date:
                        cell_val = date_to_str(raw_val)

                    if is_listlike(cell_val):
                        cell_val = "; ".join([v for v in cell_val if v != ""])

                    meta: Metadata = metadata_by_field[table_name][field]
                    row[meta.colgroup][meta.display_name] = cell_val
                    cell_vals_by_field[table_and_field] = cell_val

                # append row data to overall row list
                rows.append(row)
        # return list of rows
        return rows

    def __set_multiline_cell_val(
        self,
        row: Dict[str, Any],
        cell_vals_by_field: Dict[str, str],
        table_and_field: str,
        raw_val: Any,
        raw_val_type: Any,
        meta: Metadata,
    ) -> None:
        """Sets a cell val as the formatted value if it is a single value, or
        as lines of values if multiple values.

        Args:
            row (Dict[str, Any]): The dict containing Excel row final values,
            with keys as columns and values as cell content.

            cell_vals_by_field (Dict[str, str]): The final cell values for each
            column in the row indexed by their table and field name.

            table_and_field (str): The table and field name for the data col.

            raw_val (Any): The unformatted value to be processed for writing
            to the Excel

            raw_val_type (Any): The type of the unformatted value

            meta (Metadata): The metadata describing the data col.
        """
        # show unspec if None
        cell_val: str = ""
        if raw_val is None:
            cell_val = "Unspecified"

        # if raw is string, format as list of values or single value
        elif raw_val_type == str:
            cell_val = ";\n ".join(list(set(raw_val.split("; "))))

        # set final value for Excel writing
        row[meta.colgroup][meta.display_name] = cell_val
        cell_vals_by_field[table_and_field] = cell_val

    def default_data_getter_legend(self, tab, class_name: str = "Policy"):
        # get all metadata
        db = self.db
        metadata = select(
            i
            for i in db.Metadata
            if i.export == True and i.class_name == class_name  # noqa: E712
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
                        row[d.colgroup]["Attachment for policy"] = (
                            "URL of permanently hosted PDF document(s) for "
                            "the policy"
                        )
                    elif row_type == "possible_values":
                        row[d.colgroup]["Attachment for policy"] = "Any URL(s)"
                else:
                    row[d.colgroup][d.display_name] = getattr(d, row_type)
            rows.append(row)
        return rows
