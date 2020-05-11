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
from .export import ExcelExport, SheetSettings
from api import schema

# constants
pp = pprint.PrettyPrinter(indent=4)


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

    def __init__(self, db, filters):
        self.db = db
        self.data = None
        self.init_irow = {
            'logo': 0,
            'title': 1,
            'subtitle': 2,
            'intro_text': 3,
            'gap': 4,
            'colgroups': 5,
            'colnames': 6,
            'data': 7
        }
        self.filters = filters
        self.sheet_settings = [
            SheetSettings(
                name='Exported data',
                type='data',
                intro_text='The table below lists policies implemented to address the COVID-19 pandemic as downloaded from the COVID MAPS website.',
                init_irow={
                    'logo': 0,
                    'title': 1,
                    'subtitle': 2,
                    'intro_text': 3,
                    'gap': 4,
                    'colgroups': 5,
                    'colnames': 6,
                    'data': 7
                },
                data_getter=self.default_data_getter
            ),
            SheetSettings(
                name='Legend',
                type='legend',
                intro_text='A description for each data column in the "Exported data" tab and its possible values is provided below.',
                init_irow={
                    'logo': 0,
                    'title': 1,
                    'subtitle': 2,
                    'intro_text': 3,
                    'gap': 4,
                    'colgroups': 5,
                    'colnames': 6,
                    'data': 7
                },
                data_getter=self.default_data_getter_legend
            )
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
        for settings in self.sheet_settings:
            worksheet = workbook.add_worksheet(settings.name)

            # hide gridlines
            worksheet.hide_gridlines(2)

            # define formats
            settings.formats = WorkbookFormats(workbook)

            settings.write_header(
                worksheet,
                logo_fn='./api/assets/images/logo.png',
                logo_offset={
                    'x_offset': 5,
                    'y_offset': 25,
                },
                title=settings.name,
                intro_text=settings.intro_text)

            data = settings.data
            settings.write_colgroups(worksheet, data)
            settings.write_colnames(worksheet, data)
            settings.write_rows(worksheet, data)

            if settings.type == 'legend':
                settings.write_legend_labels(worksheet)
                worksheet.set_row(settings.init_irow['data'], 220)
            elif settings.type == 'data':
                worksheet.freeze_panes(settings.init_irow['data'], 0)
                worksheet.autofilter(
                    settings.init_irow['colnames'],
                    0,
                    settings.init_irow['colnames'],
                    settings.num_cols - 1
                )
            worksheet.set_column(0, 0, 25)

        return self

    def default_data_getter(self):

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
            joined_entity_list = joined_entity_string.split('.')
            joined_entity = main_entity

            for d in joined_entity_list:
                joined_entity = getattr(joined_entity, d.lower())
            return joined_entity

        # get all metadata
        db = self.db
        metadata = select(
            i for i in db.Metadata
            if i.export == True
        )

        # get all policies (one policy per row exported)
        policies = schema.get_policy(
            filters=self.filters, return_db_instances=True
        )

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
            'area1': lambda instance, value: value if instance.level != 'Country' else 'N/A',
            'area2': lambda instance, value: value if instance.level == 'Local area' else 'N/A',
        }

        # for each policy
        # TODO clean up
        for d in policies:
            row = defaultdict(dict)
            for dd in metadata:
                if dd.entity == 'Policy':
                    value = getattr(d, dd.field)

                    # format date values properly
                    if type(value) == date:
                        row[dd.colgroup][dd.display_name] = str(value)
                    elif type(value) != str and iterable(value):
                        value_list = []
                        for v in value:
                            if type(v) == db.Policy:
                                value_list.append(str(v.id))
                            else:
                                value_list.append(str(v))
                        row[dd.colgroup][dd.display_name] = \
                            "; ".join(value_list)
                    else:
                        if dd.field in formatters:
                            row[dd.colgroup][dd.display_name] = formatters[dd.field](
                                d, value)
                        else:
                            row[dd.colgroup][dd.display_name] = value
                else:
                    join = get_joined_entity(d, dd.entity)

                    if join is None:
                        row[dd.colgroup][dd.display_name] = ''
                        continue
                    else:
                        if iterable(join):
                            values = list()
                            if dd.field not in formatters:
                                values = "; ".join(
                                    [getattr(ddd, dd.field) for ddd in join
                                        if getattr(ddd, dd.field) is not None]
                                )
                            else:
                                func = formatters[dd.field]
                                values = "; ".join(
                                    [func(ddd, getattr(ddd, dd.field))
                                     for ddd in join]
                                )

                            row[dd.colgroup][dd.display_name] = values
                            continue

                    if type(join) == list:
                        values = "; ".join([getattr(v, dd.field)
                                            for v in join])
                        row[dd.colgroup][dd.display_name] = values
                    else:
                        value = getattr(join, dd.field)
                        if dd.field in formatters:
                            row[dd.colgroup][dd.display_name] = formatters[dd.field](
                                join, value)
                        else:
                            row[dd.colgroup][dd.display_name] = value
            rows.append(row)
        return rows

    def default_data_getter_legend(self):
        # get all metadata
        db = self.db
        metadata = select(
            i for i in db.Metadata
            if i.export == True
        )

        # init export data list
        rows = list()

        # for each metadatum
        for row_type in ('definition', 'possible_values'):
            row = defaultdict(dict)
            for d in metadata:
                row[d.colgroup][d.display_name] = getattr(d, row_type)
            rows.append(row)
        return rows
