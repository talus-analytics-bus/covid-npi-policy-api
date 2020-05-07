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

# local modules
from .formats import WorkbookFormats
from .export import ExcelExport, SheetSettings
from api import schema


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

    def __init__(self, db):
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
        self.sheet_settings = [
            SheetSettings(
                name='Exported data',
                type='data',
                intro_text='The table below lists policies implemented to address the COVID-19 pandemic as downloaded from the COVID Policy Tracker website.',
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
                logo_fn='./api/assets/images/logo-talus.png',
                title=settings.name,
                intro_text=settings.intro_text)

            data = settings.data
            settings.write_colgroups(worksheet, data)
            settings.write_colnames(worksheet, data)
            settings.write_rows(worksheet, data)

            if settings.type == 'legend':
                settings.write_legend_labels(worksheet)
            elif settings.type == 'data':
                worksheet.freeze_panes(settings.init_irow['colnames'], 0)
                worksheet.autofilter(
                    settings.init_irow['colnames'],
                    0,
                    settings.init_irow['colnames'],
                    settings.num_cols - 1
                )

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
            for entity_name in joined_entity_list:
                joined_entity = getattr(joined_entity, entity_name.lower())
                if joined_entity is None:
                    return None
                else:
                    continue
            return joined_entity

        # get all metadata
        db = self.db
        metadata = select(
            i for i in db.Metadata
        )

        # get all policies (one policy per row exported)
        policies = schema.get_policy().data

        # init export data list
        rows = list()

        # for each policy
        for d in policies:
            row = defaultdict(dict)
            for dd in metadata:
                if dd.entity == 'Policy':
                    value = getattr(d, dd.id)

                    # format date values properly
                    if type(value) == date:
                        row[dd.colgroup][dd.display_name] = str(value)
                    else:
                        row[dd.colgroup][dd.display_name] = value
                else:
                    join = get_joined_entity(d, dd.entity)
                    if join is None:
                        row[dd.colgroup][dd.display_name] = ''
                    elif type(join) == list:
                        values = "; ".join([getattr(v, dd.id) for v in join])
                        row[dd.colgroup][dd.display_name] = values
                    else:
                        value = getattr(join, dd.id)
                        row[dd.colgroup][dd.display_name] = value
            rows.append(row)
        return rows

        # Test data
        # Name, email
        # Hobbies, Favorite color
        # Dict of column groups, which are a list of columns, which are
        # dictionaries with key = colname, value = data to show
        return [
            {
                'Basic information':
                {
                    'Name': 'Mike',
                    'E-mail': 'mvanmaele@talusanalytics.com',
                },
                'Additional details':
                {
                    'Hobbies': 'Opera; Triathlon; Yoga',
                    'Favorite color': 'Blue',
                }
            },
            {
                'Basic information':
                {
                    'Name': 'Mike2',
                    'E-mail': 'mvanmaele@talusanalytics.com2',
                },
                'Additional details':
                {
                    'Hobbies': 'Opera; Triathlon; Yoga2',
                    'Favorite color': 'Blue2',
                }
            },
        ]

    def default_data_getter_legend(self):
        # Test data
        # Name, email
        # Hobbies, Favorite color
        # Dict of column groups, which are a list of columns, which are
        # dictionaries with key = colname, value = data to show
        return [
            {
                'Basic information':
                {
                    'Name': 'The name',
                    'E-mail': 'The e-mail',
                },
                'Additional details':
                {
                    'Hobbies': 'Any semicolon-delimited list of hobbies',
                    'Favorite color': 'Any color',
                }
            },
            {
                'Basic information':
                {
                    'Name': 'Any text',
                    'E-mail': 'Any email',
                },
                'Additional details':
                {
                    'Hobbies': 'Any semicolon-delimited list of text',
                    'Favorite color': 'Any text',
                }
            },
        ]
