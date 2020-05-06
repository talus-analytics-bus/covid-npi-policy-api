"""Define API data processing methods"""
# standard modules
from io import BytesIO
from datetime import datetime, date

# 3rd party modules
from pony.orm import db_session, select, get
from openpyxl import load_workbook
from openpyxl.worksheet.table import Table, TableStyleInfo
import pandas as pd
import pandas.io.formats.excel
from fastapi.responses import FileResponse, Response

# local modules
from ingest import CovidPolicyPlugin
from .models import Policy, PolicyList, Auth_Entity, Place, Doc
from db import db


# settings
pandas.io.formats.excel.header_style = None


@db_session
def export():
    # Create bytes output to return to client
    io = BytesIO()
    writer = pd.ExcelWriter('temp.xlsx', engine='xlsxwriter')
    writer.book.filename = io
    workbook = writer.book

    def create_color_header_fmt(color_hex):
        return workbook.add_format(
            {
                # Specific header styling:
                'bg_color': color_hex,
                'font_color': '#ffffff',

                # All header styling:
                'bold': True,
                'font_size': 18,
                'font_name': 'Calibri (Body)',
                'valign': 'vcenter',
                'border': 2,
                'border_color': '#CCCDCB',
            }
        )

    # define formats
    # TODO in separate module
    # Style header
    sheet_title_fmt = workbook.add_format(
        {
            'bold': True,
            'font_size': 26
        }
    )

    # Style subtitle of sheet
    sheet_subtitle_fmt = workbook.add_format(
        {
            'italic': True,
            'font_size': 18,
            'font_name': 'Calibri (Body)',
        }
    )

    # Exported data ############################################################

    # Style header
    sheet_title_fmt = workbook.add_format(
        {
            'bold': True,
            'font_size': 26
        }
    )

    # Style subtitle of sheet
    sheet_subtitle_fmt = workbook.add_format(
        {
            'italic': True,
            'font_size': 18,
            'font_name': 'Calibri (Body)',
        }
    )

    # Style columns
    section_header_fmt = workbook.add_format(
        {
            'bold': True,
            'font_size': 22,
            'bg_color': '#DEDEDE',
            'font_name': 'Calibri (Body)',
            'valign': 'vcenter',
            'align': 'center',
            'border': 2,
            'border_color': '#CCCDCB',
        }
    )

    # Style cells
    cell_fmt = workbook.add_format(
        {
            'font_size': 14,
            'bg_color': '#ffffff',
            'font_name': 'Calibri (Body)',
            'valign': 'vcenter',
            'border': 2,
            'border_color': '#CCCDCB',
            'text_wrap': True
        }
    )

    cell_num_fmt = workbook.add_format(
        {
            'font_size': 14,
            'bg_color': '#ffffff',
            'font_name': 'Calibri (Body)',
            'valign': 'vcenter',
            'border': 2,
            'border_color': '#CCCDCB',
            'text_wrap': True,
            'num_format': '#,##0.00',
        }
    )
    def_cell_fmt = workbook.add_format(
        {
            'bg_color': '#DBDBDB',
            'bold': True,
            'font_size': 18,
            'font_name': 'Calibri (Body)',
            'border': 2,
            'border_color': '#CCCDCB',
            'valign': 'top',
        }
    )

    def create_color_header_fmt(color_hex):
        return workbook.add_format(
            {
                # Specific header styling:
                'bg_color': color_hex,
                'font_color': '#ffffff',

                # All header styling:
                'bold': True,
                'font_size': 18,
                'font_name': 'Calibri (Body)',
                'valign': 'vcenter',
                'border': 2,
                'border_color': '#CCCDCB',
            }
        )

    blue_header_fmt = create_color_header_fmt('#4682B4')
    green_header_fmt = create_color_header_fmt('#336326')
    purple_header_fmt = create_color_header_fmt('#3D003B')
    dblue_header_fmt = create_color_header_fmt('#203764')

    project_cols = [
        'project_name',
        'description',
        'data_sources',
        'core_capacities'
    ]
    funder_cols = [
        'source',
        'source_type',
    ]
    recipient_cols = [
        'target',
        'target_type',
    ]
    trans_cols = [
        'committed_funds',
        'disbursed_funds',
        'year_range',
        'currency_iso',
        'assistance_type',
    ]

    dfData

    # Style and prepare the Excel workbook
    # Sheet title
    worksheet = workbook.add_worksheet("Exported data")
    worksheet.hide_gridlines(2)
    section_name_row = 4
    col_name_row = section_name_row + 1
    worksheet.freeze_panes(col_name_row + 1, 1)
    worksheet.autofilter(col_name_row, 0, col_name_row, len(cols) - 1)

    # Add logo
    worksheet.set_row(0, 85)
    worksheet.insert_image(
        0, 0, './api/assets/images/logo.png',
        {
            'y_scale': 1.1,
        }
    )

    # Sheet title
    worksheet.write(1, 0, 'Exported data', sheet_title_fmt)

    # "Downloaded on..."
    today = date.today()
    worksheet.write(2, 0, 'Downloaded on ' + str(today), sheet_subtitle_fmt)

    # Column sections
    i = section_name_row
    j = 0
    col_slugs = [d[0] for d in cols]
    sections = {
        'Project information': {
            'n': len([d for d in col_slugs if d in project_cols])
        },
        'Funder information': {
            'n': len([d for d in col_slugs if d in funder_cols])
        },
        'Recipient information': {
            'n': len([d for d in col_slugs if d in recipient_cols])
        },
        'Transaction information': {
            'n': len([d for d in col_slugs if d in trans_cols])
        },
    }

    def insert_entity_role_icon(entity_role):
        x_offset_base = 35 if entity_role == 'source' else 18
        worksheet.insert_image(
            i, start, './api/assets/images/' + entity_role + '.png',
            {
                'x_scale': 1.6 * image_scale,
                'y_scale': .9 * image_scale,
                'x_offset': x_offset_base + n * 50 * 2
            }
        )

    image_scale = .7
    for section_name in sections:
        n = sections[section_name]['n']
        if n > 0:
            start = j
            end = j + n - 1
            if start != end:
                worksheet.merge_range(
                    i, start, i, end, section_name, section_header_fmt
                )
            else:
                worksheet.write(
                    i, end, section_name, section_header_fmt
                )
            # if section_name == 'Funder information':
            #     insert_entity_role_icon('source')
            # elif section_name == 'Recipient information':
            #     insert_entity_role_icon('target')

        j = end + 1

    # Style header row
    worksheet.set_row(col_name_row, 41)

    # Style columns (widths mainly)
    worksheet.set_column(0, len(cols), 50)
    for j in idx_wide:
        worksheet.set_column(j, j, 100)

    # Write headers
    i = col_name_row
    j = 0
    for d in cols:
        if d[0] in project_cols:
            worksheet.write(i, j, d[1], blue_header_fmt)
        elif d[0] in funder_cols:
            worksheet.write(i, j, d[1], green_header_fmt)
        elif d[0] in recipient_cols:
            worksheet.write(i, j, d[1], purple_header_fmt)
        elif d[0] in trans_cols:
            worksheet.write(i, j, d[1], dblue_header_fmt)
        j += 1

    # Write data, using number format for money cols.
    i = col_name_row + 1
    j = 0
    for d in dfData:
        for cell_data in d:
            if j in idx_money:
                worksheet.write(i, j, cell_data, cell_num_fmt)
            else:
                worksheet.write(i, j, cell_data, cell_fmt)
            j += 1
        worksheet.set_row(i, 260)
        i += 1
        j = 0

    ############################################################################

    # close writer
    writer.close()

    # return to start of IO stream
    io.seek(0)

    # return export file
    path = io.read()
    return Response(content=path, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


@db_session
def get_doc(id: int):
    doc = get(i for i in db.Doc if i.id == id)
    fn = f'''api/pdf/{doc.pdf}.pdf'''
    return FileResponse(fn)


@db_session
def get_policy(filters=None):
    q = select(i for i in db.Policy)
    if filters is not None:
        q = apply_filters(q, filters)

    instance_list = []
    for d in q:
        d_dict = d.to_dict()
        if 'auth_entity' in d_dict:
            instance = db.Auth_Entity[d_dict['auth_entity']]
            d_dict['auth_entity'] = \
                Auth_Entity(
                    **instance.to_dict())
        if 'place' in d_dict:
            instance = db.Place[d_dict['place']]
            d_dict['place'] = \
                Place(
                    **instance.to_dict())
        if d.doc is not None:
            instances = d.doc
            d_dict['policy_docs'] = list()
            for instance in instances:
                instance_dict = instance.to_dict()
                instance_dict['pdf'] = None if instance_dict['pdf'] == '' \
                    else f'''/get/doc?id={instance.id}'''
                d_dict['policy_docs'].append(
                    Doc(**instance_dict)
                )
        instance_list.append(
            Policy(**d_dict)
        )
    res = PolicyList(
        data=instance_list,
        success=True,
        message=f'''{len(q)} policies found'''
    )
    return res


def get_auth_entity_loc(i):
    if i.area2.lower() not in ('unspecified', 'n/a'):
        return f'''{i.area2}, {i.area1}, {i.iso3}'''
    elif i.area1.lower() not in ('unspecified', 'n/a'):
        return f'''{i.area1}, {i.iso3}'''
    else:
        return i.iso3


@db_session
def get_optionset(fields=list()):
    """Given a list of data fields and an entity name, returns the possible
    values for those fields based on what data are currently in the database.

    TODO add support for getting possible fields even if they haven't been
    used yet in the data

    TODO list unspecified last

    Parameters
    ----------
    fields : list
        List of strings of data fields names.
    entity_name : str
        The name of the entity for which to check possible values.

    Returns
    -------
    api.models.OptionSetList
        List of possible optionset values for each field.

    """
    data = dict()
    for d_str in fields:
        d_arr = d_str.split('.')
        entity_class = getattr(db, d_arr[0])
        field = d_arr[1]
        options = select(getattr(i, field)
                         for i in entity_class)[:][:]
        options.sort()
        options.sort(key=lambda x: x == 'Unspecified')
        id = 0
        data[field] = []
        for dd in options:
            data[field].append(
                {
                    'id': id,
                    'value': dd,
                    'label': dd
                }
            )
            id = id + 1
    return {
        'success': True,
        'message': f'''Optionset values retrieved''',
        'data': data
    }


def ingest_covid_npi_policy():
    plugin = CovidPolicyPlugin()
    plugin.load_client().load_data().process_data(db)
    return []


def test():
    ingest_covid_npi_policy()


def apply_filters(q, filters):
    """Given the PonyORM query and filters, applies filters with AND logic.

    TODO ensure this works for arbitrary large numbers of filtered fields.

    Parameters
    ----------
    q : pony.orm.Query
        A Query instance, e.g., created by a call to `select`.
    filters : dict[str, list]
        Dictionary with keys of field names and values of lists of
        allowed values (AND logic).

    Returns
    -------
    pony.orm.Query
        The query with filters applied.

    """
    for field, allowed_values in filters.items():
        if len(allowed_values) == 0:
            continue
        if field.startswith('date'):
            def str_to_date(s):
                return datetime.strptime(s, '%Y-%m-%d').date()
            allowed_values = list(
                map(str_to_date, allowed_values)
            )
        join = field in ('level', 'loc', 'area1')
        if not join:
            q = select(
                i
                for i in q
                if getattr(i, field) in allowed_values
            )
        else:
            q = select(
                i
                for i in q
                if getattr(i.place, field) in allowed_values
            )
    return q
