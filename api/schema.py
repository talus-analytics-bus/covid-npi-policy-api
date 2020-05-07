"""Define API data processing methods"""
# standard modules
from io import BytesIO
from datetime import datetime, date

# 3rd party modules
from pony.orm import db_session, select, get
from fastapi.responses import FileResponse, Response

# local modules
from ingest import CovidPolicyPlugin
from .export import CovidPolicyExportPlugin
from .models import Policy, PolicyList, Auth_Entity, Place, Doc
from db import db


@db_session
def export():
    # Create Excel export file
    genericExcelExport = CovidPolicyExportPlugin(db)
    content = genericExcelExport.build()

    return Response(content=content, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


@db_session
def get_doc(id: int):
    doc = get(i for i in db.Doc if i.id == id)
    fn = f'''api/pdf/{doc.pdf}.pdf'''
    return FileResponse(fn)


@db_session
def get_policy(filters=None, return_db_instances=False):
    q = select(i for i in db.Policy)
    if filters is not None:
        q = apply_filters(q, filters)

    if return_db_instances:
        return q
    else:
        instance_list = []
        for d in q:
            d_dict = d.to_dict()
            auth_entity_list = []

            for dd in d.auth_entity:
                dd_dict = dd.to_dict()
                place_dict = Place(**dd.place.to_dict())
                dd_dict['place'] = place_dict
                auth_entity_list.append(Auth_Entity(**dd_dict))

            d_dict['auth_entity'] = auth_entity_list
            place_instance = Place(**d.place.to_dict())
            d_dict['place'] = place_instance
            # if 'place' in d_dict:
            #     instance = db.Place[d_dict['place']]
            #     d_dict['place'] = \
            #         Place(
            #             **instance.to_dict())
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
