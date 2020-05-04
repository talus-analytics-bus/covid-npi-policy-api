"""Define API data processing methods"""
# 3rd party modules
from pony.orm import db_session, select

# local modules
from ingest import CovidPolicyPlugin
from .models import Policy, PolicyList, Auth_Entity, Place
from db import db


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
