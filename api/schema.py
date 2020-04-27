"""Define API data processing methods"""
# 3rd party modules
from pony.orm import db_session, select

# local modules
from ingest import CovidPolicyPlugin
from .models import Policy, PolicyList, Auth_Entity
from db import db


@db_session
def get_policy(filters=None):
    q = select(i for i in db.Policy)
    if filters is not None:
        print('Filters:')
        print(filters)
        q = apply_filters(q, filters)
    # else:
    #     print('Applying DEBUG filters')
    #     q = apply_filters(q,
    #                       {
    #                           'primary_ph_measure': [
    #                               'Support for public health and clinical capacity'
    #                           ],
    #                           'ph_measure_details': [
    #                               'Crisis standards of care'
    #                           ]
    #                       }
    #                       )
    instance_list = []
    for d in q:
        d_dict = d.to_dict()
        if 'auth_entity' in d_dict:
            auth_entity_instance = db.Auth_Entity[d_dict['auth_entity']]
            desc = get_auth_entity_desc(auth_entity_instance)
            d_dict['auth_entity'] = \
                Auth_Entity(**auth_entity_instance.to_dict(), desc=desc)
        instance_list.append(
            Policy(**d_dict)
        )
    res = PolicyList(
        data=instance_list,
        success=True,
        message=f'''{len(q)} policies found'''
    )
    return res


def get_auth_entity_desc(i):
    main_desc = f'''{i.area1}, {i.iso3}: {i.office}'''
    if i.area2 != 'Unspecified':
        return i.area2 + ', ' + main_desc
    else:
        return main_desc


@db_session
def get_optionset(fields=list(), entity_name=None):
    """Given a list of data fields and an entity name, returns the possible
    values for those fields based on what data are currently in the database.

    TODO add support for getting possible fields even if they haven't been
    used yet in the data

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
    if entity_name is None:
        return {
            'success': False,
            'message': 'Must provide value for parameter `entity_name`',
            'data': {}
        }
    try:
        entity_class = getattr(db, entity_name)
    except AttributeError as e:
        return {
            'success': False,
            'message':
                f'''Database entity with `entity_name` {entity_name} not found''',
            'data': {}
        }
    data = dict()
    for d in fields:
        options = select(getattr(i, d)
                         for i in entity_class)[:][:]
        options.sort()
        data[d] = options
    return {
        'success': True,
        'message': f'''Optionset values retrieved for entity `{entity_name}`''',
        'data': data
    }


def ingest_covid_npi_policy():
    plugin = CovidPolicyPlugin()
    plugin.load_client().load_data().process_data(db)
    return []


def test():
    ingest_covid_npi_policy()


def apply_filters(q, filters):
    for field, allowed_values in filters.items():
        print('field')
        print(field)
        print('allowed_values')
        print(allowed_values)
        q = select(
            i
            for i in q
            if getattr(i, field) in allowed_values
        )
    return q
