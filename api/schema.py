# 3rd party modules
from pony.orm import db_session, select

# local modules
from ingest import CovidPolicyPlugin
from .models import Policy, PolicyList, Auth_Entity
from db import db


@db_session
def get_policy():
    q = select(i for i in db.Policy)
    instance_list = []
    for d in q:
        d_dict = d.to_dict()
        print(d_dict)
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


def ingest_covid_npi_policy():
    plugin = CovidPolicyPlugin()
    plugin.load_client().load_data().process_data(db)
    return []


def test():
    ingest_covid_npi_policy()


# test()
