"""Ingest utility methods"""
# standard packages
import urllib3
import certifi

# 3rd party modules
from pony.orm import db_session, commit, get, select
from pony.orm.core import EntityMeta
import pprint

# constants
pp = pprint.PrettyPrinter(indent=4)


@db_session
def upsert(cls, get, set=None, skip=[]):
    """
    Interacting with Pony entities.
    https://github.com/ponyorm/pony/issues/131#issuecomment-343869846

    :param cls: The actual entity class
    :param get: Identify the object (e.g. row) with this dictionary
    :param set:
    :return:
    """
    # does the object exist
    assert isinstance(
        cls, EntityMeta), "{cls} is not a database entity".format(cls=cls)

    # if no set dictionary has been specified
    # TODO DELETE
    set = set or {}

    if not cls.exists(**get):
        # make new object
        print("\nCreated")
        return cls(**set, **get)
    else:
        # get the existing object
        obj = cls.get(**get)
        for key, value in set.items():
            if key in skip:
                continue
            true_update = str(value).strip() != str(getattr(obj, key)).strip() \
                and value != getattr(obj, key)
            if true_update:
                print('Updated: value was ' + str(key) +
                      ' = ' + str(getattr(obj, key)))
                print('--changed to ' + str(key) + ' = ' + str(value))
            obj.__setattr__(key, value)
        commit()
        return obj


def download_pdf(download_url, fn, write_path, as_object=True):
    http = urllib3.PoolManager(
        cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
    user_agent = 'Mozilla/5.0'
    try:
        response = http.request('GET', download_url, headers={
                                'User-Agent': user_agent})
        if response is not None and response.data is not None:
            if as_object:
                return response.data
            else:
                with open(write_path + fn, 'wb') as out:
                    out.write(response.data)
                return True
    except Exception as e:
        return None
    else:
        print('Error when downloading PDF (404)')
        return False
