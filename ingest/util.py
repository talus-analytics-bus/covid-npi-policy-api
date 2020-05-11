"""Ingest utility methods"""
# 3rd party modules
from pony.orm import db_session, commit, get, select
from pony.orm.core import EntityMeta
import pprint

# constants
pp = pprint.PrettyPrinter(indent=4)


@db_session
def upsert(cls, get, set=None):
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
        print("Created")
        return cls(**set, **get)
    else:
        # get the existing object
        print('Existed')
        obj = cls.get(**get)
        for key, value in set.items():
            if getattr(obj, key) != value:
                print('Updated: ' + str(key) + ' = ' + str(value))
            obj.__setattr__(key, value)
        commit()
        return obj
