"""Ingest utility methods"""
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
                print(len(getattr(obj, key)))
                print('--changed to ' + str(key) + ' = ' + str(value))
                print(len(value))
            obj.__setattr__(key, value)
        commit()
        return obj
