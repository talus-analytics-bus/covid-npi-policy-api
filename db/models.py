"""Define database models."""
# standard modules
import datetime
from datetime import date

# 3rd party modules
from pony.orm import PrimaryKey, Required, Optional, Optional, Set, StrArray, select, db_session
# from enum import Enum
# from pony.orm.dbapiprovider import StrConverter

# local modules
from .config import db

# # Define enum type support
# class State(Enum):
#     mv = 'mv'
#     jk = 'jk'
#     ac = 'ac'
#
#
# # Adapted from:
# # https://stackoverflow.com/questions/31395663/how-can-i-store-a-python-enum-using-pony-orm
# class EnumConverter(StrConverter):
#     def validate(self, val):
#         if not isinstance(val, Enum):
#             raise ValueError('Must be an Enum.  Got {}'.format(type(val)))
#         return val
#
#     def py2sql(self, val):
#         return val.name
#
#     def sql2py(self, value):
#         return self.py_type[value].name
# db.provider.converter_classes.append((Enum, EnumConverter))


@db_session
def custom_delete(entity_class, records):
    """A custom delete method which deletes any records from the database that
    are not in the provided record set.

    Parameters
    ----------
    entity_class : PonyORM database entity class
        The PonyORM database entity class from which to delete instances.
    records : set
        Set of instances of `entity_class` which should be in the database.

    Returns
    -------
    int
        The number of records deleted from the database.

    """
    to_delete = select(
        i for i in entity_class
        if i not in records
    )
    to_delete.delete()
    return len(to_delete)


class Version(db.Entity):
    _table_ = "version"
    id = PrimaryKey(int, auto=True)
    name = Optional(str, nullable=True)
    date = Required(date)
    last_datum_date = Optional(datetime.date)
    type = Required(str)


class Metadata(db.Entity):
    """Display names, definitions, etc. for fields."""
    _table_ = "metadata"
    field = Required(str)
    ingest_field = Optional(str)
    order = Required(float)
    display_name = Optional(str)
    colgroup = Optional(str)
    definition = Optional(str)
    possible_values = Optional(str)
    notes = Optional(str)
    entity_name = Required(str)
    export = Required(bool)
    class_name = Required(str)
    PrimaryKey(class_name, entity_name, field)

    def delete_2(records):
        """Custom delete function for Metadata class.

        See `custom_delete` definition for more information.

        """
        return custom_delete(db.Metadata, records)


class Glossary(db.Entity):
    """Definitions of terms, including parents of sub-categories."""
    _table_ = "glossary"
    id = PrimaryKey(int, auto=True)
    term = Required(str)
    subterm = Optional(str, default="n/a")
    definition = Optional(str, default="Definition currently being developed")
    reference = Optional(str, default="None")
    entity_name = Optional(str)
    field = Optional(str)

    def delete_2(records):
        """Custom delete function for Glossary class.

        See `custom_delete` definition for more information.

        """
        return custom_delete(db.Glossary, records)


class Plan(db.Entity):
    """Plans. Similar to policies but they lack legal authority."""
    pass


class Policy(db.Entity):
    """Non-pharmaceutical intervention (NPI) policies."""
    id = PrimaryKey(int, auto=False)
    source_id = Required(str)

    # descriptive information
    policy_name = Optional(str)
    desc = Optional(str)
    primary_ph_measure = Optional(str)
    ph_measure_details = Optional(str)
    policy_type = Optional(str)
    primary_impact = Optional(StrArray)
    intended_duration = Optional(str)
    announcement_data_source = Optional(str)
    policy_data_source = Optional(str)
    subtarget = Optional(str)  # multiselect, concat
    policy_number = Optional(int, nullable=True)
    relaxing_or_restricting = Optional(str)
    # enum_test = Optional(State, column='enum_test_str')

    # authority data
    auth_entity_has_authority = Optional(str)
    authority_name = Optional(str)
    auth_entity_authority_data_source = Optional(str)

    # key dates
    date_issued = Optional(date)
    date_start_effective = Optional(date)
    date_end_anticipated = Optional(date)
    date_end_actual = Optional(date)

    # relationships
    file = Set('File', table="file_to_policy")
    auth_entity = Set('Auth_Entity', table="auth_entity_to_policy")
    place = Optional('Place')
    prior_policy = Set('Policy', table="policy_to_prior_policy")
    _prior_policy = Set('Policy')

    # Currently unused attributes
    # policy_number = Optional(int)
    # legal_challenge = Optional(bool)
    # case_name = Optional(str)

    def delete_2(records):
        """Custom delete function for Policy class.

        See `custom_delete` definition for more information.

        """
        return custom_delete(db.Policy, records)

    def to_dict_2(self, **kwargs):
        """Converts instances of this entity class to dictionaries, along with
        any first-level children it has which are also instances of a supported
        database class.

        Parameters
        ----------
        **kwargs : dict
            Keyword arguments, used to support native `to_dict` behavior.

        Returns
        -------
        dict
            The dictionary.

        """
        # get which fields should be returned by entity name
        return_fields_by_entity = \
            kwargs['return_fields_by_entity'] if 'return_fields_by_entity' \
            in kwargs else dict()

        # if `only` was specified, use that as the `policy` entity's return
        # fields, and delete the `return_fields_by_entity` data.
        if 'only' in kwargs:
            return_fields_by_entity['policy'] = kwargs['only']
            del kwargs['only']
        del kwargs['return_fields_by_entity']

        # convert the policy instance to a dictionary, which may contain
        # various other types of entities in it represented only by their
        # unique IDs, rather than having their data provided as a dictionary
        instance_dict = None
        if 'policy' in return_fields_by_entity and \
                len(return_fields_by_entity['policy']) > 0:
            instance_dict = Policy.to_dict(
                self, only=return_fields_by_entity['policy'], **kwargs)
        else:
            instance_dict = Policy.to_dict(self, **kwargs)

        # iterate over the items in the Policy instance's dictionary in search
        # for other entity types for which we have unique IDs but need full
        # data dictionaries
        for k, v in instance_dict.items():

            # For each supported entity type, convert its unique ID into a
            # dictionary of data fields, limited to those defined in
            # `return_fields_by_entity`, if applicable.
            #
            # TODO ensure `return_fields_by_entity` is fully implemented
            # and flexible

            # Place
            if k == 'place':
                instance_dict[k] = Place[v].to_dict(
                    only=return_fields_by_entity['place'])

            # Auth_Entity
            elif k == 'auth_entity':
                instances = list()
                for id in v:
                    instances.append(Auth_Entity[id].to_dict())
                instance_dict[k] = instances

            # File
            elif k == 'file':
                instance_dict['file'] = list()
                file_fields = ['id']
                for id in v:
                    instance = File[id]
                    doc_instance_dict = instance.to_dict(only=file_fields)

                    # append file dict to list
                    instance_dict['file'].append(
                        doc_instance_dict['id']
                    )
        return instance_dict


class Place(db.Entity):
    _table_ = "place"
    id = PrimaryKey(int, auto=True)
    level = Optional(str)
    iso3 = Optional(str)
    country_name = Optional(str, nullable=True)
    area1 = Optional(str)
    area2 = Optional(str)
    loc = Optional(str)
    home_rule = Optional(str)
    dillons_rule = Optional(str)

    # relationships
    policies = Set('Policy')
    auth_entities = Set('Auth_Entity')
    observations = Set('Observation')


class Observation(db.Entity):
    """Observations made on places at dates."""
    _table_ = "observation"
    id = PrimaryKey(int, auto=True)
    date = Required(date)
    metric = Required(int)
    value = Required(str)
    source_id = Required(str)

    # relationships
    place = Required('Place')


class Auth_Entity(db.Entity):
    """Authorizing entities."""
    _table_ = "auth_entity"
    id = PrimaryKey(int, auto=True)
    name = Optional(str)
    office = Optional(str)

    # relationships
    policies = Set('Policy')
    place = Optional('Place')


class File(db.Entity):
    """Supporting documentation."""
    _table_ = "file"
    id = PrimaryKey(int, auto=True)
    name = Optional(str)
    type = Required(str)
    data_source = Optional(str)
    permalink = Optional(str, nullable=True)
    filename = Optional(str, nullable=True)
    airtable_attachment = Required(bool, default=False)

    # relationships
    policies = Set('Policy')
