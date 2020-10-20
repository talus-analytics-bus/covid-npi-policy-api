"""Define database models."""
# standard modules
import datetime
import json
from datetime import date

# 3rd party modules
from pony.orm import PrimaryKey, Required, Optional, Optional, Set, \
    StrArray, select, db_session, IntArray

# local modules
from .config import db


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
    id = PrimaryKey(int, auto=False)
    source_id = Required(str)

    # descriptive information
    name = Optional(str)
    desc = Optional(str)
    name_and_desc = Optional(str)
    org_name = Optional(str)
    primary_loc = Optional(str)
    org_type = Optional(str)
    search_text = Optional(str)

    # dates
    date_issued = Optional(date)
    date_start_effective = Optional(date)
    date_end_effective = Optional(date)

    # standardized fields / tags
    n_phases = Optional(int)
    auth_entity_has_authority = Optional(str)
    reqs_essential = Optional(StrArray, nullable=True)
    reqs_private = Optional(StrArray, nullable=True)
    reqs_school = Optional(StrArray, nullable=True)
    reqs_social = Optional(StrArray, nullable=True)
    reqs_hospital = Optional(StrArray, nullable=True)
    reqs_public = Optional(StrArray, nullable=True)
    reqs_other = Optional(StrArray, nullable=True)

    # university only
    residential = Optional(bool)

    # sourcing and PDFs
    plan_data_source = Optional(str)
    announcement_data_source = Optional(str)

    # relationships
    policy = Optional('Policy')
    file = Set('File', table="file_to_plan")
    place = Set('Place', table="place_to_plan")
    auth_entity = Set('Auth_Entity', table="auth_entity_to_plan")

    # TODO reuse code from `Policy` entity instead of repeating here

    def delete_2(records):
        """Custom delete function for Plan class.

        See `custom_delete` definition for more information.

        """
        return custom_delete(db.Plan, records)

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
            return_fields_by_entity['plan'] = kwargs['only']
            del kwargs['only']
        del kwargs['return_fields_by_entity']

        # convert the policy instance to a dictionary, which may contain
        # various other types of entities in it represented only by their
        # unique IDs, rather than having their data provided as a dictionary
        instance_dict = None
        if 'plan' in return_fields_by_entity and \
                len(return_fields_by_entity['plan']) > 0:
            instance_dict = Plan.to_dict(
                self, only=return_fields_by_entity['plan'], **kwargs)
        else:
            instance_dict = Plan.to_dict(self, **kwargs)

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
                instances = list()
                for id in v:
                    try:
                        instances.append(Place[id].to_dict())
                    except:
                        pass
                instance_dict[k] = instances

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


class Policy(db.Entity):
    """Non-pharmaceutical intervention (NPI) policies."""
    id = PrimaryKey(int, auto=False)
    source_id = Required(str)

    # descriptive information
    policy_name = Optional(str)
    desc = Optional(str)
    name_and_desc = Optional(str)
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
    search_text = Optional(str)

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
    place = Set('Place', table="place_to_policy")
    policy_numbers = Set('Policy_Number', table="policy_number_to_policy")
    prior_policy = Set('Policy', table="policy_to_prior_policy")
    _prior_policy = Set('Policy', reverse='prior_policy')
    plan = Optional('Plan')
    court_challenges = Set(
        'Court_Challenge',
        table="policies_to_court_challenges"
    )

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
                instances = list()
                for id in v:
                    try:
                        instances.append(Place[id].to_dict())
                    except:
                        pass
                instance_dict[k] = instances

            # Auth_Entity
            elif k == 'auth_entity':
                instances = list()
                for id in v:
                    try:
                        only = return_fields_by_entity['auth_entity'] if \
                            'auth_entity' in return_fields_by_entity else \
                            None
                        instances.append(
                            Auth_Entity[id].to_dict_2(only=only)
                        )
                    except:
                        pass
                instance_dict[k] = instances

            # File
            elif k == 'file':
                instance_dict['file'] = list()
                file_fields = ['id']
                for id in v:
                    try:
                        instance = File[id]
                        doc_instance_dict = instance.to_dict(only=file_fields)

                        # append file dict to list
                        instance_dict['file'].append(
                            doc_instance_dict['id']
                        )
                    except:
                        pass
        return instance_dict


class Policy_Number(db.Entity):
    """Policy numbers grouping sets of policies (i.e., policy
    sections) together.

    """
    id = PrimaryKey(int, auto=False) # the policy number

    # relationships
    policies = Set('Policy')

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
    plans = Set('Plan')
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
    plans = Set('Plan')
    place = Optional('Place')

    def to_dict_2(self, only=None, **kwargs):
        # get basic dict
        d = self.to_dict(
            with_collections=True,
            related_objects=True,
            only=only
        )

        # process place
        if 'place' in d:
            d['place'] = d['place'].to_dict()
        return d


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
    plans = Set('Plan')


class Court_Challenge(db.Entity):
    """Court challenges for policies."""
    _table_ = "court_challenge"

    # Standard fields
    id = PrimaryKey(int, auto=True)
    jurisdiction = Optional(str)
    court = Optional(str)
    legal_authority_challenged = Optional(str)
    parties = Optional(str)
    date_of_complaint = Optional(date)
    date_of_decision = Optional(date)
    case_number = Optional(str)
    legal_citation = Optional(str)
    filed_in_state_or_federal_court = Optional(str)
    summary_of_action = Optional(str)
    complaint_category = Optional(StrArray, nullable=True)
    legal_challenge = Optional(bool, nullable=True)
    case_name = Optional(str)
    procedural_history = Optional(str)
    holding = Optional(str)
    government_order_upheld_or_enjoined = Optional(str)
    subsequent_action_or_current_status = Optional(str)
    did_doj_file_statement_of_interest = Optional(str)
    summary_of_doj_statement_of_interest = Optional(str)
    data_source_for_complaint = Optional(str)
    data_source_for_decision = Optional(str)
    data_source_for_doj_statement_of_interest = Optional(str)
    pdf_documentation = Optional(StrArray, nullable=True)  # TODO grab files
    policy_or_law_name = Optional(str)
    source_id = Required(str)
    search_text = Optional(str)

    # Relationships
    policies = Set('Policy', table="policies_to_court_challenges")
    matter_numbers = Optional(IntArray, nullable=True)

    def to_dict_2(self, **kwargs):

        # get fields to return
        only = kwargs['return_fields_by_entity']['court_challenge'] \
            if 'return_fields_by_entity' in kwargs else None

        i_dict = Court_Challenge.to_dict(
            self,
            with_collections=True,
            related_objects=True,
            only=only
        )
        return json.loads(json.dumps(i_dict, default=jsonify_custom))

    def delete_2(records):
        """Custom delete function for Court_Challenge class.

        See `custom_delete` definition for more information.

        """
        return custom_delete(db.Court_Challenge, records)


# class Matter_Number(db.Entity):
#     """Matter numbers organizing court challenges into groups."""
#     # Standard
#     id = PrimaryKey(int, auto=True)
#     ids = Optional(StrArray, nullable=True)
#     parties = Optional(StrArray, nullable=True)
#     case_numbers = Optional(StrArray, nullable=True)
#
#     # Relationships
#     court_challenges = Set(
#         'Court_Challenge', table="court_challenges_to_matter_numbers")

only = {
    'Policy': [
        'id',
        'policy_name',
    ]
}


def jsonify_custom(obj):
    """Define how related entities should be represented as JSON.

    Parameters
    ----------
    obj : type
        Description of parameter `obj`.

    Returns
    -------
    type
        Description of returned object.

    """
    to_check = only.keys()

    if isinstance(obj, set):
        return list(obj)
        raise TypeError
    elif isinstance(obj, date):
        return str(obj)
    else:
        for entity_name in to_check:
            if isinstance(obj, getattr(db, entity_name)):
                return obj.to_dict(only=only[entity_name])
