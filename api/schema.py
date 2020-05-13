"""Define API data processing methods"""
# standard modules
import functools
from io import BytesIO
from datetime import datetime, date
from collections import defaultdict

# 3rd party modules
import boto3
from pony.orm import db_session, select, get, commit
from fastapi.responses import FileResponse, Response

# local modules
from .export import CovidPolicyExportPlugin
from .models import Policy, PolicyList, Auth_Entity, Place, File
from db import db


# constants
s3 = boto3.client('s3')


def cached(func):
    """ Caching """
    cache = {}

    @functools.wraps(func)
    def wrapper(*func_args, **kwargs):

        key = str(kwargs)
        if key in cache:
            return cache[key]

        results = func(*func_args, **kwargs)
        cache[key] = results
        return results

        # # Code for JWT-friendly caching below.
        # # get jwt
        # jwt_client = func_args[1].context.args.get('jwt_client')
        #
        # # if not debug mode and JWT is missing, return nothing
        # if not args.debug and jwt_client is None:
        #     return []
        #
        # # form key using user type
        # type = 'unspecified'
        # if jwt_client is not None:
        #     jwt_decoded_json = jwt.decode(jwt_client, args.jwt_secret_key)
        #     type = jwt_decoded_json['type']
        # key = str(kwargs) + ':' + type

    return wrapper


@db_session
@cached
def export(filters):
    # Create Excel export file
    genericExcelExport = CovidPolicyExportPlugin(db, filters)
    content = genericExcelExport.build()

    return Response(content=content, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


@db_session
@cached
def get_metadata(fields: list):
    """Returns Metadata instance fields for the fields specified.

    Parameters
    ----------
    fields : list
        List of fields as strings with entity name prefixed, e.g.,
        `policy.id`.

    Returns
    -------
    dict
        Response containing metadata information for the fields.

    """
    # for each field, parse its entity and get the metadata for it
    data = dict()

    for d in fields:
        entity_name, field = d.split('.')
        metadatum = get(
            i for i in db.Metadata
            if i.field == field
            and i.entity_name.lower() == entity_name
        )
        if metadatum is not None:
            data[d] = metadatum.to_dict()
        else:
            data[d] = dict()
    return {
        'success': True,
        'message': f'''Metadata values retrieved''',
        'data': data
    }


@db_session
def get_file(id: int):
    """Serves the file from S3 that corresponds to the File instances with
    the specified id.

    Parameters
    ----------
    id : int
        Unique ID of the File instance which corresponds to the S3 file to
        be served.

    Returns
    -------
    fastapi.responses.Response
        The file.

    """

    # define filename from db
    file = db.File[id]
    file_key = file.filename
    s3_bucket = 'covid-npi-policy-storage'

    # retrieve file and write it to IO file object
    io_instance = BytesIO()
    try:
        s3.download_fileobj(s3_bucket, file_key, io_instance)
    except Exception as e:
        print('e')
        print(e)
        return 'Document not found (404)'

    # return to start of IO stream
    io_instance.seek(0)

    # return export file
    content = io_instance.read()

    # return file
    media_type = 'application'
    if file_key.endswith('.pdf'):
        media_type = 'application/pdf'

    return Response(content=content, media_type=media_type)


@db_session
@cached
def get_policy(
    filters: dict = None,
    fields: list = None,
    order_by_field: str = 'date_start_effective'
    return_db_instances: bool = False,
):
    """Returns Policy instance data that match the provided filters.

    Parameters
    ----------
    filters : dict
        Dictionary of filters to be applied to policy data (see function
        `apply_filters` below).
    fields : list
        List of Policy instance fields that should be returned. If None, then
        all fields are returned.
    order_by_field : type
        String defining the field in the class `Policy` that is used to
        order the policies returned.
    return_db_instances : bool
        If true, returns the PonyORM database query object containing the
        filtered policies, otherwise returns the list of dictionaries
        containing the policy data as part of a response dictionary

    Returns
    -------
    pony.orm.Query **or** dict
        Query instance if `return_db_instances` is true, otherwise a list of
        dictionaries in a response dictionary

    """
    # return all fields?
    all = fields is None

    # get ordered policies from database
    q = select(i for i in db.Policy).order_by(
        getattr(db.Policy, order_by_field))

    # apply filters if any
    if filters is not None:
        q = apply_filters(q, filters)

    # return query object if arguments requested it
    if return_db_instances:
        return q

    # otherwise prepare list of dictionaries to return
    else:

        return_fields_by_entity = defaultdict(list)
        if fields is not None:
            return_fields_by_entity['policy'] = fields

        # TODO dynamically set fields returned for Place and other
        # linked entities
        return_fields_by_entity['place'] = ['id', 'level', 'area1', 'loc']

        # define list of instances to return
        data = []

        # for each policy
        for d in q:

            # convert it to a dictionary returning only the specified fields
            d_dict = d.to_dict_2(
                return_fields_by_entity=return_fields_by_entity)

            # add it to the output list
            data.append(d_dict)

        # create response from output list
        res = PolicyList(
            data=data,
            success=True,
            message=f'''{len(q)} policies found'''
        )
        return res


@db_session
@cached
def get_optionset(fields: list = list()):
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
    dict
        List of possible optionset values for each field, contained in a
        response dictionary

    """
    # define output data dict
    data = dict()

    # for each field to get optionset values for:
    for d_str in fields:

        # split into entity class name and field
        entity_class_name, field = d_str.split('.')
        entity_class = getattr(db, entity_class_name)

        # get all possible values for the field in the database, and sort them
        # such that "Unspecified" is last
        # TODO handle other special values like "Unspecified" as needed
        options = select(getattr(i, field) for i in entity_class)[:][:]
        options.sort()
        options.sort(key=lambda x: x == 'Unspecified')

        # return values and labels, etc. for each option
        id = 0

        # init list of optionset values for the field
        data[field] = []

        # for each possible option currently in the data
        for dd in options:

            # append an optionset entry
            data[field].append(
                {
                    'id': id,
                    'value': dd,
                    'label': dd
                }
            )
            id = id + 1

    # return all optionset values
    return {
        'data': data,
        'success': True,
        'message': f'''Returned {len(fields)} optionset lists''',
    }


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
