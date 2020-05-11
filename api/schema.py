"""Define API data processing methods"""
# standard modules
import functools
from io import BytesIO
from datetime import datetime, date

# 3rd party modules
import boto3
from pony.orm import db_session, select, get, commit
from fastapi.responses import FileResponse, Response

# local modules
from ingest import CovidPolicyPlugin
from .export import CovidPolicyExportPlugin
from .models import Policy, PolicyList, Auth_Entity, Place, Doc
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
    # for each field, parse its entity and get the metadata for it
    data = dict()

    for d in fields:
        entity_name, field = d.split('.')
        metadatum = get(
            i for i in db.Metadata
            if i.field == field
            and i.entity.lower() == entity_name
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
def clean_docs():
    # update database to remove docs with broken links
    docs = db.Doc.select()
    n = len(docs)
    i = 0
    for doc in docs:
        i = i + 1
        print(str(i) + ' of ' + str(n))
        if doc.pdf is None:
            print('No file, skipping')
            continue
        # define filename from db
        file_key = doc.pdf + '.pdf'
        s3_bucket = 'covid-npi-policy-storage'

        # retrieve file and write it to IO file object
        # io_instance = BytesIO()
        try:
            s3.head_object(Bucket=s3_bucket, Key=file_key)
            print('File found')
        except Exception as e:
            print('e')
            print(e)
            doc.pdf = None
            commit()
            print('Document not found (404)')

    return 'Done'


@db_session
# @cached
def get_doc(id: int):

    # define filename from db
    doc = db.Doc[id]
    file_key = doc.pdf + '.pdf'
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
    return Response(content=content, media_type='application/pdf')


@db_session
@cached
def get_policy(
    filters=None,
    fields=None,
    return_db_instances=False
):
    all = fields is None
    q = select(i for i in db.Policy)
    if filters is not None:
        q = apply_filters(q, filters)

    if return_db_instances:
        return q
    else:
        instance_list = []
        for d in q:
            d_dict = d.to_dict_2(only=fields)
            instance_list.append(d_dict)
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
@cached
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
