"""Define API data processing methods"""
# standard modules
import functools
from io import BytesIO
from datetime import datetime, date, timedelta
from collections import defaultdict

# 3rd party modules
import boto3
from pony.orm import db_session, select, get, commit, desc, count, raw_sql
from fastapi.responses import FileResponse, Response

# local modules
from .export import CovidPolicyExportPlugin
from .models import Policy, PolicyList, PolicyDict, PolicyStatus, PolicyStatusList, \
    Auth_Entity, Place, File
from .util import str_to_date
from db import db

# # Code optimization profiling
# import cProfile
# import pstats
# p = cProfile.Profile()

# constants
s3 = boto3.client('s3')
S3_BUCKET_NAME = 'covid-npi-policy-storage'


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
def export(filters: dict = None):
    """Return XLSX data export for policies with the given filters applied.

    Parameters
    ----------
    filters : dict
        The filters to apply.

    Returns
    -------
    fastapi.responses.Response
        The XLSX data export file.

    """
    # Create Excel export file
    genericExcelExport = CovidPolicyExportPlugin(db, filters)
    content = genericExcelExport.build()
    media_type = 'application/' + \
        'vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    return Response(
        content=content,
        media_type=media_type
    )


@db_session
def get_version():
    # q = select(i for i in db.Version)
    # data = [i.to_dict(only=['type', 'date']) for i in q]
    data_tmp = db.Version.select_by_sql(f'''
        SELECT distinct on ("type") * FROM "version"
        ORDER BY "type", "date" desc
                                    ''')
    data = [i.to_dict(only=['type', 'date'])
            for i in data_tmp]
    data.sort(key=lambda x: x['type'], reverse=True)
    data.sort(key=lambda x: x['date'], reverse=True)
    return {
        'success': True,
        'data': data,
        'message': 'Success'
    }


@db_session
# @cached
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
    # define output data dict
    data = dict()

    # for each field for which metadat is needed
    n = 0
    for d in fields:

        # get entity class name and field
        entity_name, field = d.split('.')

        # get metadata instance from db that matches this field
        metadatum = get(
            i for i in db.Metadata
            if i.field == field
            and i.entity_name.lower() == entity_name
        )

        # store metadata fields in output data if they exist
        if metadatum is not None:
            data[d] = metadatum.to_dict()
            n += 1
        else:
            data[d] = dict()

    # return response dictionary
    return {
        'success': True,
        'message': f'''Found {n} metadata values.''',
        'data': data
    }


@db_session
def get_file_title(id: int):
    """Gets file title from database.

    Parameters
    ----------
    id : int
        Unique ID of the File instance which corresponds to the S3 file to
        be served.

    Returns
    -------
    str
        The file title.

    """

    # define filename from File instance field
    file = db.File[id]
    return file.name


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

    # define filename from File instance field
    file = db.File[id]
    key = file.filename

    # retrieve file and write it to IO file object `data`
    # if the file is not found in S3, return a 404 error
    data = BytesIO()
    try:
        s3.download_fileobj(S3_BUCKET_NAME, key, data)
    except Exception as e:
        print('e')
        print(e)
        return 'Document not found (404)'

    # return to start of IO stream
    data.seek(0)

    # return export file
    content = data.read()

    # return file with correct media type given its extension
    media_type = 'application'
    if key.endswith('.pdf'):
        media_type = 'application/pdf'
    return Response(content=content, media_type=media_type)


@db_session
@cached
def get_policy(
    filters: dict = None,
    fields: list = None,
    order_by_field: str = 'date_start_effective',
    return_db_instances: bool = False,
    by_category: str = None,
):
    """Returns Policy instance data that match the provided filters.

    Parameters
    ----------
    filters : dict
        Dictionary of filters to be applied to policy data (see function
        `apply_policy_filters` below).
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
        desc(getattr(db.Policy, order_by_field)))

    # apply filters if any
    if filters is not None:
        q = apply_policy_filters(q, filters)

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
        return_fields_by_entity['place'] = [
            'id', 'level', 'loc']

        # define list of instances to return
        data = []

        # for each policy
        for d in q:

            # convert it to a dictionary returning only the specified fields
            d_dict = d.to_dict_2(
                return_fields_by_entity=return_fields_by_entity)

            # add it to the output list
            data.append(d_dict)

        # if by category: transform data to organize by category
        # NOTE: assumes one `primary_ph_measure` per Policy
        if by_category is not None:
            data_by_category = defaultdict(list)
            for i in data:
                data_by_category[i[by_category]].append(i)

            res = PolicyDict(
                data=data_by_category,
                success=True,
                message=f'''{len(q)} policies found'''
            )
        else:
            # create response from output list
            res = PolicyList(
                data=data,
                success=True,
                message=f'''{len(q)} policies found'''
            )
        return res


@cached
@db_session
def get_policy_status(
    is_lockdown_level: bool = None,
    geo_res: str = None,
    name: str = None,
    filters: dict = dict()
):
    """TODO"""

    # DEBUG filter by USA only
    filters['iso3'] = ['USA']

    # get ordered policies from database
    q = select(i for i in db.Policy if i.place.level ==
               'State / Province')

    # initialize output data
    data = None

    # Case A: Lockdown level
    if is_lockdown_level is None:
        is_lockdown_level = (
            'lockdown_level' in filters and
            filters['lockdown_level'][0] == 'lockdown_level'
        )
    if is_lockdown_level:

        # get dates to check
        start = None
        end = None
        if 'dates_in_effect' in filters:
            start, end = filters['dates_in_effect']
            start = datetime.strptime(start, '%Y-%m-%d').date()
            end = datetime.strptime(end, '%Y-%m-%d').date()

        # If a date range is provided and the dates aren't the same, return
        # a not implemented message
        if start is not None and end is not None and start != end:
            return PolicyStatusList(
                data=list(),
                success=False,
                message=f'''Start and end dates must be identical.'''
            )
        else:
            # if date is not provided, return it in the response
            specify_date = start is None and end is None

            # collate list of lockdown level statuses based on state / province
            data = list()

            # RETURN MOST RECENT OBSERVATION FOR EACH PLACE
            if name is None:
                q = db.Observation.select_by_sql(
                    f'''
                            select distinct on (place) *
                            from observation o
                            where date <= '{str(start)}'
                            order by place, date desc
                    ''')
                data = [
                    {
                        'place_name': i.place.area1,
                        'value': i.value,
                        'datestamp': i.date,
                    } for i in q
                ]
            else:

                # get all observations for the current date and convert them into
                # policy statuses
                observations = select(
                    i for i in db.Observation
                    if i.metric == 0
                    and (start is None or i.date == start)
                ).order_by(db.Observation.date)

                if name is not None:
                    observations = observations.filter(
                        lambda x: x.place.area1 == name)

                for d in observations:
                    datum = {
                        'value': d.value,
                    }
                    if specify_date:
                        datum['datestamp'] = d.date
                    if name is None:
                        datum['place_name'] = d.place.area1
                    data.append(
                        PolicyStatus(
                            **datum
                        )
                    )
    else:

        # Case B: Any other category
        # apply filters if any
        if filters is not None:
            q = apply_policy_filters(q, filters)

        q_area1 = select(i.place.area1 for i in q)

        data_tmp = dict()
        for i in q_area1:
            if i not in data_tmp:
                data_tmp[i] = PolicyStatus(
                    place_name=i,
                    value="policy in place"
                )
        data = list(data_tmp.values())

    # create response from output list
    res = PolicyStatusList(
        data=data,
        success=True,
        message=f'''Found {str(len(data))} statuses{'' if name is None else ' for ' + name}'''
    )
    return res


@cached
@db_session
def get_lockdown_level(
    geo_res: str = None,
    name: str = None,
    date: str = None,
    end_date: str = None,
):
    """TODO"""

    # if date is not provided, return it in the response
    specify_date = date is None

    # collate list of lockdown level statuses based on state / province
    data = list()

    # RETURN MOST RECENT OBSERVATION FOR EACH PLACE
    distinct_clause = \
        'distinct on (place)' if end_date is None else \
        'distinct on (place, date)'
    q = db.Observation.select_by_sql(
        f'''
                select {distinct_clause} *
                from observation o
                where date <= '{date if end_date is None else end_date}'
                order by place, date desc
        ''')

    for i in q:
        datum = {
            'value': i.value,
            'datestamp': i.date,
        }
        if name is None:
            datum['place_name'] = i.place.area1
        elif i.place.area1 != name:
            continue
        data.append(datum)

    # if `end_date` is specified, keep adding data until it is reached
    if end_date is not None and len(data) > 0:

        end_date_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
        pull_final_value_forward = data[0]['datestamp'] < end_date_dt
        if pull_final_value_forward:
            last_datum = data[len(data) - 1]
            prv_date = last_datum['datestamp']
            cur_date = prv_date + timedelta(days=1)
            while prv_date < end_date_dt:
                datum = {
                    'value': last_datum['value'],
                    'datestamp': str(cur_date),
                }
                if 'place_name' in last_datum:
                    datum['place_name'] = last_datum['place_name']

                data.insert(
                    0,
                    datum
                )
                prv_date = cur_date
                cur_date = cur_date + timedelta(days=1)

    # create response from output list
    res = PolicyStatusList(
        data=data,
        success=True,
        message=f'''Found {str(len(data))} statuses{'' if name is None else ' for ' + name}'''
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

    TODO remove bottleneck in AWS deployed version

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

    # # Enable profiling
    # p.enable()

    # define which data fields use groups
    # TODO dynamically
    fields_using_groups = ('Policy.ph_measure_details')
    fields_using_geo_groups = ('Place.area1', 'Place.area2')

    # define output data dict
    data = dict()

    # for each field to get optionset values for:
    for d_str in fields:

        # split into entity class name and field
        entity_name, field = d_str.split('.')
        entity_class = getattr(db, entity_name)

        # get all possible values for the field in the database, and sort them
        # such that "Unspecified" is last
        # TODO handle other special values like "Unspecified" as needed
        options = select(
            getattr(i, field) for i in entity_class
        ).filter(lambda x: x is not None)[:][:]
        options.sort()
        options.sort(key=lambda x: x == 'Other')
        options.sort(key=lambda x: x in('Unspecified', 'Local'))

        # skip blank strings
        options = filter(lambda x: x.strip() != '', options)

        # assign groups, if applicable
        uses_nongeo_groups = d_str in fields_using_groups
        uses_geo_groups = d_str in fields_using_geo_groups
        uses_groups = uses_nongeo_groups or uses_geo_groups
        if uses_nongeo_groups:
            options_with_groups = list()
            for option in options:
                # get group from glossary data
                parent = db.Glossary.get(
                    **{
                        'entity_name': entity_name,
                        'field': field,
                        'subterm': option
                    }
                )
                # if a parent was found use its term as the group, otherwise
                # specify "Other" as the group
                if parent:
                    options_with_groups.append([option, parent.term])
                else:
                    # TODO figure out best way to handle "Other" cases
                    options_with_groups.append([option, 'Other'])
            options = options_with_groups
        elif uses_geo_groups:
            options_with_groups = list()
            if field == 'area1':
                for option in options:
                    # get group from glossary data
                    parent = select(
                        i for i in db.Place
                        if i.area1 == option
                    ).first()
                    # if a parent was found use its term as the group, otherwise
                    # specify "Other" as the group
                    if parent:
                        options_with_groups.append(
                            [option, parent.country_name])
                    else:
                        # TODO figure out best way to handle "Other" cases
                        options_with_groups.append([option, 'Other'])
            elif field == 'area2':
                for option in options:
                    # get group from glossary data
                    parent = select(
                        i for i in db.Place
                        if i.area2 == option
                    ).first()
                    # if a parent was found use its term as the group, otherwise
                    # specify "Other" as the group
                    if parent:
                        options_with_groups.append([option, parent.area1])
                    else:
                        # TODO figure out best way to handle "Other" cases
                        options_with_groups.append([option, 'Other'])

            options = options_with_groups

        # return values and labels, etc. for each option
        id = 0

        # init list of optionset values for the field
        data[field] = []

        # for each possible option currently in the data
        for dd in options:

            # append an optionset entry
            value = dd if not uses_groups else dd[0]

            # skip unspecified values
            if value == 'Unspecified':
                continue

            group = None if not uses_groups else dd[1]
            datum = {
                'id': id,
                'value': value,
                'label': value,
                # 'label': get_label_from_value(field, value),
            }
            if uses_groups:
                datum['group'] = group
            data[field].append(datum)
            id = id + 1

    # # Disable profiling
    # p.disable()
    #
    # # Dump the stats to a file
    # p.dump_stats("res_focus.prof")
    # p2 = pstats.Stats('res_focus.prof')
    # p2.sort_stats('cumulative').print_stats(10)

    # return all optionset values
    return {
        'data': data,
        'success': True,
        'message': f'''Returned {len(fields)} optionset lists''',
    }


def get_label_from_value(field, value):
    """Given the data field and value, return the label that should be used
    to refer to the value in front-ends.

    TODO more dynamically

    Parameters
    ----------
    field : type
        Description of parameter `field`.
    value : type
        Description of parameter `value`.

    Returns
    -------
    type
        Description of returned object.

    """
    if field == 'level':
        if value == 'Intermediate area':
            return 'State / province'
        elif value == 'Local area':
            return 'Local'
        else:
            return value
    else:
        return value


@db_session
def apply_policy_filters(q, filters: dict = dict()):
    """Given the PonyORM query for policies and relevant filters, applies
    filters with AND logic.

    TODO ensure this works for arbitrary large numbers of filtered fields.

    Parameters
    ----------
    q : pony.orm.Query
        A Query instance, e.g., created by a call to `select`, for policies
    filters : dict[str, list]
        Dictionary with keys of field names and values of lists of
        allowed values (AND logic).

    Returns
    -------
    pony.orm.Query
        The query with filters applied.

    """
    # for each filter set provided
    for field, allowed_values in filters.items():

        # if no values were specified, assume no filter is applied
        # and continue
        if len(allowed_values) == 0:
            continue

        # if it is a date field, handle it specially
        if field.startswith('date'):

            # set allowed values to be start and end date instances
            allowed_values = list(
                map(str_to_date, allowed_values)
            )

            # if it's the special "dates_in_effect" filter, handle it
            # and continue
            if field == 'dates_in_effect':
                start = allowed_values[0]
                end = allowed_values[1]

                q = select(
                    i for i in q
                    # starts before or during `start` when end date unknown
                    if (
                        i.date_end_actual is None and i.date_end_anticipated \
                        is None and i.date_start_effective <= start
                    )
                    # starts before AND ends after
                    or (
                        i.date_start_effective < start and (i.date_end_actual > end or (
                            i.date_end_actual is None and i.date_end_anticipated > end))
                    )

                    # starts during OR ends during
                    or (
                        (i.date_start_effective >= start and i.date_start_effective <= end) or (
                            (i.date_end_actual >= start and i.date_end_actual <= end) or (
                                i.date_end_actual is None and (
                                    i.date_end_anticipated >= start and i.date_end_anticipated <= end)
                            )
                        )
                    )

                )
                continue

        # is the filter applied by joining a policy instance to a
        # different entity?
        # TODO generalize this and rename function `apply_policy_filters`
        join = field in ('level', 'loc', 'area1',
                         'iso3', 'country_name', 'area2')

        # if the filter is not a join, i.e., is on policy native fields
        if not join:

            # apply the filter
            q = select(
                i
                for i in q
                if getattr(i, field) in allowed_values
            )

        # otherwise, apply the filter to the linked entity
        else:
            q = select(
                i
                for i in q
                if getattr(i.place, field) in allowed_values
            )

    # return the filtered query instance
    return q
