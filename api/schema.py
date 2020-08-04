"""Define API data processing methods"""
# standard modules
import functools
import math
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
    Auth_Entity, Place, File, PlanList
from .util import str_to_date, find, download_file
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
def get_countries_with_lockdown_levels():
    countries_with_lockdown_levels = select(
        i.place.iso3
        for i in db.Observation
        if i.metric == 0
    )
    return {
        'success': True,
        'message': 'Success',
        'data': countries_with_lockdown_levels[:][:]
    }


@db_session
@cached
def export(filters: dict = None, class_name: str = 'Policy'):
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
    media_type = 'application/' + \
        'vnd.openxmlformats-officedocument.spreadsheetml.sheet'

    # If all data: return static Excel file
    if class_name == 'all_static':
        today = date.today()
        file = download_file(
            'https://gida.ghscosting.org/downloads/COVID AMP - Policy and Plan Data Export.xlsx', 'COVID AMP - Full Data Export - ' + str(today).replace('-', ''), None, as_object=True)
        return Response(
            content=file,
            media_type=media_type
        )
    else:
        # Create Excel export file
        genericExcelExport = CovidPolicyExportPlugin(db, filters, class_name)
        content = genericExcelExport.build()

        return Response(
            content=content,
            media_type=media_type
        )


@db_session
def get_version():
    data_tmp = db.Version.select_by_sql(f'''
        SELECT distinct on ("type") * FROM "version"
        ORDER BY "type", "date" desc
                                    ''')
    data = [i.to_dict(only=['type', 'date', 'last_datum_date'])
            for i in data_tmp]
    data.sort(key=lambda x: x['type'], reverse=True)
    data.sort(key=lambda x: x['date'], reverse=True)
    return {
        'success': True,
        'data': data,
        'message': 'Success'
    }


@db_session
def get_count(class_names):
    """Return the number of instances for entities in the db, if they are
    on the list of supported entities.

    Parameters
    ----------
    class_names : type
        Description of parameter `class_names`.

    Returns
    -------
    type
        Description of returned object.

    """
    supported_entities = ('Policy', 'Plan')
    data = dict()
    for d in class_names:
        if d not in supported_entities or not hasattr(db, d):
            continue
        else:
            n = get(count(i) for i in getattr(db, d))
            data[d] = n
    return {
        'success': True,
        'data': data,
        'message': 'Success'
    }


@db_session
# @cached
def get_metadata(fields: list, entity_class_name: str):
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
            and i.class_name == entity_class_name
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
    ordering: list = [],
    # ordering: list = [['policy_name', 'asc'], ['date_start_effective', 'desc']],
    fields: list = None,
    order_by_field: str = 'date_start_effective',
    return_db_instances: bool = False,
    by_category: str = None,
    page: int = None,
    pagesize: int = 100
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

    # use pagination if all fields are requested, and set value for `page` if
    # none was provided in the URL query args
    # TODO use pagination in all cases with a URL param arg-definable
    # page size
    use_pagination = (all or page is not None) and not return_db_instances
    if use_pagination and (page is None or page == 0):
        page = 1

    # get ordered policies from database
    q = select(i for i in db.Policy)
    ordering.reverse()
    print(ordering)
    for field, direction in ordering:
        print(field)
        print(direction)
        if direction == 'desc':
            q = q.order_by(desc(getattr(db.Policy, field)))
        else:
            q = q.order_by(getattr(db.Policy, field))

    # apply filters if any
    if filters is not None:
        q = apply_policy_filters(q, filters)

    # get len of query
    n = count(q) if use_pagination else None

    # apply pagination if using
    if use_pagination:
        q = q.page(page, pagesize=pagesize)

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

        # if pagination is being used, get next page URL if there is one
        n_pages = None if not use_pagination else math.ceil(n / pagesize)
        more_pages = use_pagination and page < n_pages
        next_page_url = None if not more_pages else \
            f'''/get/policy?page={str(page + 1)}&pagesize={str(pagesize)}'''

        # if by category: transform data to organize by category
        # NOTE: assumes one `primary_ph_measure` per Policy
        if by_category is not None:
            data_by_category = defaultdict(list)
            for i in data:
                data_by_category[i[by_category]].append(i)

            res = PolicyDict(
                data=data_by_category,
                success=True,
                message=f'''{len(q)} policies found''',
                next_page_url=next_page_url
            )
        else:
            # create response from output list
            res = PolicyList(
                data=data,
                success=True,
                message=f'''{len(q)} policies found''',
                next_page_url=next_page_url
            )
        return res


@db_session
@cached
def get_plan(
    filters: dict = None,
    fields: list = None,
    order_by_field: str = 'date_issued',
    return_db_instances: bool = False,
    by_category: str = None,
):
    """Returns Plan instance data that match the provided filters.

    Parameters
    ----------
    filters : dict
        Dictionary of filters to be applied to plan data (see function
        `apply_policy_filters` below).
    fields : list
        List of Plan instance fields that should be returned. If None, then
        all fields are returned.
    order_by_field : type
        String defining the field in the class `Plan` that is used to
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
    q = select(i for i in db.Plan).order_by(
        desc(getattr(db.Plan, order_by_field)))

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
            return_fields_by_entity['plan'] = fields

        # TODO dynamically set fields returned for Place and other
        # linked entities
        return_fields_by_entity['place'] = [
            'id', 'level', 'loc']
        return_fields_by_entity['auth_entity'] = [
            'id', 'name']

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
            pass
            # data_by_category = defaultdict(list)
            # for i in data:
            #     data_by_category[i[by_category]].append(i)
            #
            # res = PolicyDict(
            #     data=data_by_category,
            #     success=True,
            #     message=f'''{len(q)} plans found'''
            # )
        else:
            # create response from output list
            res = PlanList(
                data=data,
                success=True,
                message=f'''{len(q)} plans found'''
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
    filters['iso3'] = ['USA'] if geo_res == 'state' else []

    # get ordered policies from database
    level = 'State / Province'
    if geo_res == 'country':
        level = 'Country'

    q = select(i for i in db.Policy)
    # q = select(i for i in db.Policy if i.place.level == level)

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
                        'place_name': i.place.area1 if geo_res == 'state' else i.place.iso3,
                        'value': i.value,
                        'datestamp': i.date,
                    } for i in q if i.place.level == level
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

        loc_field = 'area1'
        if geo_res == 'country':
            loc_field = 'iso3'

        q_loc = select(getattr(i.place, loc_field) for i in q)

        data_tmp = dict()
        for i in q_loc:
            if i not in data_tmp:
                data_tmp[i] = PolicyStatus(
                    place_name=i,
                    value="t"
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
    iso3: str = None,
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

    country_only = geo_res == 'country'
    for i in q:
        if country_only and i.place.level != 'Country':
            continue
        else:
            datum = {
                'value': i.value,
                'datestamp': i.date,
            }
            if country_only:
                if iso3 == 'all':
                    datum['place_name'] = i.place.iso3
                elif i.place.iso3 != iso3:
                    continue
            else:
                if name is None:
                    datum['place_name'] = i.place.area1
                elif i.place.area1 != name:
                    continue
        data.append(datum)

    # if `end_date` is specified, keep adding data until it is reached
    if end_date is not None and len(data) > 0:

        # enddate date instance
        end_date_dt = datetime.strptime(end_date, '%Y-%m-%d').date()

        pull_final_value_forward = data[0]['datestamp'] < end_date_dt
        if pull_final_value_forward:
            last_datum = data[0]
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
# @cached
def get_optionset(fields: list = list(), class_name: str = 'Policy'):
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

    # get all glossary terms if needed
    need_glossary_terms = any(d_str in fields_using_groups for d_str in fields)
    glossary_terms = \
        select(i for i in db.Glossary)[:][:] if need_glossary_terms \
        else list()

    # check places relevant only for the entity of `class_name`
    class_name_field = 'policies' if class_name == 'Policy' \
        else 'plans'

    # get all places if needed
    need_places = any(d_str in fields_using_geo_groups for d_str in fields)
    place_instances = \
        select(
            (i.area1, i.area2, i.country_name)
            for i in db.Place
            if len(getattr(i, class_name_field)) > 0
        )[:][:] if need_places \
        else list()

    # for each field to get optionset values for:
    for d_str in fields:

        # split into entity class name and field
        entity_name, field = d_str.split('.')
        entity_class = getattr(db, entity_name)

        # get all possible values for the field in the database, and sort them
        # such that "Unspecified" is last
        # TODO handle other special values like "Unspecified" as needed
        options = None
        if field == 'country_name' or field == 'level':
            options = select(
                getattr(i, field) for i in entity_class
                if len(getattr(i, class_name_field)) > 0
            ).filter(lambda x: x is not None)[:][:]
        else:
            options = select(
                getattr(i, field) for i in entity_class
            ).filter(lambda x: x is not None)[:][:]

        options.sort()
        options.sort(key=lambda x: x != 'Social distancing')
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
                parent = find(
                    lambda i:
                        i.entity_name == entity_name
                        and i.field == field
                        and i.subterm == option,
                    glossary_terms
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
                    parent = find(
                        lambda i:
                            i[0] == option and i[2] != 'N/A',
                        place_instances
                    )

                    # if a parent was found use its term as the group, otherwise
                    # specify "Other" as the group
                    if parent:
                        options_with_groups.append(
                            [option, parent[2]])
                    else:
                        continue
                        # # TODO figure out best way to handle "Other" cases
                        # options_with_groups.append([option, 'Other'])
            elif field == 'area2':
                for option in options:
                    # get group from glossary data
                    parent = find(
                        lambda i:
                            i[1] == option and i[2] != 'N/A',
                        place_instances
                    )

                    # if a parent was found use its term as the group, otherwise
                    # specify "Other" as the group
                    if parent:
                        options_with_groups.append([option, parent[0]])
                    else:
                        continue
                        # # TODO figure out best way to handle "Other" cases
                        # options_with_groups.append([option, 'Other'])

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

            elif field == 'date_issued':
                # return instances where `date_issued` falls within the
                # specified range, inclusive
                start = allowed_values[0]
                end = allowed_values[1]

                q = select(
                    i for i in q
                    if i.date_issued is not None
                    and i.date_issued <= end
                    and i.date_issued >= start
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
        elif join:
            q = select(
                i
                for i in q
                if i.place.filter(lambda x: getattr(x, field) in allowed_values)
            )

    # return the filtered query instance
    return q
