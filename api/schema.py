"""Define API data processing methods"""
# standard modules
import functools
import math
import itertools
# import pprint
from io import BytesIO
from datetime import datetime, date, timedelta
from collections import defaultdict

# 3rd party modules
import boto3
from pony.orm import db_session, select, get, commit, desc, count, raw_sql, concat, coalesce, exists, group_concat
from fastapi.responses import FileResponse, Response
from fuzzywuzzy import fuzz

# local modules
from .export import CovidPolicyExportPlugin
from .models import Policy, PolicyList, PolicyDict, PolicyStatus, PolicyStatusList, \
    Auth_Entity, Place, File, PlanList, ChallengeList, PolicyNumber, \
    PolicyNumberList
from .util import str_to_date, find, download_file
from db import db

# # Code optimization profiling
# import cProfile
# import pstats
# p = cProfile.Profile()

# constants
s3 = boto3.client('s3')
S3_BUCKET_NAME = 'covid-npi-policy-storage'

# # pretty printing: for printing JSON objects legibly
# pp = pprint.PrettyPrinter(indent=4)

# IMPLEMENTED_NO_RESTRICTIONS = False


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
def get_policy_number(
    filters: dict = None,
    fields: list = None,
    order_by_field: str = 'date_start_effective',
    return_db_instances: bool = False,
    by_category: str = None,
    ordering: list = [],
    page: int = None,
    pagesize: int = 100
):

    # return all fields?
    all = fields is None

    # use pagination if all fields are requested, and set value for `page` if
    # none was provided in the URL query args
    use_pagination = (all or page is not None) and not return_db_instances
    if use_pagination and (page is None or page == 0):
        page = 1

    # get base query
    entity_class = db.Policy_Number
    q = select(i for i in entity_class)

    # apply filters if any
    if filters is not None:
        q = apply_entity_filters(q, entity_class, filters)

    # apply ordering
    ordering.reverse()
    for field_tmp, direction in ordering:
        if 'place.' in field_tmp:
            field = field_tmp.split('.')[1]
            if direction == 'desc':
                q = q.order_by(
                    lambda i: desc(
                        group_concat(getattr(p, field) for p in i.place)
                    )
                )
            else:
                q = q.order_by(
                    lambda i:
                        group_concat(getattr(p, field) for p in i.place)

                )
        else:
            field = field_tmp
            if direction == 'desc':
                q = q.order_by(desc(getattr(entity_class, field)))
            else:
                q = q.order_by(getattr(entity_class, field))

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
            return_fields_by_entity['policy_number'] = fields

        # TODO dynamically set fields returned for Place and other
        # linked entities
        return_fields_by_entity['place'] = [
            'id', 'level', 'loc'
        ]
        return_fields_by_entity['auth_entity'] = [
            'id', 'place', 'office', 'name'
        ]

        # define list of instances to return
        data = []
        # for each policy
        for d in q:
            # convert it to a dictionary returning only the specified fields
            d_dict = d.to_dict_2(
                with_collections=True,
                related_objects=True,
                return_fields_by_entity=return_fields_by_entity)
            print(d_dict)
            # add it to the output list
            # policies = list()
            # for p in d_dict['policies']:
            #     policy = db.Policy[p]
            #     policies.append(
            #         Policy(
            #             id=policy.id,
            #             primary_ph_measure=p['primary_ph_measure'],
            #             ph_measure_details=p['ph_measure_details'],
            #             date_start_effective=p['date_start_effective']
            #         )
            #     )
            datum = PolicyNumber(
                policy_number=d_dict['id'],
                titles=d_dict['names'],
                auth_entity_offices=[ae.office for ae in d_dict['auth_entity']],
                policies=[
                    Policy(
                        id=p.id,
                        primary_ph_measure=p.primary_ph_measure,
                        ph_measure_details=p.ph_measure_details,
                        date_start_effective=p.date_start_effective
                    ) for p in d_dict['policy']
                ]
            )
            data.append(datum)

        # if pagination is being used, get next page URL if there is one
        n_pages = None if not use_pagination else math.ceil(n / pagesize)
        more_pages = use_pagination and page < n_pages
        next_page_url = None if not more_pages else \
            f'''/get/policy_number?page={str(page + 1)}&pagesize={str(pagesize)}'''

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
            #     message=f'''{len(q)} policies found''',
            #     next_page_url=next_page_url,
            #     n=n
            # )
        else:
            # create response from output list
            res = PolicyNumberList(
                data=data,
                success=True,
                message=f'''{len(q)} policy numbers found''',
                next_page_url=next_page_url,
                n=n
            )
        return res


@db_session
@cached
def get_policy(
    filters: dict = None,
    fields: list = None,
    order_by_field: str = 'date_start_effective',
    return_db_instances: bool = False,
    by_category: str = None,
    ordering: list = [],
    page: int = None,
    pagesize: int = 100
):
    """Returns Policy instance data that match the provided filters.

    Parameters
    ----------
    filters : dict
        Dictionary of filters to be applied to policy data (see function
        `apply_entity_filters` below).
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
    use_pagination = (all or page is not None) and not return_db_instances
    if use_pagination and (page is None or page == 0):
        page = 1
    q = select(i for i in db.Policy)

    # apply filters if any
    if filters is not None:
        q = apply_entity_filters(q, db.Policy, filters)

    # apply ordering
    ordering.reverse()
    for field_tmp, direction in ordering:
        if 'place.' in field_tmp:
            field = field_tmp.split('.')[1]
            if direction == 'desc':
                q = q.order_by(
                    lambda i: desc(
                        group_concat(getattr(p, field) for p in i.place)
                    )
                )
            else:
                q = q.order_by(
                    lambda i:
                        group_concat(getattr(p, field) for p in i.place)

                )
        else:
            field = field_tmp
            if direction == 'desc':
                q = q.order_by(desc(getattr(db.Policy, field)))
            else:
                q = q.order_by(getattr(db.Policy, field))

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
            'id', 'level', 'loc'
        ]
        return_fields_by_entity['auth_entity'] = [
            'id', 'place', 'office', 'name', 'official'
        ]

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
                next_page_url=next_page_url,
                n=n
            )
        else:
            # create response from output list
            res = PolicyList(
                data=data,
                success=True,
                message=f'''{len(q)} policies found''',
                next_page_url=next_page_url,
                n=n
            )
        return res


@db_session
@cached
def get_challenge(
    filters: dict = None,
    fields: list = None,
    order_by_field: str = 'date_of_complaint',
    return_db_instances: bool = False,
    by_category: str = None,
    ordering: list = [],
    page: int = None,
    pagesize: int = 100
):
    """Returns Challenge instance data that match the provided filters.

    Parameters
    ----------
    filters : dict
        Dictionary of filters to be applied to data (see function
        `apply_entity_filters` below).
    fields : list
        List of instance fields that should be returned. If None, then
        all fields are returned.
    order_by_field : type
        String defining the field in the class that is used to
        order the policies returned.
    return_db_instances : bool
        If true, returns the PonyORM database query object containing the
        filtered instances, otherwise returns the list of dictionaries
        containing the instance data as part of a response dictionary

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
    use_pagination = (all or page is not None) and not return_db_instances
    if use_pagination and (page is None or page == 0):
        page = 1
    q = select(i for i in db.Court_Challenge)

    # apply filters if any
    if filters is not None:
        q = apply_entity_filters(q, db.Court_Challenge, filters)

    # apply ordering
    ordering.reverse()
    for field_tmp, direction in ordering:
        if 'place.' in field_tmp:
            field = field_tmp.split('.')[1]
            if direction == 'desc':
                q = q.order_by(
                    lambda i: desc(
                        group_concat(getattr(p, field) for p in i.place)
                    )
                )
            else:
                q = q.order_by(
                    lambda i:
                        group_concat(getattr(p, field) for p in i.place)

                )
        else:
            field = field_tmp
            if direction == 'desc':
                q = q.order_by(raw_sql(f'''i.{field} DESC NULLS LAST'''))
            else:
                q = q.order_by(raw_sql(f'''i.{field} NULLS LAST'''))

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
        return_fields_by_entity['court_challenge'] = fields

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
            f'''/get/challenge?page={str(page + 1)}&pagesize={str(pagesize)}'''

        # if by category: transform data to organize by category
        # NOTE: assumes one `primary_ph_measure` per Court_Challenge
        if by_category is not None:
            return []
            # data_by_category = defaultdict(list)
            # for i in data:
            #     data_by_category[i[by_category]].append(i)
            #
            # res = PolicyDict(
            #     data=data_by_category,
            #     success=True,
            #     message=f'''{len(q)} challenges found''',
            #     next_page_url=next_page_url,
            #     n=n
            # )
        else:
            # create response from output list
            res = ChallengeList(
                data=data,
                success=True,
                message=f'''{len(q)} challenges found''',
                next_page_url=next_page_url,
                n=n
            )
        return res


@db_session
@cached
def get_plan(
    filters: dict = None,
    ordering: list = [],
    fields: list = None,
    order_by_field: str = 'date_issued',
    return_db_instances: bool = False,
    by_category: str = None,
    page: int = None,
    pagesize: int = 100
):
    """Returns Plan instance data that match the provided filters.

    Parameters
    ----------
    filters : dict
        Dictionary of filters to be applied to plan data (see function
        `apply_entity_filters` below).
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

    # use pagination if all fields are requested, and set value for `page` if
    # none was provided in the URL query args
    use_pagination = (all or page is not None) and not return_db_instances
    if use_pagination and (page is None or page == 0):
        page = 1
    q = select(i for i in db.Plan)

    # apply filters if any
    if filters is not None:
        q = apply_entity_filters(q, db.Plan, filters)

    # apply ordering
    ordering.reverse()
    for field_tmp, direction in ordering:
        if 'place.' in field_tmp:
            field = field_tmp.split('.')[1]
            if direction == 'desc':
                q = q.order_by(
                    lambda i: desc(str(getattr(i.place, field)))
                )
            else:
                q = q.order_by(
                    lambda i: str(getattr(i.place, field))
                )
        else:
            field = field_tmp
            if direction == 'desc':
                q = q.order_by(desc(getattr(db.Plan, field)))
            else:
                q = q.order_by(getattr(db.Plan, field))

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

        # if pagination is being used, get next page URL if there is one
        n_pages = None if not use_pagination else math.ceil(n / pagesize)
        more_pages = use_pagination and page < n_pages
        next_page_url = None if not more_pages else \
            f'''/get/policy?page={str(page + 1)}&pagesize={str(pagesize)}'''

        # if by category: transform data to organize by category
        # NOTE: assumes one `primary_ph_measure` per Court_Challenge
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
                message=f'''{len(q)} plans found''',
                next_page_url=next_page_url,
                n=n
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
            q = apply_entity_filters(q, db.Policy, filters)

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


# def parse_lockdown_level(value_tmp):
#     """If "No restrictions" has not yet been implemented on the frontend, then
#     return "new normal" instead of "no restrictions". Otherwise return the val.
#
#     Parameters
#     ----------
#     value_tmp : type
#         Description of parameter `value_tmp`.
#
#     Returns
#     -------
#     type
#         Description of returned object.
#
#     """
#     if not IMPLEMENTED_NO_RESTRICTIONS:
#         if value_tmp == 'No restrictions':
#             return 'New normal'
#         else:
#             return value_tmp


@cached
@db_session
def get_lockdown_level(
    geo_res: str = None,
    iso3: str = None,
    name: str = None,
    date: str = None,
    end_date: str = None,
    deltas_only: bool = False,
):
    """TODO"""

    # if date is not provided, return it in the response
    specify_date = date is None

    # collate list of lockdown level statuses based on state / province
    data = list()

    # RETURN MOST RECENT OBSERVATION FOR EACH PLACE
    distinct_clause = 'distinct on (place)' if end_date is None else \
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
                # else:
                #     datum['place_name'] = i.place.iso3
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

    # if only the deltas are needed, return one datum representing the date
    # each different distancing level was entered, instead of all dates
    message_noun = 'statuses'
    if deltas_only:

        # create list to hold output
        deltas_only_data = list()

        # Get iter funcs; assumes sorted by desc. datestamp
        by_level = itertools.groupby(data, key=lambda x: x['value'])

        # Create iterator to iterate over groups of dates that had a given
        # continous distancing level and keep only the oldest date
        for level, items in by_level:
            items = list(items)
            deltas_only_data.append(items[len(items) - 1])
        data = deltas_only_data
        message_noun = 'status changes'

    # create response from output list
    message_name = None
    if name is not None:
        message_name = name
    elif iso3 is not None:
        message_name = iso3

    res = PolicyStatusList(
        data=data,
        success=True,
        message=f'''Found {str(len(data))} {message_noun}{'' if message_name is None else ' for ' + message_name}'''
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
    glossary_terms = select(i for i in db.Glossary)[:][:] if need_glossary_terms \
        else list()

    # check places relevant only for the entity of `class_name`
    class_name_field = 'policies' if class_name == 'Policy' \
        else 'plans'

    # get all places if needed
    need_places = any(d_str in fields_using_geo_groups for d_str in fields)
    place_instances = select(
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

        if isinstance(options[0], list):
            options = list(set([item for sublist in options for item in sublist]))

        options.sort()
        options.sort(key=lambda x: x != 'Social distancing')
        options.sort(key=lambda x: x == 'Other')
        options.sort(key=lambda x: x in ('Unspecified', 'Local'))

        # skip blank strings
        options = list(filter(lambda x: x.strip() != '', options))

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

        if d_str == 'Court_Challenge.government_order_upheld_or_enjoined':
            data['government_order_upheld_or_enjoined'].append(
                {'id': -1, 'value': 'Pending', 'label': 'Pending'},
            )

    # # Disable profiling
    # p.disable()
    #
    # # Dump the stats to a file
    # p.dump_stats("res_focus.prof")
    # p2 = pstats.Stats('res_focus.prof')
    # p2.sort_stats('cumulative').print_stats(10)

    # return all optionset values

    # apply special ordering
    if 'ph_measure_details' in data:
        data['ph_measure_details'].sort(key=lambda x: 'other' in x['value'].lower())

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
def apply_entity_filters(q, entity_class, filters: dict = dict()):
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

        # custom text search with fuzzy matching
        if field == '_text':
            text = allowed_values[0].lower()
            thresh = 80
            new_q_ids = []
            q_search_text_only = select((i.id, i.search_text) for i in q)

            for id, search_text in q_search_text_only:
                if search_text is None:
                    continue
                else:
                    # return exact match - if no exact match return partial match
                    exact_match = text in search_text
                    if exact_match:
                        new_q_ids.append(id)
                    else:
                        ratio = fuzz.partial_ratio(
                            text, search_text)
                        partial_match = ratio >= thresh
                        if partial_match:
                            new_q_ids.append(id)
            q = select(
                i
                for i in entity_class
                if i.id in new_q_ids
            )

            # # Text match with direct case insensitive matches only
            # q = select(
            #     i for i in q
            #     if text in i.search_text
            # )
            continue

        # Complaint category field needs to be handled separately
        # because the field contains arrays instead of strings
        if field == 'complaint_category':

            for value in allowed_values:
                print(allowed_values)
                q = select(
                    i for i in q if value in i.complaint_category
                )

            continue

        if field == 'government_order_upheld_or_enjoined':
            if 'Pending' in allowed_values:
                q = select(
                    i
                    for i in q
                    if getattr(i, field) in allowed_values
                    or getattr(i, field) == ''
                )

            else:
                q = select(
                    i
                    for i in q
                    if getattr(i, field) in allowed_values
                )

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

            if field == 'date_of_decision':
                # return instances where `date_of_decision` falls within the
                # specified range, inclusive
                start = allowed_values[0]
                end = allowed_values[1]

                q = select(
                    i for i in q
                    if i.date_of_decision is not None
                    and i.date_of_decision <= end
                    and i.date_of_decision >= start
                )
                continue

            if field == 'date_of_complaint':
                # return instances where `date_of_complaint` falls within the
                # specified range, inclusive
                start = allowed_values[0]
                end = allowed_values[1]

                q = select(
                    i for i in q
                    if i.date_of_complaint is not None
                    and i.date_of_complaint <= end
                    and i.date_of_complaint >= start
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
        # TODO generalize this and rename function `apply_entity_filters`
        join_place = field in ('level', 'loc', 'area1',
                               'iso3', 'country_name', 'area2')

        join_policy = not join_place and field in ('policy.policy_number',)

        # if filter is a join, apply the filter to the linked entity
        # joined to place entity
        if join_place:
            q = q.filter(
                lambda i:
                    exists(
                        t for t in i.place
                        if getattr(t, field) in allowed_values
                    )
            )
        # joined to policy entity
        elif join_policy:
            q = q.filter(
                lambda i:
                    exists(
                        t for t in i.policies
                        if t.policy_number in allowed_values
                    )
            )
        else:
            # if the filter is not a join, i.e., is on policy native fields
            q = select(
                i
                for i in q
                if getattr(i, field) in allowed_values
            )

    # return the filtered query instance
    return q


@db_session
def get_policy_search_text(i):
    """Given Policy instance `i`, returns the search text string that should
    be checked against by plain text search.

    Parameters
    ----------
    i : type
        Description of parameter `i`.

    Returns
    -------
    type
        Description of returned object.

    """

    # Define fields on entity class to concatenate
    fields_by_type = [
        {
            'type': str,
            'fields': [
                'policy_name',
                'desc',
                'primary_ph_measure',
                'ph_measure_details',
                'subtarget',
                'relaxing_or_restricting',
                'authority_name',
            ]
        },
        {
            'type': list,
            'fields': [
                'primary_impact',
            ]
        },
    ]

    # Define the same but for linked entities
    linked_fields_by_type = [
        {
            'linked_field': 'place',
            'linked_type': list,
            'type': str,
            'fields': [
                'level',
                'loc',
                # 'country_name',
                # 'area1',
                # 'area2',
            ]
        }
    ]

    return get_search_text(i, fields_by_type, linked_fields_by_type)


@db_session
def get_plan_search_text(i):
    """Given Plan instance `i`, returns the search text string that should
    be checked against by plain text search.

    Parameters
    ----------
    i : type
        Description of parameter `i`.

    Returns
    -------
    type
        Description of returned object.

    """

    # Define fields on entity class to concatenate
    fields_by_type = [
        {
            'type': str,
            'fields': [
                'name',
                'desc',
                'primary_loc',
                'org_name',
                'org_type',
            ]
        },
        {
            'type': list,
            'fields': [
                'reqs_essential',
                'reqs_private',
                'reqs_school',
                'reqs_social',
                'reqs_hospital',
                'reqs_public',
                'reqs_other',
            ]
        },
    ]

    # Define the same but for linked entities
    linked_fields_by_type = [
        {
            'linked_field': 'place',
            'linked_type': list,
            'type': str,
            'fields': [
                'level',
                'loc',
            ]
        }
    ]

    return get_search_text(i, fields_by_type, linked_fields_by_type)


@db_session
def get_challenge_search_text(i):
    """Given Court_Challenge instance `i`, returns the search text string that
    should be checked against by plain text search.

    """

    # Define fields on entity class to concatenate
    fields_by_type = [
        {
            'type': str,
            'fields': [
                'jurisdiction',
                'court',
                'legal_authority_challenged',
                'parties',
                'case_number',
                'legal_citation',
                'filed_in_state_or_federal_court',
                'summary_of_action',
                'case_name',
                'procedural_history',
                'holding',
                'government_order_upheld_or_enjoined',
                'subsequent_action_or_current_status',
                'did_doj_file_statement_of_interest',
                'summary_of_doj_statement_of_interest',
                'data_source_for_complaint',
                'data_source_for_decision',
                'data_source_for_doj_statement_of_interest',
                'policy_or_law_name',
                'source_id',
                'search_text'
            ]
        },
        {
            'type': list,
            'fields': [
                'complaint_category',
            ]
        },
    ]

    # Define the same but for linked entities
    linked_fields_by_type = [
        {
            'linked_field': 'policies',
            'linked_type': list,
            'type': str,
            'fields': [
                'policy_name',
            ]
        }
    ]

    return get_search_text(i, fields_by_type, linked_fields_by_type)


def get_search_text(i, fields_by_type, linked_fields_by_type):
    """Short summary.

    Parameters
    ----------
    i : type
        Description of parameter `i`.
    fields_by_type : type
        Description of parameter `fields_by_type`.
    linked_fields_by_type : type
        Description of parameter `linked_fields_by_type`.

    Returns
    -------
    type
        Description of returned object.

    """
    # for each field on the entity class, concatenate it to the search text
    search_text_list = list()
    for field_group in fields_by_type:
        field_type = field_group['type']
        # string type fields are concatenated directly
        if field_type == str:
            for field in field_group['fields']:
                value = getattr(i, field)
                if value is not None:
                    search_text_list.append(value.lower())

        # list type fields - each element concatenated
        elif field_type == list:
            for field in field_group['fields']:
                for d in getattr(i, field):
                    if d is not None:
                        search_text_list.append(d.lower())

    # for each linked entity field, do the same
    for field_group in linked_fields_by_type:
        field_type = field_group['type']
        linked_type = field_group['linked_type']
        linked_field = field_group['linked_field']

        # string type fields are concatenated directly
        if linked_type == list:
            linked_instances = getattr(i, linked_field)
            if linked_instances is not None and len(linked_instances) > 0:
                for linked_instance in linked_instances:
                    if field_type == str:
                        for field in field_group['fields']:
                            value = getattr(linked_instance, field)
                            if value is not None:
                                search_text_list.append(
                                    value.lower()
                                )

    # return joined text string
    search_text = ' - '.join(search_text_list)
    return search_text


@db_session
def add_search_text():
    """Add searchable text strings to instances.

    """
    for i in db.Policy.select():
        i.search_text = get_policy_search_text(i)
    for i in db.Plan.select():
        i.search_text = get_plan_search_text(i)
    for i in db.Court_Challenge.select():
        i.search_text = get_challenge_search_text(i)
