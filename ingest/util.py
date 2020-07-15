"""Ingest utility methods"""
# standard packages
import urllib3
import certifi
import requests
import csv
import json
from collections import defaultdict

# 3rd party modules
from pony.orm import db_session, commit, get, select
from pony.orm.core import EntityMeta
import pprint

# constants
pp = pprint.PrettyPrinter(indent=4)

# define colors for printing colorized terminal text


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


special_fields = ('home_rule', 'dillons_rule')


@db_session
def upsert(cls, get: dict, set: dict = None, skip: list = []):
    """Insert or update record into specified class based on checking for
    existence with dictionary data field map `get`, and creating with
    data based on values in dictionaries `get` and `set`, skipping any
    data fields defined in `skip`.

    Parameters
    ----------
    cls : type
        Description of parameter `cls`.
    get : dict
        Description of parameter `get`.
    set : dict
        Description of parameter `set`.
    skip : list
        Description of parameter `skip`.

    Returns
    -------
    type
        Description of returned object.

    """
    # does the object exist
    assert isinstance(
        cls, EntityMeta), "{cls} is not a database entity".format(cls=cls)

    # if no set dictionary has been specified
    set = set or {}

    if not cls.exists(**get):
        # make new object
        return ('insert', cls(**set, **get))
    else:
        # get the existing object
        obj = cls.get(**get)
        action = 'none'
        for key, value in set.items():
            if key in skip:
                continue
            true_update = str(value).strip() != str(getattr(obj, key)).strip() \
                and value != getattr(obj, key)
            if true_update:
                action = 'update'
                # print('\nUpdated: value was ' + str(key) +
                #       ' = ' + str(getattr(obj, key)))
                # print('--changed to ' + str(key) + ' = ' + str(value))
                # print(cls)
                # print(get['field'])
            # special cases
            if key in special_fields:
                cur_val = getattr(obj, key)
                if cur_val != '' and cur_val is not None:
                    continue
            obj.__setattr__(key, value)

        commit()
        return (action, obj)


def download_file(
    download_url: str, fn: str = None, write_path: str = None, as_object: bool = True
):
    """Download the PDF at the specified URL and either save it to disk or
    return it as a byte stream.

    Parameters
    ----------
    download_url : type
        Description of parameter `download_url`.
    fn : type
        Description of parameter `fn`.
    write_path : type
        Description of parameter `write_path`.
    as_object : type
        Description of parameter `as_object`.

    Returns
    -------
    type
        Description of returned object.

    """
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


def nyt_caseload_csv_to_dict(download_url: str):

    output = defaultdict(list)

    r = requests.get(download_url, allow_redirects=True)
    file_dict = defaultdict(list)
    rows = r.iter_lines(decode_unicode=True)

    # remove the header row from the generator
    next(rows)

    for row in rows:
        row_list = row.split(',')

        file_dict[row_list[1]].append(row_list)

    for state, data in file_dict.items():
        for day in data:
            output[day[1]].append({
                'date':  day[0],
                'state':  day[1],
                'fips':  day[2],
                'cases':  day[3],
                'deaths':  day[4],
            })
    return output


@db_session
def jhu_caseload_csv_to_dict(download_url: str, db):

    output = defaultdict(list)

    r = requests.get(download_url, allow_redirects=True)
    file_dict = defaultdict(list)
    rows = r.iter_lines(decode_unicode=True)

    # remove the header row from the generator
    headers_raw = next(rows).split(',')
    dates_raw = headers_raw[4:]
    dates = list()
    for d in dates_raw:
        date_parts = d.split('/')
        mm = date_parts[0] if len(date_parts[0]) == 2 else \
            '0' + date_parts[0]
        dd = date_parts[1] if len(date_parts[1]) == 2 else \
            '0' + date_parts[1]
        yyyy = date_parts[2] if len(date_parts[2]) == 4 else \
            '20' + date_parts[2]
        date_str = yyyy + '-' + mm + '-' + dd
        dates.append(date_str)

    headers = headers_raw[0:4] + dates
    skip = ('Lat', 'Long', 'Province/State')

    row_lists = list()
    for row in rows:
        row_list = row.split(',')

        row_lists.append(row_list)

    missing_names = set()
    data = list()
    for row_list in row_lists:
        if row_list[0] != '':
            continue

        datum = dict()
        idx = 0
        for header in headers:
            if header in skip:
                idx += 1
                continue
            else:
                if header == 'Country/Region':
                    datum['name'] = row_list[idx] \
                        .replace('"', '') \
                        .replace('*', '')

                else:
                    datum[header] = int(float(row_list[idx]))
                idx += 1

        # get place ISO, ID from name
        p = select(
            i for i in db.Place
            if i.name == datum['name']
            or datum['name'] in i.other_names
        ).first()
        if p is None:
            print('ERROR: No place found for ' + datum['name'])
            missing_names.add(datum['name'])
            continue
        else:
            datum['place'] = p

        # reshape again
        for date in dates:
            datum_final = dict()
            datum_final['date'] = date
            datum_final['value'] = datum[date]
            datum_final['place'] = datum['place']
            data.append(datum_final)

    print('\nmissing_names')
    pp.pprint(missing_names)
    # input('Press enter to continue.')

    print('\ndata')
    pp.pprint(data)
    # input('Press enter to continue.')

    # return output
    return data
