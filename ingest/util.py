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


def us_caseload_csv_to_dict(download_url: str):

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
