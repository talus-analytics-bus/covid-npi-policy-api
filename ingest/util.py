"""Ingest utility methods"""
# standard packages
from typing import Any, Dict, List, Set
import urllib3
import certifi
import requests
from collections import defaultdict

# 3rd party modules
from pony.orm import db_session, commit, select
from pony.orm.core import Entity, EntityMeta
import pprint

# constants
pp = pprint.PrettyPrinter(indent=4)

# define colors for printing colorized terminal text


class bcolors:
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


special_fields = ("home_rule", "dillons_rule")


def find_all(i, filter_func):
    """Finds all instances in iterable `i` for which func `filter_func`
    returns True, returns emptylist otherwise.

    Parameters
    ----------
    i : type
        Description of parameter `i`.
    filter_func : type
        Description of parameter `filter_func`.

    Returns
    -------
    type
        Description of returned object.

    """
    return list(filter(filter_func, i))


# detect a null character in a string
def has_null(s: str):
    nulls = ("\x00", "0x00")
    return s in nulls


@db_session
def upsert(
    cls, get: dict, set: dict = None, skip: list = [], do_commit: bool = True
):
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
        cls, EntityMeta
    ), "{cls} is not a database entity".format(cls=cls)

    # if no set dictionary has been specified
    set = set or {}

    for k, v in set.items():
        if type(v) == str:
            set[k] = v.replace("\x00", "")

    if not cls.exists(**get):
        # make new object
        return ("insert", cls(**set, **get))
    else:
        # get the existing object
        obj = cls.get(**get)
        action = "none"
        for key, value in set.items():
            if key in skip:
                continue
            true_update = str(value).strip() != str(
                getattr(obj, key)
            ).strip() and value != getattr(obj, key)
            if true_update:
                action = "update"

            # special cases
            if key in special_fields:
                cur_val = getattr(obj, key)
                if cur_val != "" and cur_val is not None:
                    continue
            obj.__setattr__(key, value)

        if do_commit:
            commit()
        return (action, obj)


def download_file(
    download_url: str,
    fn: str = None,
    write_path: str = None,
    as_object: bool = True,
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
        cert_reqs="CERT_REQUIRED", ca_certs=certifi.where()
    )
    user_agent = "Mozilla/5.0"
    try:
        response = http.request(
            "GET", download_url, headers={"User-Agent": user_agent}
        )
        if response is not None and response.data is not None:
            if as_object:
                return response.data
            else:
                with open(write_path + fn, "wb") as out:
                    out.write(response.data)
                return True
    except Exception:
        return None
    else:
        print("Error when downloading PDF (404)")
        return False


def nyt_county_caseload_csv_to_dict(
    download_url: str, for_dates: Set[str] = None
) -> Dict[str, List[dict]]:
    """
    Download county-level COVID-19 case and death data from the New York Times
    GitHub and convert it from CSV format into a dictionary of lists, indexed
    by county FIPS code.

    TODO Add feature where only observations whose datetimes aren't already
    in the dataset are added. Perhaps by skipping them in the

    Args:
        download_url (str): The URL at which the county data CSV file is found

    Returns:
        List[dict]: A dictionary of lists of COVID-19 data observations,
        indexed by county FIPS code.
    """

    output = defaultdict(list)

    r = requests.get(download_url, allow_redirects=True)
    file_dict = defaultdict(list)
    rows = r.iter_lines(decode_unicode=True)

    # define index of unique ID element
    UNIQUE_ID_IDX: int = 3

    # remove the header row from the generator
    next(rows)

    for row in rows:
        row_list = row.split(",")

        file_dict[row_list[UNIQUE_ID_IDX]].append(row_list)

    for _county_name, data in file_dict.items():
        for day in data:
            # skip unless matches date if provided
            if for_dates is not None and day[0] not in for_dates:
                continue
            else:
                fips_no_zeros: str = get_fips_no_zeros(day[UNIQUE_ID_IDX])
                output[fips_no_zeros].append(
                    {
                        "date": day[0],
                        "county": day[1],
                        "fips": fips_no_zeros,
                        "cases": day[4],
                        "deaths": day[5],
                    }
                )
    return output


def get_fips_no_zeros(raw_fips: str) -> str:
    """Given a raw numeric FIPS code in string format, remove any leading zeros
    and return the string representation of that integer.

    Args:
        raw_fips (str): A numeric FIPS code as a string, e.g., `01001`.

    Returns:
        str: That FIPS code without leading zeros, e.g., `1001`.
    """
    if raw_fips == "":
        return ""
    else:
        return str(int(raw_fips))


def nyt_caseload_csv_to_dict(download_url: str):

    output = defaultdict(list)

    r = requests.get(download_url, allow_redirects=True)
    file_dict = defaultdict(list)
    rows = r.iter_lines(decode_unicode=True)

    # remove the header row from the generator
    next(rows)

    for row in rows:
        row_list = row.split(",")

        file_dict[row_list[1]].append(row_list)

    for state, data in file_dict.items():
        for day in data:
            output[day[1]].append(
                {
                    "date": day[0],
                    "state": day[1],
                    "fips": day[2],
                    "cases": day[3],
                    "deaths": day[4],
                }
            )
    return output


def get_inst_by_col(e: Entity, c: str) -> Dict[Any, Entity]:
    """Returns all instances in the database of the given entity indexed in a
    dictionary by the given column.

    Args:
        e (Entity): Entity whose instances will be returned.
        c (str): Name of column by which instances will be indexed.

    Returns:
        Dict[Any, Entity]: Dictionary of instances in database indexed by a
        particular column's values.
    """
    # get all instances from database
    inst: List[Entity] = select(i for i in e)

    # return indexed by column value
    by_col: Dict[Any, Entity] = defaultdict(list)
    for i in inst:
        by_col[getattr(i, c)].append(i)
    return by_col
