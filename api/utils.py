"""API utility functions"""
# standard modules
import functools
import pathlib
import urllib3
import certifi
import os
import requests
from datetime import datetime, date
from typing import Any, Callable, Union

# 3rd party modules
from pony.orm.core import Multiset, SetInstance
from pony.orm.ormtypes import TrackedArray

USE_CACHING: bool = os.environ.get("USE_CACHING", "true") == "true"


def str_to_date(s: str):
    """Given the date string in format YYYY-MM-DD, return a date instance.

    Parameters
    ----------
    s : str
        YYYY-MM-DD

    Returns
    -------
    datetime.date

    """
    return datetime.strptime(s, "%Y-%m-%d").date()


def date_to_str(dt: date) -> str:
    """Returns date as YYYY-MM-DD string.

    Args:
        dt (date): The date.

    Returns:
        str: The string representation of the date YYYY-MM-DD.
    """
    return dt.strftime("%Y-%m-%d")


def download_file(
    download_url: str,
    fn: str = None,
    write_path: str = None,
    as_object: bool = True,
):
    """
    Download the PDF at the specified URL and either save it to disk or
    return the response object.

    """
    user_agent = "Mozilla/5.0"
    try:
        response = requests.get(
            download_url, allow_redirects=True, headers={"user-agent": "Mozilla/5.0"}
        )
        if response.status_code == 200:
            if as_object:
                return response
            else:
                open(write_path + fn, "wb").write(response.content)
                return True
    except Exception:
        return None
    else:
        print("Error when downloading PDF (404)")
        return False


def use_relpath(relpath: str, abspath: str) -> str:
    """Returns the absolute path to the relative path provided

    Args:
        relpath (str): The relative path
        abspath (str): The absolute path to the current file

    Returns:
        str: The absolute path to the relative path provided.
    """
    path = pathlib.Path(abspath).parent / relpath
    return path.absolute()


def cached(func: Callable):
    """Decorator that returns function output if previously generated, as
    indexed by the concatenated kwargs; otherwise, runs the function and stores
    the output in the cache indexed by the concatenated kwargs.

    Args:
        func (Callable): Any function

    Returns:
        Any: The function result, possibly from the cache.
    """
    cache: dict = {}

    @functools.wraps(func)
    def wrapper(*func_args, **kwargs):
        if USE_CACHING:
            random = kwargs.get("random", False)
            key = str(kwargs)
            if key in cache and not random:
                return cache[key]

            results = func(*func_args, **kwargs)
            if not random:
                cache[key] = results
            return results
        else:
            return func(*func_args, **kwargs)

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


def get_first(i: Union[set, list], default: Any = None, as_list: bool = False) -> Any:
    """Given a set or list, return the first element of that list if it has
    one, otherwise return the default.

    Args:
        i (Union[set, list]): The set or list

        default (Any, optional): The default value to return if the set or list
        has no elements. Defaults to None.

        as_list (bool, optional): If True, returns a list with the value,
        otherwise returns only the value. If value is None then an empty list
        is returned.

    Returns:
        Union[Any, List[Any]]: The first value of the set or list, or the
        default value if the set or list has no elements. If `as_list` is True,
        a list with one element is returned, but if the element is None then
        an empty list is returned.
    """
    v: Any = None
    if len(i) > 0:
        v = i[0]
    else:
        v = default

    if not as_list:
        return v
    else:
        if v is None:
            return list()
        else:
            return [v]


def is_listlike(obj: Any) -> bool:
    """Returns True if the object is listlike, False otherwise.

    Args:
        obj (Any): Any object

    Returns:
        bool: True if the object is listlike, False otherwise.
    """
    obj_type: Any = type(obj)
    return (obj_type in (set, list)) or issubclass(
        obj_type, (Multiset, TrackedArray, SetInstance)
    )


def get_today_datetime_stamp() -> str:
    today: datetime = datetime.today()
    return today.strftime("%Y-%m-%d %H:%M:%S %Z").strip()
