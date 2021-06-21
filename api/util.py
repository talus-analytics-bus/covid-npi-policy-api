"""API utility functions"""
# standard modules
from datetime import datetime
import functools
import pathlib
from typing import Any, Union
import urllib3
import certifi
import os

USE_CACHING: bool = os.environ.get("USE_CACHING", "true") == "true"


def find(filter_func, i):
    result = None
    try:
        result = next(d for d in i if filter_func(d))
    except Exception:
        pass
    return result


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


def use_relpath(relpath: str, abspath: str) -> str:
    path = pathlib.Path(abspath).parent / relpath
    return path.absolute()


def cached(func):
    """Caching"""
    cache = {}

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


def get_first(
    i: Union[set, list], default: Any = None, as_list: bool = False
) -> Any:
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


def set_level_filters_from_geo_filters(filters: dict) -> None:
    """Given a set of filters for `get_policy`, return the set of filters with
    place level filters added based on existing geographic area filters, if any

    Args:
        filters (dict): The filters passed to method `schema.get_policy`
    """
    if filters is not None and (
        "level" not in filters or len(filters["level"]) == 0
    ):
        if "area2" in filters:
            filters["level"] = ["Local"]
        elif "area1" in filters:
            filters["level"] = "State / Province"
        elif "country_name" in filters:
            filters["level"] = "Country"
