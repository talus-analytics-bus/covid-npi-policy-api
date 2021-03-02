"""API utility functions"""
# standard modules
from datetime import datetime, date
import pathlib
import urllib3
import certifi


def find(filter_func, i):
    result = None
    try:
        result = next(d for d in i if filter_func(d))
    except:
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
    except Exception as e:
        return None
    else:
        print("Error when downloading PDF (404)")
        return False


def use_relpath(relpath: str, abspath: str) -> str:
    path = pathlib.Path(abspath).parent / relpath
    return path.absolute()
