"""API utility functions"""
# standard modules
from datetime import datetime, date


def find(filter_func, i):
    result = None
    try:
        result = next(
            d for d in i
            if filter_func(d)
        )
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
    return datetime.strptime(s, '%Y-%m-%d').date()
