import functools

from db import db
from pony.orm.core import BindingError

def db_mapping(func):
    """Wrapper to generate database mapping if not already done yet.

    TODO use this in various CLI functions where the below implementation
    is currently explicitly written.

    Args:
        func (Callabe): Wrapped

    Returns:
        Callable: Wrapper 
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            db.generate_mapping(create_tables=False)
        except BindingError:
            pass
        return func(*args, **kwargs)
    return wrapper