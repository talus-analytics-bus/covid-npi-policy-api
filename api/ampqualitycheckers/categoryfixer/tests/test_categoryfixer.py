import pandas as pd
from pony.orm.core import BindingError
from db import db
from ..core import CategoryFixer
from ..helpers import debug_df, debug_df_expected


def test_categoryfixer():
    """Text the CategoryFixer class."""

    # Do PonyOrm binding, unless it has already been done by another test.
    try:
        db.generate_mapping(create_tables=False)
    except BindingError:
        pass

    # Perform tests
    category_fixer: CategoryFixer = CategoryFixer()
    fixed_df: pd.DataFrame = category_fixer.fix(debug_df)
    assert fixed_df.equals(debug_df_expected)
