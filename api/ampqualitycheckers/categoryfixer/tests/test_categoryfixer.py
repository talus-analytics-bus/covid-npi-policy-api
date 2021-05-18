import pandas as pd
from db import db
from ..core import CategoryFixer
from ..helpers import debug_df, debug_df_expected


def test_categoryfixer():
    db.generate_mapping(create_tables=False)
    category_fixer: CategoryFixer = CategoryFixer()
    fixed_df: pd.DataFrame = category_fixer.fix(debug_df)
    assert fixed_df.equals(debug_df_expected)
