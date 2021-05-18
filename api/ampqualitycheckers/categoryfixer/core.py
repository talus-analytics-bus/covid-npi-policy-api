import logging
from pony.orm.core import db_session, select
from db.models import Glossary
from typing import Dict, Iterable, cast
import pandas as pd


class CategoryFixer:
    @db_session
    def __init__(self) -> None:
        """Initialize a new AMP policy category fixer."""

        # Get map of subcategories to categories as dictionary
        self.subcat_to_cat: Dict[str, str] = dict()
        GlossaryTable: Iterable[Glossary] = cast(Iterable[Glossary], Glossary)
        cat: str = None
        subcat: str = None
        for cat, subcat in select(
            (i.term, i.subterm)
            for i in GlossaryTable
            if i.field == "ph_measure_details"
        ):
            self.subcat_to_cat[subcat] = cat
        return None

    def fix(self, df: pd.DataFrame) -> pd.DataFrame:
        """Given a data frame containing columns for `primary_ph_measure`
        (category) and `ph_measure_details` (subcategory), sets the category to
        correspond to the subcategory, or logs an error if an unknown
        subcategory is encountered.

        Args:
            df (pd.DataFrame): The data frame of policies.

        Returns:
            pd.DataFrame: The updated data frame with fixed categories.
        """

        self.n_corrected: int = 0

        # For each data frame row, perform category correction if needed
        df = df.apply(self.__fix_category, axis="columns")

        # Log the number of corrections made if any
        if self.n_corrected > 0:
            logging.warning(
                "Corrected this many categories which were incorrect for "
                "their sub-categories: " + str(self.n_corrected)
            )

        # Return the updated data frame
        return df

    def __fix_category(self, row: pd.Series) -> pd.Series:
        """Given a row (series) from a data frame, fix row's category if it is
        incorrect for the defined sub-category in the row, per the glossary
        data.

        Args:
            row (pd.Series): The data frame row.

        Returns:
            pd.Series: The (potentially) modified data frame row.
        """
        # Get the correct category for the subcategory
        subcat: str = row["ph_measure_details"]
        correct_cat: str = self.subcat_to_cat.get(subcat, None)
        if correct_cat is not None:
            orig_cat: str = row["primary_ph_measure"]
            if orig_cat != correct_cat:
                logging.warning(
                    f"""Incorrect category "{orig_cat}" was assigned for """
                    f"""sub-category "{subcat}"; """
                    f'''correct one is "{correct_cat}"'''
                )
                row["primary_ph_measure"] = correct_cat
                self.n_corrected = self.n_corrected + 1

        else:
            logging.error(
                f"""No category found for sub-category "{subcat}" """
                f"""for policy with ID = {row['id']}"""
            )
        return row
