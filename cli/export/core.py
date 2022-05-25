import shutil
from typing import Union
from datetime import date

import click
from fastapi.responses import Response


@click.command(help="Download the full dataset and summary dataset as Excel files")
@click.option(
    "--filename",
    "-f",
    default=None,
    show_default=True,
    type=str,
    help="Filename at which to save the Excel workbook.",
)
@click.option(
    "--brief",
    "-b",
    default=False,
    show_default=True,
    is_flag=True,
    help="If flag is set, brief summary version of dataset is downloaded which has"
    " limited data columns.",
)
@click.option(
    "--save-static",
    "-s",
    is_flag=True,
    help="If this flag is specified, the Excel file will be saved in the API server"
    " code's static file repository and served from endpoint"
    " `GET /export/static`, which is used by the main COVID AMP site and"
    " ghssidea.org to obtain the full dataset rapidly (i.e., without needing to"
    " generate it).\n\nThis cannot be used with `--filename`.",
)
def export(filename: Union[str, None], brief: bool, save_static: bool):
    from api import schema
    from db import db

    if filename is not None and save_static:
        raise ValueError("Cannot specify `--filename` if using `--save-static`")

    db.generate_mapping(create_tables=False)
    excel_response: Response = schema.export(
        filters=dict(),
        class_name="All_data_recreate" if not brief else "All_data_recreate_summary",
    )
    if excel_response is None:
        raise ValueError(
            "Something went wrong when preparing the Excel sheet response. Please"
            " contact a developer for assistance."
        )

    if filename is not None:
        with open(filename, "wb") as f:
            f.write(excel_response.body)

    if save_static:
        STATIC_EXCEL_FN_BASE: str = "staticfull" if not brief else "staticsummary"
        STATIC_EXCEL_FN: str = STATIC_EXCEL_FN_BASE + ".xlsx"
        STATIC_EXCEL_DIR: str = "api/export/static"
        backup_excel_fn: str = (
            f"""{STATIC_EXCEL_FN_BASE}_{date.today().strftime("%Y%m%d")}.xlsx"""
        )
        STATIC_EXCEL_PATH: str = f"{STATIC_EXCEL_DIR}/{STATIC_EXCEL_FN}"

        # replace api/methods/excelexport/data/static*.xlsx
        with open(STATIC_EXCEL_PATH, "wb") as f:
            f.write(excel_response.body)

        # make backup copy
        shutil.copy(
            STATIC_EXCEL_PATH,
            f"{STATIC_EXCEL_DIR}/old/{backup_excel_fn}",
        )
