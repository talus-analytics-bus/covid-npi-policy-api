# Common tasks for extending and maintaining code
Notes on common tasks for extending and maintaining code follow.

## Edit fields shown in Excel exports
The instructions below apply to adding Excel file columns for policy data. A similar procedure should be used if columns need to be added for plan data, opening the file [`api/export/planexport.py`](./api/export/planexport.py) in step 1 instead.
1. Open [`api/export/policyexport.py`](./api/export/policyexport.py)
1. To variable `export_fields` add the entity and field that contains the data you would like to add as an Excel column, e.g., `Policy.id`
1. To variable `instances` add a SQLAlchemy field expression in the same index as the element you added to `export_fields` that represents the data, e.g., `i.id` (`i` represents a Policy instance)
1. If you added an element to `instances` **before** the element `i.date_issued`, then update line 126 to `order_by` the index that corresponds to `i.date_issued`
1. Open [`api/export/plugins.py`](./api/export/plugins.py)
1. Review the code block beginning at line 337. This block transforms raw instances from the PostgreSQL database into content for the Excel file's rows. Ensure the logic in the following lines handles the new column values as appropriate. Note that here, `instances` and `export_fields` contain the same data as in the [`api/export/policyexport.py`](./api/export/policyexport.py) file you edited.
1. Generate the Excel file using CLI command `python -m amp export -f testnewfile.xlsx` which will create the file `testnewfile.xlsx` in the root directory of the repository. Open an review the file to ensure the new column(s) appear as intended.

## Add data fields to the database and SQLAlchemy models
1. Add the column desired to the table `policy`
1. Update the Airtable data dictionary to map the column name to the Airtable field name here: https://airtable.com/appoXaOlIgpiHK3I2/tbllRN8eltGNBbSpf?blocks=hide
1. Open [db/models.py](./db/models.py)
1. Add the field to the appropriate entity, e.g., line 88 for policy data, following patterns already used for other fields
1. Use the newly added data field in SQLAlchemy queries as needed