"""Define data sources for ingesting data into databases"""
# standard packages
import os

# 3rd party packages for AirtableSource
from airtable import Airtable
import pprint

# Packages for GoogleSheetSource
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

# constants
pp = pprint.PrettyPrinter(indent=4)
__all__ = ['GoogleSheetSource']


class DataSource():
    def __init__(self, name: str):
        self.name = name


class AirtableSource(DataSource):
    def __init__(
        self,
        name: str,
        base_key: str,
        api_key: str
    ):
        DataSource.__init__(self, name)
        self.base_key = base_key
        self.api_key = api_key

    def connect(self):
        return self

    def workbook(self, key: str):
        print('WARNING: `workbook` method not implemented for AirtableSource.')
        return self

    def worksheet(self, name: str):
        try:
            ws = Airtable(self.base_key, table_name=name, api_key=self.api_key)
            self.ws = ws
            return self

        except Exception as e:
            print(e)
            print('Failed to open worksheet with name ' + str(name))

    def as_dataframe(self, header_row: int = 0, view: str = None):
        try:
            records_tmp = self.ws.get_all() if view is None else \
                self.ws.get_all(view=view)
            records = list()
            for r_tmp in records_tmp:
                r = r_tmp['fields']
                r['source_id'] = r_tmp['id']
                records.append(r)

            df = pd.DataFrame.from_records(records)

            print(f'''Found {len(df)} records in worksheet''')

            # remove NaN values
            df = df.replace(pd.np.nan, '', regex=True)
            print(df)
            return df
        except Exception as e:
            print(e)
            print('Failed to open worksheet')


class GoogleSheetSource(DataSource):
    def __init__(
        self,
        name: str,
        config_json_relpath: str
    ):
        DataSource.__init__(self, name)

    def connect(self):
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        cur_dir = os.path.dirname(os.path.abspath(__file__))
        config_file = os.path.join(cur_dir, 'config/googleKey.json')
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            config_file,
            scope
        )

        try:
            self.client = gspread.authorize(credentials)
            return self
        except Exception as e:
            print(e)
            self.client = None
            print('Failed to connect to Google Sheets service.')
            return False

    def workbook(self, key: str):
        try:
            wb = self.client.open_by_key(key)
            self.wb = wb
            return self
        except Exception as e:
            print(e)
            print('Failed to open workbook with key ' + str(key))

    def worksheet(self, name: str):
        try:
            ws = self.wb.worksheet(name)
            self.ws = ws
            return self
        except Exception as e:
            print(e)
            print('Failed to open worksheet with name ' + str(name))

    def as_dataframe(self, header_row: int = 0):
        try:
            data = self.ws.get_all_values()
            headers = data.pop(header_row)
            df = pd.DataFrame(data, columns=headers)
            return df
        except Exception as e:
            print(e)
            print('Failed to open worksheet with name ' + str(name))
