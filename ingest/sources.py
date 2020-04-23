# standard packages
import os

# Packages for GoogleSheetSource
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd


__all__ = ['GoogleSheetSource']


class DataSource():
    def __init__(self, name: str):
        self.name = name


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

    def as_dataframe(self):
        try:
            data = self.ws.get_all_values()
            headers = data.pop(0)
            df = pd.DataFrame(data, columns=headers)
            return df
        except Exception as e:
            print(e)
            print('Failed to open worksheet with name ' + str(name))
