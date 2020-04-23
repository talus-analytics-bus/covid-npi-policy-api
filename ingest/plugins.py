# local modules
from .sources import GoogleSheetSource
import pandas as pd


__all__ = ['GenericGoogleSheetPlugin']


class IngestPlugin():
    def __init__(self, name: str):
        self.name = name


class GenericGoogleSheetPlugin(IngestPlugin):
    def __init__(self):
        return None

    def load_client(self):
        client = GoogleSheetSource(
            name='Google',
            config_json_relpath='config/googleKey.json'
        )
        self.client = client
        return self

    def load_raw_data(self):
        prod_gs_key = '135XlMpxubqpq6UFOOIMVrNqSU0tuA0ZZtaXFEIICZX4'
        dev_gs_key = '1syh6kMwFtmqcwTTSyMQwZu54hWRBhlI6N_9vfNO-9Io'
        data = self.client.connect() \
            .workbook(key=dev_gs_key) \
            .worksheet(name='data') \
            .as_dataframe()
        self.data = data
        return self
    #
    #
    # def connect_db(self):
    #     # TODO
    #     pass
    #
    #
    # def create_table(self):
    #     # TODO
    #     pass
    #
    #
    # def create_enums(self):
    #     # TODO
    #     pass
