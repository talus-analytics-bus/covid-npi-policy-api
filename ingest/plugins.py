# standard modules
from os import sys

# local modules
from .sources import GoogleSheetSource
import pandas as pd


__all__ = ['CovidPolicyPlugin']


class IngestPlugin():
    def __init__(self, name: str):
        self.name = name


class CovidPolicyPlugin(IngestPlugin):
    def __init__(self):
        return None

    def load_client(self):
        client = GoogleSheetSource(
            name='Google',
            config_json_relpath='config/googleKey.json'
        )
        self.client = client
        return self

    def load_data(self):
        prod_gs_key = '135XlMpxubqpq6UFOOIMVrNqSU0tuA0ZZtaXFEIICZX4'
        dev_gs_key = '1syh6kMwFtmqcwTTSyMQwZu54hWRBhlI6N_9vfNO-9Io'

        self.client.connect() \
            .workbook(key=dev_gs_key)

        self.data = self.client \
            .worksheet(name='data') \
            .as_dataframe(header_row=1)

        self.data_dictionary = self.client \
            .worksheet(name='appendix: data dictionary') \
            .as_dataframe()

        self.glossary = self.client \
            .worksheet(name='appendix: glossary') \
            .as_dataframe()

        return self

    def process_data(self):
        data = self.data

        # drop extraneous rows
        data = data.drop(0)
        data = data.drop(1)

        # sort by policy ID
        data.sort_values('id')

        # analyze for QA/QC
        valid = self.check(data)
        if not valid:
            print('Data are invalid. Please correct issues and try again.')
            sys.exit(0)

        print(data)
        return self

    def check(self, data):
        """Perform QA/QC on the data and return a report.
        TODO

        Parameters
        ----------
        data : type
            Description of parameter `data`.

        Returns
        -------
        type
            Description of returned object.

        """
        print('Performing QA/QC on dataset...')
        print('QA/QC not yet implemented. Continuing.\n')
        return True

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
