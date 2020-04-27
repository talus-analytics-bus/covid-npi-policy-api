"""Define project-specific methods for data ingestion."""
# standard modules
from os import sys
from datetime import date

# 3rd party modules
from pony.orm import db_session, commit, get
from pony.orm.core import CacheIndexError, ObjectNotFound

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
        key = '135XlMpxubqpq6UFOOIMVrNqSU0tuA0ZZtaXFEIICZX4'

        self.client.connect() \
            .workbook(key=key)

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

    def process_data(self, db):
        self.data

        # drop extraneous rows
        self.data = self.data.drop(0)
        self.data = self.data.drop(1)

        # sort by policy ID
        self.data.sort_values('id')

        # analyze for QA/QC
        valid = self.check(self.data)
        # if not valid:
        #     print('Data are invalid. Please correct issues and try again.')
        #     sys.exit(0)
        # else:
        #     print('QA/QC found no issues. Continuing.')

        self.create_policies(db)
        self.create_auth_entities(db)
        return self

    @db_session
    def create_auth_entities(self, db):
        keys = [
            'level',
            'iso3',
            'area1',
            'area2',
            'name',
        ]

        def formatter(key, d):
            if d[key] == 'N/A' or d[key] == 'NA' or d[key] == None:
                return 'Unspecified'
            else:
                return d[key]

        for i, d in self.data.iterrows():
            instance_data_tmp = {key: formatter(key, d) for key in keys}

            offices = d['offices'].split(';')
            for dd in offices:
                instance_data = instance_data_tmp.copy()
                instance_data['office'] = dd.strip()
                try:
                    # check if auth entity with same attributes already
                    # exists
                    existing_auth_entity = get(
                        i for i in db.Auth_Entity
                        if i.level == instance_data['level']
                        and i.iso3 == instance_data['iso3']
                        and i.area1 == instance_data['area1']
                        and i.area2 == instance_data['area2']
                        and i.name == instance_data['name']
                    )

                    # create auth entity if one was not found
                    auth_entity = db.Auth_Entity(**instance_data) if \
                        existing_auth_entity is None else existing_auth_entity
                    commit()

                    # link to policy
                    try:
                        db.Policy[d['id']].auth_entity = auth_entity
                        commit()
                    except ObjectNotFound as e:
                        print('Error: Policy not found for linkage. Skipping.')
                        if len(auth_entity.policies) == 0:
                            print('Deleting orphaned auth_entity.')
                            auth_entity.delete()
                            commit()
                except CacheIndexError as e:
                    print('\nAuthorizing entity already exists, continuing')
                    print(e)

                    # link to policy
                    # TODO
                    continue
                except ValueError as e:
                    print('\nError: Unexpected value in this data:')
                    print(instance_data)
                    print(e)
                    # sys.exit(0)

    @db_session
    def create_policies(self, db):
        keys = [
            'id',
            'desc',
            'primary_ph_measure',
            'ph_measure_details',
            'policy_type',
            'date_issued',
            'date_start_effective',
            'date_end_anticipated',
            'date_end_actual',
        ]

        def formatter(key, d):
            if key.startswith('date_'):
                if d[key] == '' or d[key] is None:
                    return None
                elif len(d[key].split('/')) == 2:
                    print(f'''Unexpected format for `{key}`: {d[key]}\n''')
                    return None
                else:
                    return d[key]
            elif d[key] == 'N/A' or d[key] == 'NA' or d[key] == '':
                return 'Unspecified'
            return d[key]

        for i, d in self.data.iterrows():
            instance_data = {key: formatter(key, d) for key in keys}
            try:
                db.Policy(**instance_data)
            except CacheIndexError as e:
                print('\nError: Duplicate policy unique ID: ' +
                      str(instance_data['id']))
                # sys.exit(0)
            except ValueError as e:
                print('\nError: Unexpected value in this data:')
                print(instance_data)
                print(e)
                # sys.exit(0)

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

        valid = True

        # unique primary key `id`
        dupes = data.duplicated(['id'])
        if dupes.any():
            print('\nDetected duplicate unique IDs:')
            print(data[dupes == True].loc[:, 'id'])
            valid = False

        # dates formatted well
        # TODO

        return valid

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
