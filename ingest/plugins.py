"""Define project-specific methods for data ingestion."""
# standard modules
from os import sys
from datetime import date

# 3rd party modules
from pony.orm import db_session, commit, get
from pony.orm.core import CacheIndexError, ObjectNotFound
import pprint

# local modules
from .sources import GoogleSheetSource
import pandas as pd

# constants
pp = pprint.PrettyPrinter(indent=4)
__all__ = ['CovidPolicyPlugin']


class IngestPlugin():
    def __init__(self, name: str):
        self.name = name


class CovidPolicyPlugin(IngestPlugin):
    """Ingest COVID non-pharmaceutical interventions (NPI) policy data from a
    Google Sheet.

    """

    def __init__(self):
        return None

    def load_client(self):
        """Load client to access Google Sheets.

        Returns
        -------
        type
            Description of returned object.

        """
        client = GoogleSheetSource(
            name='Google',
            config_json_relpath='config/googleKey.json'
        )
        self.client = client
        return self

    def load_data(self):
        """Retrieve Google Sheets as Pandas DataFrames corresponding to the (1)
        data, (2) data dictionary, and (3) glossary of terms.

        Returns
        -------
        type
            Description of returned object.

        """
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
        """Perform data validation and create database entity instances
        corresponding to the data records.

        Parameters
        ----------
        db : type
            Description of parameter `db`.

        Returns
        -------
        type
            Description of returned object.

        """
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
        self.create_auth_entities_and_places(db)
        return self

    @db_session
    def create_auth_entities_and_places(self, db):
        """Create authorizing entity instances and place instances.

        Parameters
        ----------
        db : type
            Description of parameter `db`.

        Returns
        -------
        type
            Description of returned object.

        """
        auth_entity_keys = [
            'name',
            'office'
            # NOTE: `office` not included here, handled specially
        ]

        place_keys = [
            'level',
            'iso3',
            'area1',
            'area2',
        ]

        def get_place_loc(i):
            if i.area2.lower() not in ('unspecified', 'n/a'):
                return f'''{i.area2}, {i.area1}, {i.iso3}'''
            elif i.area1.lower() not in ('unspecified', 'n/a'):
                return f'''{i.area1}, {i.iso3}'''
            else:
                return i.iso3

        def get_auth_entities_from_raw_data(d):
            """Given a datum `d` from raw data, create a list of authorizing
            entities that are implied by the semicolon-delimited names and
            offices on that datum.

            Parameters
            ----------
            d : type
                Description of parameter `d`.

            Returns
            -------
            type
                Description of returned object.

            """
            entity_names = d['name'].split('; ')
            entity_offices = d['office'].split('; ')
            num_entities = len(entity_names)
            if num_entities == 1:
                return [d]
            else:
                i = 0
                entities = list()
                for instance in entity_names:
                    entities.append(
                        {
                            'id': d['id'],
                            'name': entity_names[i],
                            'office': entity_offices[i]
                        }
                    )
                return entities

        def get_policy_for_auth_entity_or_place(d):
            """Given raw datum `d` returns the unique ID in the database for the
            Policy instance that should be linked to the authorizing entity /
            place implied by `d`.

            Parameters
            ----------
            d : type
                Description of parameter `d`.

            Returns
            -------
            type
                Description of returned object.

            """
            return d['id']

        auth_entity_info = {
            'keys': auth_entity_keys,
            'check_multi': get_auth_entities_from_raw_data,
            'link': [
                {
                    'entity_class_name': 'Policy',
                    # defines func to get unique ID of instance to link on
                    'on': get_policy_for_auth_entity_or_place,
                },
            ]
        }

        place_info = {
            'keys': place_keys,
            'link': [
                {
                    'entity_class_name': 'Policy',
                    # defines func to get unique ID of instance to link on
                    'on': get_policy_for_auth_entity_or_place,
                }
            ]
        }

        def formatter(key, d):
            """Return 'Unspecified' if a null value, otherwise return value.

            Parameters
            ----------
            key : type
                Description of parameter `key`.
            d : type
                Description of parameter `d`.

            Returns
            -------
            type
                Description of returned object.

            """
            if d[key] == 'N/A' or d[key] == 'NA' or d[key] == None:
                return 'Unspecified'
            else:
                return d[key]

        # for each row of the data
        for i, d in self.data.iterrows():

            ## Add places ######################################################
            # determine whether the specified instance has been defined yet, and
            # if not, add it.
            keys = place_keys
            info = place_info
            name = 'Place'
            instance_data = {key: formatter(key, d) for key in keys}
            entity_class = getattr(db, name)
            place = get(
                i for i in entity_class
                if i.level == instance_data['level']
                and i.iso3 == instance_data['iso3']
                and i.area1 == instance_data['area1']
                and i.area2 == instance_data['area2']
            )
            instance = place

            # if entity already exists, use it
            # otherwise, create it
            if instance is None:
                instance = entity_class(**instance_data)
                instance.loc = get_place_loc(instance)
                commit()

            # link instance to required entities
            for link in info['link']:
                try:
                    entity_class = getattr(db, link['entity_class_name'])
                    setattr(entity_class[link['on'](d)],
                            name.lower(), instance)
                    commit()
                except ObjectNotFound as e:
                    print('Error: Instance not found for linkage. Skipping.')
                    # # TODO dynamically
                    # if len(instance.policies) == 0:
                    #     print('Deleting orphaned instance.')
                    #     instance.delete()
                    #     commit()
                except Error as e:
                    print('Error:')
                    print(e)

            ## Add auth_entities ###############################################
            keys = auth_entity_keys
            info = auth_entity_info
            name = 'Auth_Entity'
            raw_data = d if 'check_multi' not in info \
                else info['check_multi'](d)
            for dd in raw_data:
                instance_data = {key: formatter(key, dd) for key in keys}
                entity_class = getattr(db, name)
                auth_entity = get(
                    i for i in entity_class
                    if i.name == instance_data['name']
                    and i.office == instance_data['office']
                )
                instance = auth_entity

                # if entity already exists, use it
                # otherwise, create it
                if instance is None:
                    instance = entity_class(**instance_data)
                    commit()

                # do facile entity links
                instance.place = place

                # link instance to required entities
                for link in info['link']:
                    try:
                        entity_class = getattr(db, link['entity_class_name'])
                        setattr(entity_class[link['on'](d)],
                                name.lower(), instance)
                        commit()
                    except ObjectNotFound as e:
                        print('Error: Instance not found for linkage. Skipping.')
                        # # TODO dynamically
                        # if len(instance.policies) == 0:
                        #     print('Deleting orphaned instance.')
                        #     instance.delete()
                        #     commit()
                    except Error as e:
                        print('Error:')
                        print(e)

            #####

            # # handle special "split on" keys
            # # offices = d['offices'].split(';')
            # for dd in offices:
            #     instance_data = instance_data_tmp.copy()
            #     instance_data['office'] = dd.strip()
            #     try:
            #         # check if auth entity with same attributes already
            #         # exists
            #         existing_auth_entity = get(
            #             i for i in db.Auth_Entity
            #             if i.level == instance_data['level']
            #             and i.iso3 == instance_data['iso3']
            #             and i.area1 == instance_data['area1']
            #             and i.area2 == instance_data['area2']
            #             and i.name == instance_data['name']
            #         )
            #
            #         # create auth entity if one was not found
            #         auth_entity = db.Auth_Entity(**instance_data) if \
            #             existing_auth_entity is None else existing_auth_entity
            #         commit()
            #
            #         # link to policy
            #         try:
            #             db.Policy[d['id']].auth_entity = auth_entity
            #             commit()
            #         except ObjectNotFound as e:
            #             print('Error: Policy not found for linkage. Skipping.')
            #             if len(auth_entity.policies) == 0:
            #                 print('Deleting orphaned auth_entity.')
            #                 auth_entity.delete()
            #                 commit()
            #     except CacheIndexError as e:
            #         print('\nAuthorizing entity already exists, continuing')
            #         print(e)
            #
            #         # link to policy
            #         # TODO
            #         continue
            #     except ValueError as e:
            #         print('\nError: Unexpected value in this data:')
            #         print(instance_data)
            #         print(e)
            #         # sys.exit(0)

    @db_session
    def create_policies(self, db):
        """Create policy instances.

        Parameters
        ----------
        db : type
            Description of parameter `db`.

        Returns
        -------
        type
            Description of returned object.

        """
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
