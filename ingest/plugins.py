"""Define project-specific methods for data ingestion."""
# standard modules
import os
from os import sys
from datetime import date
from collections import defaultdict

# 3rd party modules
import boto3
from pony.orm import db_session, commit, get, select
from pony.orm.core import CacheIndexError, ObjectNotFound
import pprint

# local modules
from .sources import GoogleSheetSource, AirtableSource
from .util import upsert, download_file, bcolors
import pandas as pd

# constants
# define S3 client used for adding / checking for files in the S3
# storage bucket
s3 = boto3.client('s3')
S3_BUCKET_NAME = 'covid-npi-policy-storage'

# pretty printing: for printing JSON objects legibly
pp = pprint.PrettyPrinter(indent=4)

# define exported classes
__all__ = ['CovidPolicyPlugin']


def get_s3_bucket_keys(s3_bucket_name: str):
    """For the given S3 bucket, return all file keys, i.e., filenames.

    Parameters
    ----------
    s3_bucket_name : str
        Name of S3 bucket.

    Returns
    -------
    type
        Description of returned object.

    """
    nextContinuationToken = None
    keys = list()
    more_keys = True

    # while there are still more keys to retrieve from the bucket
    while more_keys:

        # use continuation token if it is defined
        response = None
        if nextContinuationToken is not None:
            response = s3.list_objects_v2(
                Bucket=S3_BUCKET_NAME,
                ContinuationToken=nextContinuationToken,
            )

        # otherwise it is the first request for keys, so do not include it
        else:
            response = s3.list_objects_v2(
                Bucket=S3_BUCKET_NAME,
            )

        # set continuation key if it is provided in the response,
        # otherwise do not since it means all keys have been returned
        if 'NextContinuationToken' in response:
            nextContinuationToken = response['NextContinuationToken']
        else:
            nextContinuationToken = None

        # for each response object, extract the key and add it to the
        # full list
        for d in response['Contents']:
            keys.append(d['Key'])

        # are there more keys to pull from the bucket?
        more_keys = nextContinuationToken is not None

    # return master list of all bucket keys
    return keys


class IngestPlugin():
    """Basic data ingest plugin.

    Parameters
    ----------
    name : str
        Name of project.

    Attributes
    ----------
    name

    """

    def __init__(self, name: str):
        self.name = name


class CovidPolicyPlugin(IngestPlugin):
    """Ingest COVID non-pharmaceutical interventions (NPI) policy data from an
    Airtable base.

    """

    def __init__(self):
        return None

    def load_client(self):
        """Load client to access Airtable. NOTE: You must set environment
        variable `AIRTABLE_API_KEY` to use this.

        Returns
        -------
        self

        """

        # get Airtable client for specified base
        client = AirtableSource(
            name='Airtable',
            base_key='appOtKBVJRyuH83wf',
            api_key=os.environ.get('AIRTABLE_API_KEY')
        )
        self.client = client
        return self

    def load_data(self):
        """Retrieve dataframes from Airtable base for datasets and
        data dictionary.

        Returns
        -------
        self

        """

        print('\n\n[0] Connecting to Airtable and fetching tables...')
        self.client.connect()

        # core data
        self.data = self.client \
            .worksheet(name='Policy Database') \
            .as_dataframe()

        # data dictionary
        self.data_dictionary = self.client \
            .worksheet(name='Appendix: data dictionary') \
            .as_dataframe(view='API ingest')

        # TODO glossary

        return self

    @db_session
    def process_data(self, db):
        """Perform data validation and create database entity instances
        corresponding to the data records.

        Parameters
        ----------
        db : type
            PonyORM database instance.

        Returns
        -------
        self

        """

        # sort by policy ID
        self.data.sort_values('Unique ID')

        # analyze for QA/QC and quit if errors detected
        valid = self.check(self.data)
        if not valid:
            print('Data are invalid. Please correct issues and try again.')
            sys.exit(0)
        else:
            print('QA/QC found no issues. Continuing.')

        # upsert metadata records
        self.create_metadata(db)

        # set column names to database field names
        all_keys = select((i.ingest_field, i.display_name)
                          for i in db.Metadata)[:]

        # use field names instead of column headers for core dataset
        # TODO do this for future data tables as needed
        columns = dict()
        for field, display_name in all_keys:
            columns[display_name] = field
        self.data = self.data.rename(columns=columns)

        # create Policy instances
        self.create_policies(db)

        # create and validate File instances (syncs the file objects to S3)
        self.create_files_from_attachments(db)
        self.create_files_from_urls(db)
        self.validate_docs(db)

        # create Auth_Entity and Place instances
        self.create_auth_entities_and_places(db)

        print('\nData ingest completed.')
        return self

    @db_session
    def create_auth_entities_and_places(self, db):
        """Create authorizing entity instances and place instances.

        Parameters
        ----------
        db : type
            PonyORM database instance

        Returns
        -------
        self

        """

        # Local methods ########################################################
        def get_place_loc(i):
            """Get well-known text location string for a place.

            Parameters
            ----------
            i : type
                Instance of `Place`.

            Returns
            -------
            str
                Well-known location string

            """
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

        # TODO move formatter to higher-level scope
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
            unknown
                Formattedalue of data field for the record.

            """
            if d[key] == 'N/A' or d[key] == 'NA' or d[key] == None:
                return 'Unspecified'
            else:
                return d[key]

        # Main #################################################################
        # retrieve keys needed to ingest data for Place, Auth_Entity, and
        # Auth_Entity.Place data fields.
        place_keys = select(
            i.ingest_field for i in db.Metadata if
            i.entity_name == 'Place'
            and i.export == True)[:][:]

        auth_entity_keys = select(
            i.ingest_field for i in db.Metadata if
            i.entity_name == 'Auth_Entity' and i.export == True)[:][:]

        auth_entity_place_keys = select(
            i.ingest_field for i in db.Metadata if
            i.entity_name == 'Auth_Entity.Place' and i.export == True)[:][:]

        # track upserted records
        n_inserted = 0
        n_updated = 0
        n_deleted = 0
        n_inserted_auth_entity = 0
        n_updated_auth_entity = 0
        n_deleted_auth_entity = 0

        # for each row of the data
        for i, d in self.data.iterrows():

            ## Add places ######################################################
            # determine whether the specified instance has been defined yet, and
            # if not, add it.
            instance_data = {key: formatter(key, d) for key in place_keys}

            # the affected place is different from the auth entity's place if it
            # exists (is defined in the record) and is different
            affected_diff_from_auth = d['place.level'] != None and \
                d['place.level'] != ''

            # get or create the place affected
            place_affected = None
            if affected_diff_from_auth:
                place_affected_instance_data = {
                    key.split('.')[-1]: formatter(key, d) for key in place_keys}

                # perform upsert using get and set data fields
                place_affected_get_keys = ['level', 'iso3', 'area1', 'area2']
                place_affected_set_keys = ['dillons_rule', 'home_rule']
                action, place_affected = upsert(
                    db.Place,
                    {k: place_affected_instance_data[k]
                        for k in place_affected_get_keys},
                    {k: place_affected_instance_data[k]
                        for k in place_affected_set_keys},
                )
                if action == 'update':
                    n_updated += 1
                elif action == 'insert':
                    n_inserted += 1

            # get or create the place of the auth entity
            auth_entity_place_instance_data = {key.split('.')[-1]: formatter(
                key, d) for key in auth_entity_place_keys +
                ['home_rule', 'dillons_rule']}

            # perform upsert using get and set data fields
            get_keys = ['level', 'iso3', 'area1', 'area2']
            set_keys = ['dillons_rule', 'home_rule']
            action, place_auth = upsert(
                db.Place,
                {k: auth_entity_place_instance_data[k]
                    for k in get_keys},
                {k: auth_entity_place_instance_data[k]
                    for k in set_keys},
            )
            if action == 'update':
                n_updated += 1
            elif action == 'insert':
                n_inserted += 1

            # if the affected place is undefined, set it equal to the
            # auth entity's place
            if place_affected is None:
                place_affected = place_auth

            # link instance to required entities
            # TODO consider flagging updates here
            db.Policy[d['id']].place = place_affected

            ## Add auth_entities ###############################################
            # parse auth entities in raw data record (there may be more than
            # one defined for each record)
            raw_data = get_auth_entities_from_raw_data(d)

            # for each individual auth entity
            for dd in raw_data:

                # get or create auth entity
                auth_entity_instance_data = {key: formatter(
                    key, dd) for key in auth_entity_keys}
                auth_entity_instance_data['place'] = place_auth

                # perform upsert using get and set data fields
                get_keys = ['name', 'office', 'place']
                action, auth_entity = upsert(
                    db.Auth_Entity,
                    {k: auth_entity_instance_data[k]
                        for k in get_keys},
                    {},
                )
                if action == 'update':
                    n_updated_auth_entity += 1
                elif action == 'insert':
                    n_inserted_auth_entity += 1

                # link instance to required entities
                db.Policy[d['id']].auth_entity.add(auth_entity)

        ## Delete unused instances #############################################
        # delete auth_entities that are not used
        auth_entities_to_delete = select(
            i for i in db.Auth_Entity
            if len(i.policies) == 0)
        if len(auth_entities_to_delete) > 0:
            auth_entities_to_delete.delete()
            n_deleted_auth_entity += len(auth_entities_to_delete)
            commit()

        # delete places that are not used
        places_to_delete = select(
            i for i in db.Place
            if len(i.policies) == 0 and len(i.auth_entities) == 0)
        if len(places_to_delete) > 0:
            places_to_delete.delete()
            n_deleted += len(places_to_delete)
            commit()

        print('\n\n[7] Ingesting places...')
        print('Inserted: ' + str(n_inserted))
        print('Updated: ' + str(n_updated))
        print('Deleted: ' + str(n_deleted))

        print('\n\n[8] Ingesting authorizing entities...')
        print('Inserted: ' + str(n_inserted_auth_entity))
        print('Updated: ' + str(n_updated_auth_entity))
        print('Deleted: ' + str(n_deleted_auth_entity))

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
        print('\n\n[3] Ingesting policy data...')

        keys = select(i.field for i in db.Metadata if i.entity_name ==
                      'Policy' and i.field != 'id')[:]

        # maintain dict of attributes to set post-creation
        post_creation_attrs = defaultdict(dict)

        def formatter(key, d):
            if key.startswith('date_'):
                if d[key] == '' or d[key] is None or d[key] == 'N/A' or d[key] == 'NA':
                    return None
                elif len(d[key].split('/')) == 2:
                    print(f'''Unexpected format for `{key}`: {d[key]}\n''')
                    return None
                else:
                    return d[key]
            elif d[key] == 'N/A' or d[key] == 'NA' or d[key] == '':
                if key in ('prior_policy'):
                    return set()
                else:
                    return 'Unspecified'
            elif key == 'id':
                return int(d[key])
            elif key in ('prior_policy'):
                post_creation_attrs[d['id']]['prior_policy'] = \
                    set(d[key])
                return set()
            return d[key]

        # track upserted records
        upserted = set()
        n_inserted = 0
        n_updated = 0

        for i, d in self.data.iterrows():
            # upsert policies
            action, instance = upsert(
                db.Policy,
                {'id': d['id']},
                {key: formatter(key, d) for key in keys},
                skip=['prior_policy']
            )
            if action == 'update':
                n_updated += 1
            elif action == 'insert':
                n_inserted += 1
            upserted.add(instance)

        for i, d in self.data.iterrows():
            # upsert policies
            # TODO consider how to count these updates, since they're done
            # after new instances are created (if counting them at all)
            upsert(
                db.Policy,
                {'id': d['id']},
                {'prior_policy': [db.Policy.get(
                    source_id=source_id) for source_id in d['prior_policy']]},
            )

        # delete all records in table but not in ingest dataset
        n_deleted = db.Policy.delete_2(upserted)
        commit()
        print('Inserted: ' + str(n_inserted))
        print('Updated: ' + str(n_updated))
        print('Deleted: ' + str(n_deleted))

    @db_session
    def create_metadata(self, db):
        """Create metadata instances if they do not exist. If they do exist,
        update them.

        Parameters
        ----------
        db : type
            Description of parameter `db`.

        Returns
        -------
        type
            Description of returned object.

        """
        print('\n\n[2] Ingesting metadata from data dictionary...')
        colgroup = ''
        upserted = set()
        n_inserted = 0
        n_updated = 0
        for i, d in self.data_dictionary.iterrows():
            if d['Category'] != '':
                colgroup = d['Category']
            if d['Database entity'] == '' or d['Database field name'] == '':
                continue
            metadatum_attributes = {
                'ingest_field': d['Ingest field name'],
                'display_name': d['Field'],
                'colgroup': colgroup,
                'definition': d['Definition'],
                'possible_values': d['Possible values'],
                'notes': d['Notes'],
                'order': d['ID'],
                'export': d['Export?'],
            }

            action, instance = upsert(db.Metadata, {
                'field': d['Database field name'],
                'entity_name': d['Database entity'],
            }, metadatum_attributes)
            if action == 'update':
                n_updated += 1
            elif action == 'insert':
                n_inserted += 1
            upserted.add(instance)

        # add extra metadata not in the data dictionary
        other_metadata = [
            ({
                'field': 'loc',
                'entity_name': 'Place',
            }, {
                'ingest_field': 'loc',
                'display_name': 'Country / Specific location',
                'colgroup': '',
                'definition': 'The location affected by the policy',
                'possible_values': 'Any text',
                'notes': '',
                'order': 0,
                'export': False,
            }), ({
                'field': 'source_id',
                'entity_name': 'Policy',
            }, {
                'ingest_field': 'source_id',
                'display_name': 'Source ID',
                'colgroup': '',
                'definition': 'The unique ID of the record in the original dataset',
                'possible_values': 'Any text',
                'order': 0,
                'notes': '',
                'export': False,
            })
        ]
        for get, d in other_metadata:
            action, instance = upsert(db.Metadata, get, d)
            if action == 'update':
                n_updated += 1
            elif action == 'insert':
                n_inserted += 1
            upserted.add(instance)

        # delete all records in table but not in ingest dataset
        n_deleted = db.Metadata.delete_2(upserted)
        commit()
        print('Inserted: ' + str(n_inserted))
        print('Updated: ' + str(n_updated))
        print('Deleted: ' + str(n_deleted))

    @db_session
    def create_files_from_urls(self, db):
        """Create docs instances based on policies.

        Parameters
        ----------
        db : type
            Description of parameter `db`.

        Returns
        -------
        type
            Description of returned object.

        """
        print('\n\n[5] Ingesting files from URLs / filenames...')

        policy_doc_keys = [
            'policy_name',
            'policy_filename',
            'policy_data_source',
        ]

        docs_by_id = dict()

        # track missing filenames
        missing_filenames = set()

        # track upserted PDFs -- if there are any filenames in it that are not
        # in the S3 bucket, upload those files to S3
        upserted = set()
        n_inserted = 0
        n_updated = 0

        for i, d in self.data.iterrows():
            instance_data = {key.split('_', 1)[1]: d[key]
                             for key in policy_doc_keys}
            if instance_data['filename'] is None or \
                    instance_data['filename'].strip() == '':
                missing_filenames.add(d['id'])
                continue
            instance_data['type'] = 'policy'
            id = " - ".join(instance_data.values())

            instance_data['filename'] = instance_data['filename'].replace(
                '.', '')
            instance_data['filename'] += '.pdf'
            action, file = upsert(db.File, instance_data)
            if action == 'update':
                n_updated += 1
            elif action == 'insert':
                n_inserted += 1
            upserted.add(file)

            # link file to policy
            db.Policy[d['id']].file.add(file)
            commit()

        print('Inserted: ' + str(n_inserted))
        print('Updated: ' + str(n_updated))
        print('Deleted: ' + str('n/a'))

        # display any records that were missing a PDF
        if len(missing_filenames) > 0:
            missing_filenames_list = list(
                missing_filenames)
            missing_filenames_list.sort(key=lambda x: int(x))
            print(
                f'''\n{bcolors.BOLD}[Warning] Missing filenames for {len(missing_filenames_list)} policies with these unique IDs:{bcolors.ENDC}''')
            print(bcolors.BOLD + str(", ".join(missing_filenames_list)) + bcolors.ENDC)

    # Airtable attachment parsing for documents
    @db_session
    def create_files_from_attachments(self, db):
        """Create docs instances based Airtable attachments.

        Parameters
        ----------
        db : type
            Description of parameter `db`.

        Returns
        -------
        type
            Description of returned object.

        """
        print('\n\n[4] Ingesting files from Airtable attachments...')

        policy_doc_keys = \
            {
                'test_files': {
                    'data_source': 'policy_data_source',
                    'name': 'policy_name',
                }
            }

        docs_by_id = dict()

        # track missing PDF filenames and source URLs
        missing_filenames = list()

        # track upserted PDFs -- if there are any filenames in it that are not
        # in the S3 bucket, upload those files to S3
        upserted = set()
        n_inserted = 0
        n_updated = 0

        # for each record in the raw data, potentially create file(s)
        for i, d in self.data.iterrows():
            for key in policy_doc_keys:
                if d[key] is not None and len(d[key]) > 0:
                    for dd in d[key]:
                        # create file key
                        file_key = dd['id'] + ' - ' + dd['filename']

                        # check if file exists already
                        # define get data
                        get_data = {
                            'filename': file_key
                        }

                        # define set data
                        set_data = {
                            'name': dd['filename'],
                            'type': key,
                            'data_source': d[policy_doc_keys[key]['data_source']],
                            'permalink': dd['url'],
                        }

                        # perform upsert and link to relevant policy/plan
                        action, file = upsert(db.File, get_data, set_data)
                        if action == 'update':
                            n_updated += 1
                        elif action == 'insert':
                            n_inserted += 1
                        upserted.add(file)

                        # link file to policy
                        db.Policy[d['id']].file.add(file)

        print('Inserted: ' + str(n_inserted))
        print('Updated: ' + str(n_updated))
        print('Deleted: ' + str('n/a'))

    @db_session
    def validate_docs(self, db):
        files = db.File.select()
        print(f'''\n\n[6] Validating {len(files)} files...''')
        # confirm file exists in S3 bucket for file, if not, either add it
        # or remove the PDF text
        # define filename from db

        # track what was done
        n_valid = 0
        n_missing = 0
        n_added = 0
        n_failed = 0
        keys = get_s3_bucket_keys(s3_bucket_name=S3_BUCKET_NAME)
        could_not_download = set()
        missing_filenames = set()
        for file in files:
            if file.filename is not None:
                file_key = file.filename
                if file_key in keys:
                    # print('\nFile found')
                    n_valid += 1
                    pass
                elif file.data_source is None or file.data_source.strip() == '':
                    # print('\nDocument not found (404), no URL')
                    file.filename = None
                    commit()
                    missing_filenames.add(file.name)
                    n_missing += 1
                else:
                    # print('\nFetching and adding PDF to S3: ' + file_key)
                    file_url = file.permalink if file.permalink is not None \
                        else file.data_source
                    file = download_file(
                        file_url, file_key, None, as_object=True)
                    if file is not None:
                        response = s3.put_object(
                            Body=file,
                            Bucket=S3_BUCKET_NAME,
                            Key=file_key,
                        )
                        n_added += 1
                    else:
                        print('Could not download file at URL ' +
                              str(file_url))
                        could_not_download.add(file_url)
                        n_failed += 1
            else:
                print("Skipping, no file associated")
        print('Valid: ' + str(n_valid))
        print('Added to S3: ' + str(n_added))
        print('Missing (no URL or filename): ' + str(n_missing))
        print('Failed to fetch from URL: ' + str(n_failed))
        if n_missing > 0:
            missing_filenames.sort()
            print(
                f'''\n{bcolors.BOLD}[Warning] URLs or filenames were not provided for {n_missing} files with the following names:{bcolors.ENDC}''')
            print(bcolors.BOLD + str(", ".join(missing_filenames)) + bcolors.ENDC)

        if n_failed > 0:
            could_not_download.sort()
            print(
                f'''\n{bcolors.BOLD}[Warning] Files could not be downloaded from the following {n_failed} sources:{bcolors.ENDC}''')
            print(bcolors.BOLD + str(", ".join(could_not_download)) + bcolors.ENDC)

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
        print('\n\n[1] Performing QA/QC on dataset...')

        valid = True

        # unique primary key `id`
        dupes = data.duplicated(['Unique ID'])
        if dupes.any():
            print('\nDetected duplicate unique IDs:')
            print(data[dupes == True].loc[:, 'id'])
            valid = False

        # dates formatted well
        # TODO

        return valid

    # Deprecated / Unused methods ##############################################

    def load_data_google(self):
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
