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
from .util import upsert, download_pdf
import pandas as pd

# constants
s3 = boto3.client('s3')
pp = pprint.PrettyPrinter(indent=4)
__all__ = ['CovidPolicyPlugin']
S3_BUCKET_NAME = 'covid-npi-policy-storage'


def get_s3_bucket_keys():
    nextContinuationToken = None
    keys = list()
    more_keys = True
    while more_keys:
        response = None
        if nextContinuationToken is not None:
            response = s3.list_objects_v2(
                Bucket=S3_BUCKET_NAME,
                ContinuationToken=nextContinuationToken,
            )
        else:
            response = s3.list_objects_v2(
                Bucket=S3_BUCKET_NAME,
                # ContinuationToken='string',
            )

        if 'NextContinuationToken' in response:
            nextContinuationToken = response['NextContinuationToken']
        else:
            nextContinuationToken = None

        for d in response['Contents']:
            keys.append(d['Key'])
        more_keys = nextContinuationToken is not None
    return keys


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
        client = AirtableSource(
            name='Airtable',
            base_key='appOtKBVJRyuH83wf',
            api_key=os.environ.get('AIRTABLE_API_KEY')
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

        self.client.connect()

        self.data = self.client \
            .worksheet(name='Policy Database') \
            .as_dataframe()

        self.data_dictionary = self.client \
            .worksheet(name='Appendix: data dictionary') \
            .as_dataframe(view='API ingest')

        return self

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

    @db_session
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

        # sort by policy ID
        self.data.sort_values('Unique ID')

        # analyze for QA/QC
        valid = self.check(self.data)

        self.create_metadata(db)

        # set column names to database field names
        all_keys = select((i.ingest_field, i.display_name)
                          for i in db.Metadata)[:]

        # use field names instead of column headers for data
        columns = dict()
        for field, display_name in all_keys:
            columns[display_name] = field
        self.data = self.data.rename(columns=columns)

        # if not valid:
        #     print('Data are invalid. Please correct issues and try again.')
        #     sys.exit(0)
        # else:
        #     print('QA/QC found no issues. Continuing.')

        self.create_policies(db)
        self.create_docs_2(db)
        self.create_docs(db)
        self.validate_docs(db)
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

        place_keys = select(
            i.ingest_field for i in db.Metadata if i.entity_name == 'Place' and i.export == True)[:][:]
        auth_entity_keys = select(
            i.ingest_field for i in db.Metadata if i.entity_name == 'Auth_Entity' and i.export == True)[:][:]
        auth_entity_place_keys = select(
            i.ingest_field for i in db.Metadata if i.entity_name == 'Auth_Entity.Place' and i.export == True)[:][:]

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
        n = 0
        for i, d in self.data.iterrows():

            ## Add places ######################################################
            # determine whether the specified instance has been defined yet, and
            # if not, add it.
            keys = place_keys
            info = place_info
            name = 'Place'

            instance_data = {key: formatter(key, d) for key in keys}
            entity_class = getattr(db, name)

            affected_diff_from_auth = d['place.level'] != None and d['place.level'] != ''
            place_affected = None
            if affected_diff_from_auth:
                place_affected_instance_data = {
                    key.split('.')[-1]: formatter(key, d) for key in place_keys}

                place_affected = get(
                    i for i in entity_class
                    if i.level == place_affected_instance_data['level']
                    and i.iso3 == place_affected_instance_data['iso3']
                    and i.area1 == place_affected_instance_data['area1']
                    and i.area2 == place_affected_instance_data['area2']
                )

                # if entity already exists, use it
                # otherwise, create it
                if place_affected is None:
                    place_affected = entity_class(
                        **place_affected_instance_data)
                    place_affected.loc = get_place_loc(place_affected)
                    n = n + 1
                    commit()

            # create Place instance for Auth_Entity based on Auth_Entity.Place
            # data fields
            instance_data_auth = {key.split('.')[-1]: formatter(
                key, d) for key in auth_entity_place_keys + ['home_rule', 'dillons_rule']}
            place_auth = get(
                i for i in entity_class
                if i.level == instance_data_auth['level']
                and i.iso3 == instance_data_auth['iso3']
                and i.area1 == instance_data_auth['area1']
                and i.area2 == instance_data_auth['area2']
            )

            # if entity already exists, use it
            # otherwise, create it
            if place_auth is None:
                place_auth = entity_class(**instance_data_auth)
                n = n + 1
                place_auth.loc = get_place_loc(place_auth)
                commit()

            if place_affected is None:
                place_affected = place_auth

            # link instance to required entities
            db.Policy[d['id']].place = place_affected

            ## Add auth_entities ###############################################
            keys = auth_entity_keys
            info = auth_entity_info
            name = 'Auth_Entity'
            raw_data = d if 'check_multi' not in info \
                else info['check_multi'](d)
            for dd in raw_data:
                instance_data = {key: formatter(
                    key, dd) for key in keys}
                entity_class = getattr(db, name)
                auth_entity = get(
                    i for i in entity_class
                    if i.name == instance_data['name']
                    and i.office == instance_data['office']
                    and i.place == place_auth
                )
                instance = auth_entity

                # if entity already exists, use it
                # otherwise, create it
                if instance is None:
                    instance = entity_class(
                        name=instance_data['name'],
                        office=instance_data['office'],
                        place=place_auth
                    )
                    commit()

                # # do facile entity links
                # instance.place = place_auth
                # commit()

                # link instance to required entities
                for link in info['link']:
                    try:
                        entity_class = getattr(db, link['entity_class_name'])
                        setattr(entity_class[link['on'](d)],
                                name.lower(), instance)
                        commit()
                    except ObjectNotFound as e:
                        print('Error: Instance not found for linkage. Skipping.')
                    except Error as e:
                        print('Error:')
                        print(e)
        print('Number of places created = ' + str(n))

        # delete auth_entities that are not used
        auth_entities_to_delete = select(
            i for i in db.Auth_Entity
            if len(i.policies) == 0)
        print('Deleting these auth entities:')
        print(auth_entities_to_delete[:][:])
        auth_entities_to_delete.delete()
        commit()

        # delete places that are not used
        places_to_delete = select(
            i for i in db.Place
            if len(i.policies) == 0 and len(i.auth_entities) == 0)
        print('Deleting these places:')
        print(places_to_delete[:][:])
        places_to_delete.delete()

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

        for i, d in self.data.iterrows():
            # upsert policies
            pp.pprint(d)
            print('keys')
            print(keys)
            instance = upsert(
                db.Policy,
                {'id': d['id']},
                {key: formatter(key, d) for key in keys},
                skip=['prior_policy']
            )
            upserted.add(instance)

        for i, d in self.data.iterrows():
            # upsert policies
            upsert(
                db.Policy,
                {'id': d['id']},
                {'prior_policy': [db.Policy.get(
                    source_id=source_id) for source_id in d['prior_policy']]},
            )

        # delete all records in table but not in ingest dataset
        db.Policy.delete_2(upserted)
        commit()

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

        colgroup = ''
        upserted = set()
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

            instance = upsert(db.Metadata, {
                'field': d['Database field name'],
                'entity_name': d['Database entity'],
            }, metadatum_attributes)
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
            instance = upsert(db.Metadata, get, d)
            upserted.add(instance)

        # delete all records in table but not in ingest dataset
        db.Metadata.delete_2(upserted)
        commit()

    @db_session
    def create_docs(self, db):
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
        policy_doc_keys = [
            'policy_name',
            'policy_pdf',
            'policy_data_source',
        ]

        docs_by_id = dict()

        # track mising PDF filenames and source URLs
        missing_pdfs = set()
        could_not_download = set()

        # track upserted PDFs -- if there are any filenames in it that are not
        # in the S3 bucket, upload those files to S3
        upserted = set()

        for i, d in self.data.iterrows():
            instance_data = {key.split('_', 1)[1]: d[key]
                             for key in policy_doc_keys}
            if instance_data['pdf'] is None or \
                    instance_data['pdf'].strip() == '':
                missing_pdfs.add(d['id'])
                continue
            instance_data['type'] = 'policy'
            id = " - ".join(instance_data.values())

            instance_data['pdf'] += '.pdf'
            doc = upsert(db.Doc, instance_data)

            upserted.add(doc)

            # link doc to policy
            db.Policy[d['id']].doc.add(doc)
            commit()

        # display any records that were missing a PDF
        if len(missing_pdfs) > 0:
            print(
                f'''Missing PDFs for {len(missing_pdfs)} policies with these unique IDs:''')
            pp.pprint(missing_pdfs)

    # Airtable attachment parsing for documents
    @db_session
    def create_docs_2(self, db):
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
        policy_doc_keys = \
            {
                'test_files': {
                    'data_source': 'policy_data_source',
                    'name': 'policy_name',
                }
            }

        docs_by_id = dict()

        # track missing PDF filenames and source URLs
        missing_pdfs = list()

        # track upserted PDFs -- if there are any filenames in it that are not
        # in the S3 bucket, upload those files to S3
        upserted = set()

        for i, d in self.data.iterrows():
            for key in policy_doc_keys:
                if d[key] is not None and len(d[key]) > 0:
                    print('\nd[key]')
                    print(d[key])
                    print(type(d[key][0]))

                    for dd in d[key]:
                        # create file key
                        file_key = dd['id'] + ' - ' + dd['filename']

                        # check if doc exists already
                        # define get data
                        get_data = {
                            'pdf': file_key
                        }

                        # define set data
                        set_data = {
                            'name': d[policy_doc_keys[key]['name']],
                            'type': key,
                            'data_source': d[policy_doc_keys[key]['data_source']],
                            'permalink': dd['url'],
                        }

                        # perform upsert and link to relevant policy/plan
                        doc = upsert(db.Doc, get_data, set_data)
                        upserted.add(doc)
                        db.Policy[d['id']].doc.add(doc)

    @db_session
    def validate_docs(self, db):
        print('Validating document files...')
        # confirm file exists in S3 bucket for doc, if not, either add it
        # or remove the PDF text
        # define filename from db
        keys = get_s3_bucket_keys()
        could_not_download = set()
        for doc in db.Doc.select():
            if doc.pdf is not None:
                file_key = doc.pdf
                if file_key in keys:
                    print('\nFile found')
                    pass
                elif doc.data_source is None or doc.data_source.strip() == '':
                    # print('\nDocument not found (404), no URL')
                    doc.pdf = None
                    commit()
                    missing_pdfs.append(
                        {
                            'pdf': doc.pdf,
                            'data_source': doc.data_source,
                            'permalink': doc.permalink,
                        }
                    )
                else:
                    print('\nFetching and adding PDF to S3: ' + file_key)
                    file_url = doc.permalink if doc.permalink is not None \
                        else doc.data_source
                    file = download_pdf(
                        file_url, file_key, None, as_object=True)
                    if file is not None:
                        response = s3.put_object(
                            Body=file,
                            Bucket=S3_BUCKET_NAME,
                            Key=file_key,
                        )
                        print('Added PDF')
                    else:
                        print('Could not download PDF at URL ' +
                              str(file_url))
                        could_not_download.add(file_url)
            else:
                print("Skipping, no PDF associated")
        if len(could_not_download) > 0:
            pp.pprint('Files could not be downloaded from the following sources:')
            pp.pprint(could_not_download)

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
        dupes = data.duplicated(['Unique ID'])
        if dupes.any():
            print('\nDetected duplicate unique IDs:')
            print(data[dupes == True].loc[:, 'id'])
            valid = False

        # dates formatted well
        # TODO

        return valid
