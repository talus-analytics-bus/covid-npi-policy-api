"""Define project-specific methods for data ingestion."""
# standard modules
import os
import pytz
import time
from os import sys
from datetime import date, datetime, timedelta
from collections import defaultdict

# 3rd party modules
import boto3
from pony.orm import db_session, commit, get, select, delete, StrArray
from pony.orm.core import CacheIndexError, ObjectNotFound
import pprint

# local modules
from .sources import GoogleSheetSource, AirtableSource
from .util import upsert, download_file, bcolors, nyt_caseload_csv_to_dict, \
    jhu_caseload_csv_to_dict, find_all
import pandas as pd


# constants
# define S3 client used for adding / checking for files in the S3
# storage bucket
s3 = boto3.client('s3')
S3_BUCKET_NAME = 'covid-npi-policy-storage'

# pretty printing: for printing JSON objects legibly
pp = pprint.PrettyPrinter(indent=4)

# define exported classes
__all__ = ['CovidPolicyPlugin', 'CovidCaseloadPlugin']

# show "In progress" if we find blanks in these fields
show_in_progress = (
    "auth_entity_has_authority",
    "authority_name",
    "auth_entity_authority_data_source",
)


def iterable(obj):
    """Return True if `obj` is iterable, like a string, set, or list,
    False otherwise.

    """
    try:
        iter(obj)
    except Exception:
        return False
    else:
        return True


def str_to_bool(x):
    """Convert yes/no val `x` to True/False, or None otherwise.

    """
    if x == 'Yes':
        return True
    elif x == 'No':
        return False
    else:
        return None


# load data to get country names from ISO3 codes
country_data = pd.read_json('./ingest/data/country.json') \
    .to_dict(orient='records')


def get_name_from_iso3(iso3: str):
    """Given the 3-character ISO code of a country, returns its name
    plus the code in parentheses, or `None` if no match.

    Parameters
    ----------
    iso3 : str
        3-char iso code

    Returns
    -------
    type
        Name or `None`

    """
    if iso3 == 'Unspecified':
        return 'N/A'
    try:
        country = next(d for d in country_data if d['alpha-3'] == iso3)
        return country['name'] + ' (' + iso3 + ')'
    except:
        print('Found no country match for: ' + str(iso3))
        return None


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
    if i.level == 'Country' and ';' not in i.iso3 and \
            i.country_name is not None:
        return i.country_name
    elif i.level == 'Tribal nation':
        return i.area1
    elif i.area2.lower() not in ('unspecified', 'n/a', ''):
        return f'''{i.area2}, {i.area1}, {i.country_name}'''
    elif i.area1.lower() not in ('unspecified', 'n/a', ''):
        if i.country_name is not None:
            return f'''{i.area1}, {i.country_name}'''
        else:
            return i.area1
    elif i.country_name is not None:
        return i.country_name
    elif i.level == 'Country' and ';' in i.iso3:
        return 'Multiple countries'
    else:
        print(i.to_dict())
        input('Could not determine place name for this instance.')


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


def reject(x):
    """Reject instance if this function returns False, or accept it to
    the database otherwise.

    Parameters
    ----------
    x : type
        Description of parameter `x`.

    Returns
    -------
    type
        Description of returned object.

    """
    no_desc = 'desc' in x and x['desc'] == ''

    # reject country-level policies that have a non-blank province or
    # local area
    types = ('auth_entity', 'place')
    country_level_with_prov = False
    for type in types:
        levelKey = type + ".level"
        area1Key = type + ".area1"
        area2Key = type + ".area2"
        if levelKey not in x:
            continue
        else:
            if x[levelKey] == 'Country' and (
                (area1Key in x and x[area1Key] != '') or (
                    area2Key in x and x[area2Key]) != ''
            ):
                country_level_with_prov = True
    reject = no_desc or country_level_with_prov
    return reject
    # multi_iso_aff = 'place.iso3' in x and \
    #     (
    #         ';' in x['place.iso3'] or
    #         (
    #             type(x['place.iso3']) == list and len(x['place.iso3']) > 1
    #         )
    #     )
    #
    # multi_iso_auth = 'auth_entity.iso3' in x and \
    #     (
    #         ';' in x['auth_entity.iso3'] or
    #         (
    #             type(x['auth_entity.iso3']) == list and
    #             len(x['auth_entity.iso3']) > 1
    #         )
    #     )
    # return no_desc or multi_iso_aff or multi_iso_auth


class CovidCaseloadPlugin(IngestPlugin):
    """Ingest COVID caseload data and upload to metric database

    TODO convert this to AWS Lambda

    """

    def __init__(self):
        return None

    @db_session
    def upsert_data(self, db, db_amp):
        """Upsert caseload data from different sources.

        Parameters
        ----------
        db : type
            Description of parameter `db`.
        db_amp : type
            Description of parameter `db_amp`.

        Returns
        -------
        type
            Description of returned object.

        """

        def upsert_nyt_caseload(db, db_amp):
            """Upsert NYT state-level caseload data and derived metrics for
            the USA.

            Parameters
            ----------
            db : type
                Description of parameter `db`.
            db_amp : type
                Description of parameter `db_amp`.

            Returns
            -------
            type
                Description of returned object.

            """
            print('Fetching data from New York Times server...')
            download_url = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-states.csv'
            data = nyt_caseload_csv_to_dict(download_url)
            print('Done.')

            print('\nUpserting relevant metric...')

            # upsert metric for daily US caseload
            action, covid_total_cases_provinces = upsert(
                db.Metric,
                {
                    'metric_name': 'covid_total_cases_provinces',
                    'metric_id': 72
                },
                {
                    'temporal_resolution': 'daily',
                    'spatial_resolution': 'state',
                    'spatial_extent': 'country',
                    'min_time': '2020-01-01',
                    'max_time': '2025-01-01',
                    'unit_type': 'count',
                    'unit': 'cases',
                    'num_type': 'int',
                    'metric_definition': 'The total cumulative number of COVID-19 cases by date and state / province'
                }
            )
            commit()

            # upsert metric for daily US NEW caseload
            action, covid_new_cases_provinces = upsert(
                db.Metric,
                {
                    'metric_name': 'covid_new_cases_provinces',
                    'metric_id': 73
                },
                {
                    'temporal_resolution': 'daily',
                    'spatial_resolution': 'state',
                    'spatial_extent': 'country',
                    'min_time': '2020-01-01',
                    'max_time': '2025-01-01',
                    'unit_type': 'count',
                    'unit': 'cases',
                    'num_type': 'int',
                    'metric_definition': 'The number of new COVID-19 cases by date and state / province',
                    'is_view': True,
                    'view_name': 'metric_73'
                }
            )
            commit()

            # upsert metric for 7-day US NEW caseload
            action, covid_new_cases_provinces_7d = upsert(
                db.Metric,
                {
                    'metric_name': 'covid_new_cases_provinces_7d',
                    'metric_id': 74
                },
                {
                    'temporal_resolution': 'daily',
                    'spatial_resolution': 'state',
                    'spatial_extent': 'country',
                    'min_time': '2020-01-01',
                    'max_time': '2025-01-01',
                    'unit_type': 'count',
                    'unit': 'cases',
                    'num_type': 'int',
                    'metric_definition': 'The number of new COVID-19 cases in the last 7 days by date and state / province',
                    'is_view': True,
                    'view_name': 'metric_74'
                }
            )
            commit()

            print('Done.')

            print('\nUpserting observations...')
            updated_at = datetime.now()
            last_datum_date = None
            for name in data:
                print(name)
                place = db.Place.select().filter(name=name).first()
                if place is None:
                    continue
                else:
                    # i = 0
                    # max_i = str(len(data[name]))
                    for d in data[name]:
                        # print('upserting ' + str(i) + ' of ' + max_i)
                        # i = i + 1
                        dt = select(
                            i for i in db.DateTime
                            if str((i.datetime + timedelta(hours=12)).date()) == d['date']
                        ).first()

                        if dt is None:
                            print('error: missing dt')
                            continue
                        else:
                            last_datum_date = d['date']
                            action, obs_affected = upsert(
                                db.Observation,
                                {
                                    'metric': covid_total_cases_provinces,
                                    'date_time': dt,
                                    'place': place,
                                    'data_source': 'New York Times',  # TODO correct
                                },
                                {
                                    'value': d['cases'],
                                    'updated_at': updated_at,
                                }
                            )

            # update version
            action, version = upsert(
                db_amp.Version,
                {
                    'type': 'COVID-19 caseload data',
                },
                {
                    'date': date.today(),
                    'last_datum_date': last_datum_date,
                }
            )

            print('Done.')

        def upsert_jhu_caseload(db, db_amp):
            """Upsert JHU country-level caseload data and derived metrics for
            the world.

            Parameters
            ----------
            db : type
                Description of parameter `db`.
            db_amp : type
                Description of parameter `db_amp`.

            Returns
            -------
            type
                Description of returned object.

            """
            print('Fetching data from JHU GitHub...')
            download_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'
            data = jhu_caseload_csv_to_dict(download_url, db)
            print('Done.')

            print('\nUpserting relevant metric...')

            # upsert metric for daily US caseload
            action, covid_total_cases_countries = upsert(
                db.Metric,
                {
                    'metric_name': 'covid_total_cases_countries',
                    'metric_id': 75
                },
                {
                    'temporal_resolution': 'daily',
                    'spatial_resolution': 'country',
                    'spatial_extent': 'planet',
                    'min_time': '2020-01-01',
                    'max_time': '2025-01-01',
                    'unit_type': 'count',
                    'unit': 'cases',
                    'num_type': 'int',
                    'metric_definition': 'The total cumulative number of COVID-19 cases by date and country'
                }
            )
            commit()

            # upsert metric for daily US NEW caseload
            action, covid_new_cases_countries = upsert(
                db.Metric,
                {
                    'metric_name': 'covid_new_cases_countries',
                    'metric_id': 76
                },
                {
                    'temporal_resolution': 'daily',
                    'spatial_resolution': 'country',
                    'spatial_extent': 'planet',
                    'min_time': '2020-01-01',
                    'max_time': '2025-01-01',
                    'unit_type': 'count',
                    'unit': 'cases',
                    'num_type': 'int',
                    'metric_definition': 'The number of new COVID-19 cases by date and country',
                    'is_view': True,
                    'view_name': 'metric_76'
                }
            )
            commit()

            # upsert metric for 7-day US NEW caseload
            action, covid_new_cases_countries_7d = upsert(
                db.Metric,
                {
                    'metric_name': 'covid_new_cases_countries_7d',
                    'metric_id': 77
                },
                {
                    'temporal_resolution': 'daily',
                    'spatial_resolution': 'country',
                    'spatial_extent': 'planet',
                    'min_time': '2020-01-01',
                    'max_time': '2025-01-01',
                    'unit_type': 'count',
                    'unit': 'cases',
                    'num_type': 'int',
                    'metric_definition': 'The number of new COVID-19 cases in the last 7 days by date and country',
                    'is_view': True,
                    'view_name': 'metric_77'
                }
            )
            commit()

            print('Done.')

            print('\nUpserting observations...')
            updated_at = datetime.now()
            last_datum_date = None
            n = len(data)
            i = 0
            for d in data:
                print(f'''Adding {i} of {n}''')
                i = i + 1
                place = d['place']

                dt = select(
                    i for i in db.DateTime
                    if str((i.datetime + timedelta(hours=12)).date()) == d['date']
                ).first()

                if dt is None:
                    input('error: missing dt. Press enter to continue.')
                    continue
                else:
                    last_datum_date = d['date']
                    action, obs_affected = upsert(
                        db.Observation,
                        {
                            'metric': covid_total_cases_countries,
                            'date_time': dt,
                            'place': place,
                            'data_source': 'JHU CSSE COVID-19 Dataset',
                        },
                        {
                            'value': d['value'],
                            'updated_at': updated_at,
                        }
                    )

            # update version
            action, version = upsert(
                db_amp.Version,
                {
                    'type': 'COVID-19 caseload data (countries)',
                },
                {
                    'date': date.today(),
                    'last_datum_date': last_datum_date,
                }
            )

            print('Done.')

        # perform all upserts defined above
        upsert_jhu_caseload(db, db_amp)
        upsert_nyt_caseload(db, db_amp)


class CovidPolicyPlugin(IngestPlugin):
    """Ingest COVID non-pharmaceutical interventions (NPI) policy data from an
    Airtable base.

    """

    def __init__(self):
        return None

    def load_client(self, base_key):
        """Load client to access Airtable. NOTE: You must set environment
        variable `AIRTABLE_API_KEY` to use this.

        Returns
        -------
        self

        """

        # get Airtable client for specified base
        client = AirtableSource(
            name='Airtable',
            base_key=base_key,
            api_key=os.environ.get('AIRTABLE_API_KEY')
        )
        self.client = client
        return self

    def load_metadata(self):
        """Retrieve data dictionaries from data source and store in instance.

        Returns
        -------
        self

        """

        print('\n\n[0] Connecting to Airtable and fetching tables...')
        self.client.connect()

        # show every row of data dictionary preview in terminal
        pd.set_option("display.max_rows", None, "display.max_columns", None)

        # policy data dictionary
        self.data_dictionary = self.client \
            .worksheet(name='Appendix: Policy data dictionary') \
            .as_dataframe(view='API ingest')

        # court challenges data dictionary
        self.data_dictionary_court_challenges = self.client \
            .worksheet(name='Appendix: Court challenges data dictionary') \
            .as_dataframe()

        # plan data dictionary
        self.data_dictionary_plans = self.client \
            .worksheet(name='Appendix: Plan data dictionary') \
            .as_dataframe(view='API ingest')

        # glossary
        self.glossary = self.client \
            .worksheet(name='Appendix: glossary') \
            .as_dataframe(view='API ingest')

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

        # local area database
        self.local_areas = self.client \
            .worksheet(name='Local Area Database') \
            .as_dataframe()

        # s3 bucket file keys
        self.s3_bucket_keys = get_s3_bucket_keys(s3_bucket_name=S3_BUCKET_NAME)

        # policy data
        self.data = self.client \
            .worksheet(name='Policy Database') \
            .as_dataframe()

        # plan data
        self.data_plans = self.client \
            .worksheet(name='Plan database') \
            .as_dataframe()

        return self

    def load_court_challenge_data(self):
        """Load court challenge and matter number data into the ingest
        system instance.

        """
        # court challenges
        self.data_court_challenges = self.client \
            .worksheet(name='Court challenges') \
            .as_dataframe()

        # court challenges data dictionary
        self.data_dictionary_court_challenges = self.client \
            .worksheet(name='Appendix: Court challenges data dictionary') \
            .as_dataframe()

        # matter numbers
        self.data_matter_numbers = self.client \
            .worksheet(name='Matter number database (court challenges)') \
            .as_dataframe()

        return self

    @db_session
    def process_court_challenge_data(self, db):
        """Add court challenge records to the database.

        Returns
        -------
        type
            Description of returned object.

        """

        # define data
        data = self.data_court_challenges

        # sort by unique ID
        data.sort_values('Entry ID')

        # remove records without a unique ID and other features
        data = data.loc[data['Entry ID'] != '', :]

        # analyze for QA/QC and quit if errors detected
        # TODO

        # set column names to database field names
        all_keys = select(
            (
                i.ingest_field,
                i.display_name,
                i.field
            )
            for i in db.Metadata
            if i.class_name == 'Court_Challenge'
        )[:]

        # use field names instead of column headers for core dataset
        # TODO do this for future data tables as needed
        columns = dict()
        for ingest_field, display_name, db_field in all_keys:
            field = ingest_field if ingest_field != '' else db_field
            columns[display_name] = field
        data = data.rename(columns=columns)
        self.data_court_challenges = data

        # create entity instances
        self.create_court_challenges(db)

        return self

    @db_session
    def load_observations(self, db):
        print(
            '\n\n[X] Connecting to Airtable for observations and fetching tables...')
        airtable_iter = self.client.worksheet(
            name='Status table').ws.get_iter(view='API ingest', fields=['Name', 'Date', 'Location type', 'Status'])

        # delete existing observations
        print('\n\nDeleting existing observations...')
        delete(i for i in db.Observation if i.metric == 0)
        print('Existing observations deleted.\n')

        # add new observations
        skipped = 0
        for page in airtable_iter:
            for record in page:
                # TODO add observations
                d = record['fields']
                if 'Name' not in d:
                    skipped += 1
                    continue
                if not d['Date'].startswith('2020'):
                    skipped += 1
                    continue

                place = None
                if d['Location type'] == 'State':
                    place = select(
                        i for i in db.Place
                        if i.iso3 == 'USA'
                        and i.area1 == d['Name']
                        and (i.area2 == 'Unspecified' or i.area2 == '')
                        and i.level == 'State / Province'
                    ).first()

                    if place is None:
                        # TODO generalize to all countries
                        action, place = upsert(
                            db.Place,
                            {
                                'iso3': 'USA',
                                'country_name': 'United States of America (USA)',
                                'area1': d['Name'],
                                'area2': 'Unspecified',
                                'level': 'State / Province'
                            },
                            {
                                'loc': f'''{d['Name']}, USA'''
                            }
                        )

                else:
                    # TODO
                    place = select(
                        i for i in db.Place
                        if i.iso3 == d['Name']
                        and i.level == 'Country'
                    ).first()

                    if place is None:
                        # TODO generalize to all countries
                        action, place = upsert(
                            db.Place,
                            {
                                'iso3': d['Name'],
                                'country_name': get_name_from_iso3(d['Name']) + f''' ({d['Name']})''',
                                'area1': 'Unspecified',
                                'area2': 'Unspecified',
                                'level': 'Country'
                            },
                            {
                                'loc': get_name_from_iso3(d['Name']) + f''' ({d['Name']})'''
                            }
                        )

                if place is None:
                    print('[FATAL ERROR] Missing place')
                    sys.exit(0)

                action, d = upsert(
                    db.Observation,
                    {'source_id': record['id']},
                    {
                        'date': d['Date'],
                        'metric': 0,
                        'value': 'Mixed distancing levels'
                        if d['Status'] == 'Mixed'
                        else d['Status'],
                        'place': place,
                    }
                )
                commit()

        return self

    @db_session
    def process_metadata(self, db):
        """Create `metadata` table in database based on all data dictionaries
        ingested from the data source.

        """
        # assign dd type to each dd
        self.data_dictionary.loc[:, 'Type'] = 'Policy'
        self.data_dictionary_plans.loc[:, 'Type'] = 'Plan'
        self.data_dictionary_court_challenges.loc[:,
                                                  'Type'] = 'Court_Challenge'

        full_dd = pd.concat(
            [
                self.data_dictionary,
                self.data_dictionary_plans,
                self.data_dictionary_court_challenges,
            ]
        )

        # upsert metadata records
        self.create_metadata(db, full_dd)

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

        # upsert glossary terms
        self.create_glossary(db)

        # POLICY DATA # ------------------------------------------------------#
        def process_policy_data(self, db):
            # sort by policy ID
            self.data.sort_values('Unique ID')

            # remove records without a unique ID and other features
            self.data = self.data.loc[self.data['Flag for Review'] != True, :]
            self.data = self.data.loc[self.data['Unique ID'] != '', :]
            self.data = self.data.loc[
                self.data['Policy/law type'] != 'Non-policy guidance', :
            ]
            self.data = self.data.loc[
                self.data['Authorizing level of government'] != '', :
            ]

            # # do not ingest "Tribal nation" data until a plugin is written to
            # # determine `place` instance attributes for tribal nations
            # self.data = self.data.loc[
            #     self.data['Authorizing level of government']
            #     != 'Tribal nation', :
            # ]
            # self.data = self.data.loc[
            #     self.data['Authorizing country ISO']
            #     != '', :
            # ]
            self.data = self.data.loc[self.data['Policy description'] != '', :]
            self.data = self.data.loc[self.data['Effective start date'] != '', :]
            # self.data.loc[(self.data['Authorizing local area (e.g., county, city)']
            #                != '') & (self.data['Authorizing level of government'] != 'Local'), 'Authorizing local area (e.g., county, city)'] = ''

            # analyze for QA/QC and quit if errors detected
            valid = self.check(self.data)
            if not valid:
                print('Data are invalid. Please correct issues and try again.')
                # sys.exit(0)
            else:
                print('QA/QC found no issues. Continuing.')

            # set column names to database field names
            all_keys = select(
                (
                    i.ingest_field,
                    i.display_name,
                    i.field
                )
                for i in db.Metadata
                if i.class_name == 'Policy'
            )[:]

            # use field names instead of column headers for core dataset
            # TODO do this for future data tables as needed
            columns = dict()
            for ingest_field, display_name, db_field in all_keys:
                field = ingest_field if ingest_field != '' else db_field
                columns[display_name] = field
            self.data = self.data.rename(columns=columns)

            # format certain values
            for col in ('auth_entity.level', 'place.level'):
                for to_replace, value in (
                    ('State/Province (Intermediate area)', 'State / Province'),
                    ('Local area (county, city)', 'Local'),
                    ('Multiple countries/Global policy (e.g., UN, WHO, treaty organization policy)',
                     'Country'),
                    ('Multiple countries/Global policy (e.g., UN, WHO, treaty organization policies)',
                     'Country'),
                ):
                    self.data[col] = self.data[col].replace(
                        to_replace=to_replace,
                        value=value
                    )

            def assign_standardized_local_areas():
                """Overwrite values for local areas in free text column with
                names of local area instances in the linked column.

                Returns
                -------
                type
                    Description of returned object.

                """

                def assign_local_areas_str(
                    datum,
                    level_field_name,
                    db_field_name,
                    local_area_field_name,
                    local_area_arr_field_name
                ):
                    """Assign semicolon-delimited list of local area names to the
                    appropriate Policy instance data fields based on the local area
                    entity instances defined for it.

                    Parameters
                    ----------
                    datum : type
                        Description of parameter `datum`.
                    level_field_name : type
                        Description of parameter `level_field_name`.
                    db_field_name : type
                        Description of parameter `db_field_name`.
                    local_area_field_name : type
                        Description of parameter `local_area_field_name`.

                    Returns
                    -------
                    type
                        Description of returned object.

                    """
                    if local_area_arr_field_name in datum:
                        local_areas_str_tmp = datum[local_area_arr_field_name]
                        local_areas_str = "; ".join(local_areas_str_tmp)
                        datum[local_area_field_name] = local_areas_str
                    else:
                        datum[local_area_field_name] = ''

                # assign local areas cols of policy data based on local area
                # database linkages
                print(
                    '\n\nAssigning local and intermediate area names from local area database...')
                then = time.perf_counter()
                local_areas = self.local_areas.to_dict(orient='records')
                for i, d in self.data.iterrows():

                    # Local areas
                    assign_local_areas_str(
                        datum=d,
                        level_field_name='auth_entity.level',
                        db_field_name='Policy Database',
                        local_area_field_name='auth_entity.area2',
                        local_area_arr_field_name='Authorizing local area (e.g., county, city) names - Linked to Local Area Database'
                    )
                    assign_local_areas_str(
                        datum=d,
                        level_field_name='place.level',
                        db_field_name='Policy Database 2',
                        local_area_field_name='place.area2',
                        local_area_arr_field_name='Affected local area (e.g., county, city) names - Linked to Local Area Database'
                    )

                    # Intermediate areas
                    assign_local_areas_str(
                        datum=d,
                        level_field_name='auth_entity.level',
                        db_field_name='Policy Database',
                        local_area_field_name='auth_entity.area1',
                        local_area_arr_field_name='Auth state names (lookup)'
                    )
                    assign_local_areas_str(
                        datum=d,
                        level_field_name='place.level',
                        db_field_name='Policy Database 2',
                        local_area_field_name='place.area1',
                        local_area_arr_field_name='Aff state names (lookup)'
                    )

                now = time.perf_counter()
                sec = now - then
                print('Local area names assigned, sec: ' + str(sec))
            assign_standardized_local_areas()

            # create Policy instances
            self.create_policies(db)
        process_policy_data(self, db)

        # PLAN DATA # --------------------------------------------------------#
        def process_plan_data(self, db):
            # define data
            data = self.data_plans

            # sort by unique ID
            data.sort_values('Unique ID')

            # remove records without a unique ID and other features
            # TODO confirm these criteria
            data = data.loc[data['Unique ID'] != '', :]
            data = data.loc[data['Plan description'] != '', :]
            data = data.loc[data['Plan PDF'] != '', :]

            # analyze for QA/QC and quit if errors detected
            valid = self.check(data)
            if not valid:
                print('Data are invalid. Please correct issues and try again.')
                # sys.exit(0)
            else:
                print('QA/QC found no issues. Continuing.')

            # set column names to database field names
            all_keys = select(
                (
                    i.ingest_field,
                    i.display_name,
                    i.field
                )
                for i in db.Metadata
                if i.class_name == 'Plan'
            )[:]

            # use field names instead of column headers for core dataset
            # TODO do this for future data tables as needed
            columns = dict()
            for ingest_field, display_name, db_field in all_keys:
                field = ingest_field if ingest_field != '' else db_field
                columns[display_name] = field
            data = data.rename(columns=columns)
            self.data_plans = data

            # create Plan instances
            self.create_plans(db)
        process_plan_data(self, db)

        # FILES DATA # -------------------------------------------------------#
        # create and validate File instances (syncs the file objects to S3)
        self.create_files_from_attachments(db, 'Policy')
        self.create_files_from_attachments(db, 'Plan')
        self.create_files_from_urls(db)
        self.validate_docs(db)

        # PLACES DATA # ------------------------------------------------------#
        # create Auth_Entity and Place instances
        self.create_auth_entities_and_places(db)
        self.create_auth_entities_and_places_for_plans(db)

        # VERSION DATA # -----------------------------------------------------#
        # update version
        action, version = upsert(
            db.Version,
            {
                'type': 'Policy data',
            },
            {
                'date': date.today(),
            }
        )

        print('\nData ingest completed.')
        return self

    @db_session
    def post_process_places(self, db):
        print('[X] Splitting places where needed...')
        places_to_split_area2 = select(
            i for i in db.Place
            if ';' in i.area2
        )
        for p in places_to_split_area2:
            places_to_upsert = p.area2.split('; ')
            upserted_places = list()
            for p2 in places_to_upsert:
                instance = p.to_dict()
                instance['area2'] = p2

                get_keys = ['level', 'iso3', 'area1', 'area2']
                set_keys = ['dillons_rule', 'home_rule', 'country_name']

                get_data = {k: instance[k] for k in get_keys if k in instance}
                set_data = {k: instance[k] for k in set_keys if k in instance}

                action, place = upsert(
                    db.Place,
                    get_data,
                    set_data,
                )
                place.loc = get_place_loc(place)
                place.policies += p.policies
                place.plans += p.plans
                commit()
                upserted_places.append(place)
        places_to_split_area2.delete()

        places_to_split_iso3 = select(
            i for i in db.Place
            if ';' in i.iso3
            or i.loc == 'Multiple countries'
        )
        for p in places_to_split_iso3:
            places_to_upsert = p.iso3.split('; ')
            upserted_places = list()
            for p2 in places_to_upsert:
                instance = p.to_dict()
                instance['iso3'] = p2
                instance['country_name'] = get_name_from_iso3(p2)

                get_keys = ['level', 'iso3', 'area1', 'area2']
                set_keys = ['dillons_rule', 'home_rule', 'country_name']

                get_data = {k: instance[k] for k in get_keys if k in instance}
                set_data = {k: instance[k] for k in set_keys if k in instance}

                action, place = upsert(
                    db.Place,
                    get_data,
                    set_data,
                )
                place.loc = get_place_loc(place)
                place.policies += p.policies
                place.plans += p.plans
                commit()
                upserted_places.append(place)
        places_to_split_iso3.delete()
        commit()

    @db_session
    def post_process_policies(self, db, include_court_challenges=False):
        policy_sections = select(
            i for i in db.Policy
        )
        for p in policy_sections:
            # for travel restriction policies, set affected place to auth. pl.
            if p.primary_ph_measure == 'Travel restrictions':
                all_country = all(ae.place.level ==
                                  'Country' for ae in p.auth_entity)
                p.place = set()
                for ae in p.auth_entity:
                    p.place.add(ae.place)

            # create or add to policy numbers, which group policies
            policy_number_value = None

            # if a policy section had no policy number, use its ID instead
            if p.policy_number in (None, 0):
                policy_number_value = p.id
            else:
                policy_number_value = p.policy_number

            # if a policy number exists, then add the policy section to it,
            # otherwise create it and add the policy section to it
            policy_number_exists = \
                db.Policy_Number.exists(id=policy_number_value)
            if policy_number_exists:
                policy_number = db.Policy_Number.get(id=policy_number_value)
            else:
                policy_number = db.Policy_Number(id=policy_number_value)

            policy_number.policies.add(p)
            commit()

        # link policies to court challenges
        if include_court_challenges:
            for i, d in self.data_court_challenges.iterrows():
                if d['policies'] == '':
                    continue
                else:
                    court_challenge_id = int(d['id'])
                    court_challenge = db.Court_Challenge[court_challenge_id]
                    policies = select(
                        i for i in db.Policy
                        if i.source_id in d['policies']
                    )
                    for d in policies:
                        d.court_challenges.add(court_challenge)

    @db_session
    def post_process_policy_numbers(self, db):
        """Populate certain data fields for Policy_Number records.

        """
        policy_numbers = select(
            i for i in db.Policy_Number
        )
        for num in policy_numbers:
            for section in num.policies:
                # unique auth_entity instances
                num.auth_entities.add(section.auth_entity)

                # unique place instances
                num.places.add(section.place)

                # unique policy_names
                if section.policy_name is not None:
                    if num.names is not None:
                        cur_names = set(num.names)
                        cur_names.add(section.policy_name)
                        num.names = list(cur_names)
                    else:
                        num.names = [section.policy_name]

                # earliest effective start date
                if num.earliest_date_start_effective is None or \
                        num.earliest_date_start_effective > \
                        section.date_start_effective:
                    num.earliest_date_start_effective = \
                        section.date_start_effective

                # commit to db
                commit()


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
            if d[key] == 'N/A' or d[key] == 'NA' or d[key] == None or d[key] == '':
                if key in show_in_progress:
                    return 'In progress'
                else:
                    return 'Unspecified'
            else:
                return d[key]

        # Main #################################################################
        # retrieve keys needed to ingest data for Place, Auth_Entity, and
        # Auth_Entity.Place data fields.
        place_keys = select(
            i.ingest_field for i in db.Metadata if
            i.entity_name == 'Place'
            and i.ingest_field != ''
            and i.export == True)[:][:]

        auth_entity_keys = select(
            i.ingest_field for i in db.Metadata if
            i.ingest_field != ''
            and i.entity_name == 'Auth_Entity' and i.export == True)[:][:]

        auth_entity_place_keys = select(
            i.ingest_field for i in db.Metadata if
            i.ingest_field != ''
            and i.entity_name == 'Auth_Entity.Place' and i.export == True)[:][:]

        # track upserted records
        n_inserted = 0
        n_updated = 0
        n_deleted = 0
        n_inserted_auth_entity = 0
        n_updated_auth_entity = 0
        n_deleted_auth_entity = 0

        # for each row of the data

        for i, d in self.data.iterrows():

            # if unique ID is not an integer, skip
            # TODO handle on ingest
            try:
                int(d['id'])
            except:
                continue

            if reject(d):
                continue

            ## Add places ######################################################
            # determine whether the specified instance has been defined yet, and
            # if not, add it.
            instance_data = {key: formatter(key, d)
                             for key in place_keys if key in d}

            # the affected place is different from the auth entity's place if it
            # exists (is defined in the record) and is different
            affected_diff_from_auth = d['place.level'] != None and \
                d['place.level'] != '' and d['place.iso3'] != None and \
                d['place.iso3'] != ''

            # get or create the place affected
            place_affected = None
            if affected_diff_from_auth:
                place_affected_instance_data = {
                    key.split('.')[-1]: formatter(key, d) for key in place_keys if key in d}

                # convert iso3 from list to str
                if type(place_affected_instance_data['iso3']) == list:
                    place_affected_instance_data['iso3'] = \
                        '; '.join(place_affected_instance_data['iso3'])

                # perform upsert using get and set data fields
                place_affected_instance_data['country_name'] = \
                    get_name_from_iso3(place_affected_instance_data['iso3'])
                place_affected_get_keys = ['level', 'iso3', 'area1', 'area2']
                place_affected_set_keys = [
                    'dillons_rule', 'home_rule', 'country_name']
                place_affected_get = {k: place_affected_instance_data[k]
                                      for k in place_affected_get_keys if k in place_affected_instance_data}
                place_affected_set = {k: place_affected_instance_data[k]
                                      for k in place_affected_set_keys if k in place_affected_instance_data}

                action, place_affected = upsert(
                    db.Place,
                    place_affected_get,
                    place_affected_set,
                )
                place_affected.loc = get_place_loc(place_affected)
                if action == 'update':
                    n_updated += 1
                elif action == 'insert':
                    n_inserted += 1

            # get or create the place of the auth entity
            auth_entity_place_instance_data = {key.split('.')[-1]: formatter(
                key, d) for key in auth_entity_place_keys +
                ['home_rule', 'dillons_rule'] if key in d}

            # convert iso3 from list to str
            if type(auth_entity_place_instance_data['iso3']) == list:
                auth_entity_place_instance_data['iso3'] = \
                    '; '.join(auth_entity_place_instance_data['iso3'])

            # perform upsert using get and set data fields
            # place_affected_instance_data['country_name'] = \
            auth_entity_place_instance_data['country_name'] = \
                get_name_from_iso3(auth_entity_place_instance_data['iso3'])
            get_keys = ['level', 'iso3', 'area1', 'area2']
            set_keys = ['dillons_rule', 'home_rule', 'country_name']
            place_auth_get = {k: auth_entity_place_instance_data[k]
                              for k in get_keys if k in auth_entity_place_instance_data}
            place_auth_set = {k: auth_entity_place_instance_data[k]
                              for k in set_keys if k in auth_entity_place_instance_data}

            action, place_auth = upsert(
                db.Place,
                place_auth_get,
                place_auth_set,
            )
            place_auth.loc = get_place_loc(place_auth)
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
            db.Policy[d['id']].auth_entity = set()
            for dd in raw_data:

                # get or create auth entity
                auth_entity_instance_data = {key: formatter(
                    key, dd) for key in auth_entity_keys if key in dd}
                auth_entity_instance_data['place'] = place_auth

                # perform upsert using get and set data fields
                get_keys = ['name', 'office', 'place']
                action, auth_entity = upsert(
                    db.Auth_Entity,
                    {k: auth_entity_instance_data[k]
                        for k in get_keys if k in auth_entity_instance_data},
                    {},
                )
                if action == 'update':
                    n_updated_auth_entity += 1
                elif action == 'insert':
                    n_inserted_auth_entity += 1

                # link instance to required entities
                db.Policy[d['id']].auth_entity.add(auth_entity)
            commit()

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
        print('Total in database: ' + str(len(db.Place.select())))
        print('Deleted: ' + str(n_deleted))

        print('\n\n[8] Ingesting authorizing entities...')
        print('Total in database: ' + str(len(db.Auth_Entity.select())))
        print('Deleted: ' + str(n_deleted_auth_entity))

    @db_session
    def create_auth_entities_and_places_for_plans(self, db):
        """Create authorizing entity instances and place instances specifically
        related to plans (not policies, which have a different function:
        `create_auth_entities_and_places`).

        Parameters
        ----------
        db : type
            PonyORM database instance

        Returns
        -------
        self

        """

        # Local methods ########################################################
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
            if d[key] == 'N/A' or d[key] == 'NA' or d[key] == None or d[key] == '':
                if key in show_in_progress:
                    return 'In progress'
                else:
                    return 'Unspecified'
            else:
                return d[key]

        # Main #################################################################
        # retrieve keys needed to ingest data for Place, Auth_Entity, and
        # Auth_Entity.Place data fields.
        place_keys = select(
            i.ingest_field for i in db.Metadata
            if i.entity_name == 'Place'
            and i.ingest_field != ''
            and i.export == True
            and i.class_name == 'Plan'
        )[:][:]

        auth_entity_keys = select(
            i.ingest_field for i in db.Metadata
            if i.ingest_field != ''
            and i.entity_name == 'Auth_Entity'
            and i.export == True
            and i.class_name == 'Plan'
        )[:][:]

        auth_entity_place_keys = select(
            i.ingest_field for i in db.Metadata
            if i.ingest_field != ''
            and i.entity_name == 'Auth_Entity.Place'
            # and i.export == True
            and i.class_name == 'Plan'
        )[:][:]

        # track upserted records
        n_inserted = 0
        n_updated = 0
        n_deleted = 0
        n_inserted_auth_entity = 0
        n_updated_auth_entity = 0
        n_deleted_auth_entity = 0

        # for each row of the data
        for i, d in self.data_plans.iterrows():

            # if unique ID is not an integer, skip
            # TODO handle on ingest
            try:
                int(d['id'])
            except:
                continue

            if reject(d):
                continue

            ## Add places ######################################################
            # determine whether the specified instance has been defined yet, and
            # if not, add it.
            instance_data = {key: formatter(key, d)
                             for key in place_keys if key in d}

            # get or create the place affected (for plans, will always be the
            # same as authorizing entity's place, for now)
            place_affected = None

            # get or create the place of the auth entity
            auth_entity_place_instance_data = {key.split('.')[-1]: formatter(
                key, d) for key in auth_entity_place_keys if key in d}

            # convert iso3 from list to str
            if type(auth_entity_place_instance_data['iso3']) == list:
                auth_entity_place_instance_data['iso3'] = \
                    '; '.join(auth_entity_place_instance_data['iso3'])

            # perform upsert using get and set data fields
            auth_entity_place_instance_data['country_name'] = \
                get_name_from_iso3(auth_entity_place_instance_data['iso3'])

            # determine a level based on the populated fields
            if d['org_type'] != 'Government':
                level = d['org_type']
            elif auth_entity_place_instance_data['area2'].strip() != '' and \
                    auth_entity_place_instance_data['area2'] != 'Unspecified' and \
                    auth_entity_place_instance_data['area2'] is not None:
                level = 'Local'
            elif auth_entity_place_instance_data['area1'].strip() != '' and \
                    auth_entity_place_instance_data['area1'] != 'Unspecified' and \
                    auth_entity_place_instance_data['area1'] is not None:
                level = 'State / Province'
            elif auth_entity_place_instance_data['iso3'].strip() != '' and \
                    auth_entity_place_instance_data['iso3'] != 'Unspecified' and \
                    auth_entity_place_instance_data['iso3'] is not None:
                level = 'Country'
            else:
                print('place')
                print(place)
                input('ERROR: Could not determine a `level` for place')

            # assign synthetic "level"
            auth_entity_place_instance_data['level'] = level

            get_keys = ['level', 'iso3', 'area1', 'area2']
            set_keys = ['country_name']
            place_auth_get = {k: auth_entity_place_instance_data[k]
                              for k in get_keys if k in auth_entity_place_instance_data}
            place_auth_set = {k: auth_entity_place_instance_data[k]
                              for k in set_keys if k in auth_entity_place_instance_data}

            action, place_auth = upsert(
                db.Place,
                place_auth_get,
                place_auth_set,
            )
            place_auth.loc = get_place_loc(place_auth)
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
            db.Plan[d['id']].place = place_affected

            ## Add auth_entities ###############################################
            # parse auth entities in raw data record (there may be more than
            # one defined for each record)

            # for each individual auth entity
            db.Plan[d['id']].auth_entity = set()
            for auth_entity_instance_data in \
                    [{'name': d['org_name'], 'place': place_auth}]:

                # perform upsert using get and set data fields
                get_keys = ['name', 'place']
                action, auth_entity = upsert(
                    db.Auth_Entity,
                    {k: auth_entity_instance_data[k]
                        for k in get_keys if k in auth_entity_instance_data},
                    {},
                )
                if action == 'update':
                    n_updated_auth_entity += 1
                elif action == 'insert':
                    n_inserted_auth_entity += 1

                # link instance to required entities
                db.Plan[d['id']].auth_entity.add(auth_entity)
            commit()

        ## Delete unused instances #############################################
        # delete auth_entities that are not used
        auth_entities_to_delete = select(
            i for i in db.Auth_Entity
            if len(i.policies) == 0 and
            len(i.plans) == 0
        )
        if len(auth_entities_to_delete) > 0:
            auth_entities_to_delete.delete()
            n_deleted_auth_entity += len(auth_entities_to_delete)
            commit()

        # delete places that are not used
        places_to_delete = select(
            i for i in db.Place
            if len(i.policies) == 0
            and len(i.auth_entities) == 0
            and len(i.plans) == 0
        )
        if len(places_to_delete) > 0:
            places_to_delete.delete()
            n_deleted += len(places_to_delete)
            commit()

        print('\n\n[7] Ingesting places...')
        print('Total in database: ' + str(len(db.Place.select())))
        print('Deleted: ' + str(n_deleted))

        print('\n\n[8] Ingesting authorizing entities...')
        print('Total in database: ' + str(len(db.Auth_Entity.select())))
        print('Deleted: ' + str(n_deleted_auth_entity))

    @db_session
    def create_policies(self, db):
        """Create policy instances.
        TODO generalize to plans, etc.

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

        # retrieve data field keys for policies
        keys = select(
            i.field for i in db.Metadata
            if i.entity_name == 'Policy'
            and i.class_name == 'Policy'
            and i.field != 'id'
            and i.ingest_field != ''
        )[:]

        # maintain dict of attributes to set post-creation
        post_creation_attrs = defaultdict(dict)

        def formatter(key, d):
            unspec_val = 'In progress' if key in show_in_progress else 'Unspecified'
            if key.startswith('date_'):
                if d[key] == '' or d[key] is None or d[key] == 'N/A' or d[key] == 'NA':
                    return None
                elif len(d[key].split('/')) == 2:
                    print(f'''Unexpected format for `{key}`: {d[key]}\n''')
                    return unspec_val
                else:
                    return d[key]
            elif key == 'policy_number':
                if d[key] != '':
                    return int(d[key])
                else:
                    return None
            elif d[key] == 'N/A' or d[key] == 'NA' or d[key] == '':
                if key in ('prior_policy'):
                    return set()
                else:
                    return unspec_val
            elif key == 'id':
                return int(d[key])
            elif key in ('prior_policy'):
                post_creation_attrs[d['id']]['prior_policy'] = set(d[key])
                return set()
            elif type(d[key]) != str and iterable(d[key]):
                if len(d[key]) > 0:
                    return "; ".join(d[key])
                else:
                    return unspec_val
            return d[key]

        # track upserted records
        upserted = set()
        n_inserted = 0
        n_updated = 0

        for i, d in self.data.iterrows():

            # if unique ID is not an integer, skip
            # TODO handle on ingest
            try:
                int(d['id'])
            except:
                continue

            if reject(d):
                continue

            # upsert policies
            action, instance = upsert(
                db.Policy,
                {'id': d['id']},
                {key: formatter(key, d) for key in keys if key in d},
                skip=['prior_policy']
            )
            if action == 'update':
                n_updated += 1
            elif action == 'insert':
                n_inserted += 1
            upserted.add(instance)

        for i, d in self.data.iterrows():
            # if unique ID is not an integer, skip
            # TODO handle on ingest
            try:
                int(d['id'])
            except:
                continue

            if reject(d):
                continue

            # upsert policies
            # TODO consider how to count these updates, since they're done
            # after new instances are created (if counting them at all)
            if 'prior_policy' in d and d['prior_policy'] != '':
                prior_policies = list()
                for source_id in d['prior_policy']:
                    prior_policy_instance = select(
                        i for i in db.Policy if i.source_id == source_id).first()
                    if prior_policy_instance is not None:
                        prior_policies.append(prior_policy_instance)
                upsert(
                    db.Policy,
                    {'id': d['id']},
                    {'prior_policy': prior_policies},
                )

        # delete all records in table but not in ingest dataset
        n_deleted = db.Policy.delete_2(upserted)
        commit()
        print('Inserted: ' + str(n_inserted))
        print('Updated: ' + str(n_updated))
        print('Deleted: ' + str(n_deleted))

    @db_session
    def create_plans(self, db):
        """Create plan instances.

        Parameters
        ----------
        db : type
            Description of parameter `db`.

        Returns
        -------
        type
            Description of returned object.

        """
        print('\n\n[3b] Ingesting plan data...')

        # retrieve data field keys for plans
        keys = select(
            i.field for i in db.Metadata
            if i.entity_name == 'Plan'
            and i.field != 'id'
            and i.ingest_field != ''
            and i.class_name == 'Plan'
            and i.field != 'policy'
        )[:]

        # maintain dict of attributes to set post-creation
        post_creation_attrs = defaultdict(dict)

        def formatter(key, d):
            unspec_val = 'In progress' if key in show_in_progress else 'Unspecified'
            if key.startswith('date_'):
                if d[key] == '' or d[key] is None or d[key] == 'N/A' or d[key] == 'NA':
                    return None
                elif len(d[key].split('/')) == 2:
                    print(f'''Unexpected format for `{key}`: {d[key]}\n''')
                    return unspec_val
                else:
                    return d[key]
            elif key == 'policy_number' or key == 'n_phases':
                if d[key] != '':
                    return int(d[key])
                else:
                    return None
            elif d[key] == 'N/A' or d[key] == 'NA' or d[key] == '':
                if key in ('policy'):
                    return set()
                else:
                    return unspec_val
            elif key == 'id':
                return int(d[key])
            elif key in ('policy'):
                post_creation_attrs[d['id']]['policy'] = set(d[key])
                return set()
            elif type(d[key]) != str and iterable(d[key]):
                if len(d[key]) > 0:
                    return "; ".join(d[key])
                else:
                    return unspec_val
            return d[key]

        # track upserted records
        upserted = set()
        n_inserted = 0
        n_updated = 0

        # define data
        data = self.data_plans

        for i, d in data.iterrows():

            # if unique ID is not an integer, skip
            # TODO handle on ingest
            try:
                int(d['id'])
            except:
                continue

            if reject(d):
                continue

            # upsert policies
            action, instance = upsert(
                db.Plan,
                {'id': d['id']},
                {key: formatter(key, d) for key in keys},
                skip=['policy']
            )
            if action == 'update':
                n_updated += 1
            elif action == 'insert':
                n_inserted += 1
            upserted.add(instance)

        # delete all records in table but not in ingest dataset
        n_deleted = db.Plan.delete_2(upserted)
        commit()
        print('Inserted: ' + str(n_inserted))
        print('Updated: ' + str(n_updated))
        print('Deleted: ' + str(n_deleted))

    @db_session
    def create_court_challenges(self, db):
        """Create court challenge instances.

        Parameters
        ----------
        db : type
            Description of parameter `db`.

        Returns
        -------
        type
            Description of returned object.

        """
        print('\n\n[XX] Ingesting court challenge data...')

        # skip linked fields and assign them later
        linked_fields = ('policies', 'matter_numbers')
        set_fields = ('id',)

        # retrieve data field keys for court challenge
        get_fields = select(
            i.field for i in db.Metadata
            if i.entity_name == 'Court_Challenge'
            and i.ingest_field != ''
            and i.class_name == 'Court_Challenge'
            and i.field not in linked_fields
            and i.field not in set_fields
            and i.export is True
            or i.field == 'source_id'
        )[:]

        # maintain dict of attributes to set post-creation
        post_creation_attrs = defaultdict(dict)

        def formatter(key, d):

            # define keys that, if sets, should be converted to strings or bool
            set_to_str = ('policy_or_law_name', 'case_name')
            set_to_bool = ('legal_challenge',)
            # correct errors in date entry
            if key.startswith('date_'):
                if d[key] == '' or d[key] is None or d[key] == 'N/A' or d[key] == 'NA':
                    return None
                elif len(d[key].split('/')) == 2:
                    print(f'''Unexpected format for `{key}`: {d[key]}\n''')
                    return unspec_val
                else:
                    return d[key]
            # parse IDs as integers
            elif key == 'id':
                return int(d[key])
            # parse sets, including sets of strs that should be bools
            elif type(d[key]) != str and iterable(d[key]):
                if key in set_to_str:
                    if len(d[key]) > 0:
                        return "; ".join(list(set(d[key])))
                    else:
                        return None
                elif key in set_to_bool:
                    if len(d[key]) > 0:
                        bool_list = list(
                            map(
                                str_to_bool,
                                d[key]
                            )
                        )
                        # use first item in `bool_list` because
                        # `legal_challenge` field should be unary
                        return bool_list[0]
                    else:
                        return None
                elif len(d[key]) > 0:
                    return list(set(d[key]))
                else:
                    return None
            elif key in set_to_bool:
                return None
            else:
                # if data are blank, return empty array or None, as appropriate
                if d[key] == '':
                    is_nullable = getattr(db.Court_Challenge, key).nullable
                    if is_nullable:
                        is_str_arr = getattr(db.Court_Challenge, key).py_type \
                            == StrArray
                        if is_str_arr:
                            return list()
                        else:
                            return None
                return d[key]

        # track upserted records
        upserted = set()
        n_inserted = 0
        n_updated = 0

        # define data
        data = self.data_court_challenges

        for i, d in data.iterrows():

            # if unique ID is not an integer, skip
            # TODO handle on ingest
            try:
                int(d['id'])
            except:
                continue

            if reject(d):
                continue

            # upsert instances: court challenges
            action, instance = upsert(
                db.Court_Challenge,
                {'id': d['id']},
                {field: formatter(field, d)
                 for field in get_fields if field in d},
            )
            if action == 'update':
                n_updated += 1
            elif action == 'insert':
                n_inserted += 1
            upserted.add(instance)

        # delete all records in table but not in ingest dataset
        n_deleted = db.Court_Challenge.delete_2(upserted)
        commit()
        print('Inserted: ' + str(n_inserted))
        print('Updated: ' + str(n_updated))
        print('Deleted: ' + str(n_deleted))

        # join matter numbers to court challenges
        print('\n\nAdding matter numbers to court challenges...')

        for i, d in self.data_matter_numbers.iterrows():
            if d['Court challenge link'] == '':
                continue
            else:
                court_challenges = select(
                    i for i in db.Court_Challenge
                    if i.source_id in d['Court challenge link']
                )
                for dd in court_challenges:
                    matter_number = int(d['Matter number'])
                    if dd.matter_numbers is None:
                        dd.matter_numbers = [matter_number]
                    else:
                        new_matter_numbers = set(
                            dd.matter_numbers)
                        new_matter_numbers.add(matter_number)
                        dd.matter_numbers = list(new_matter_numbers)
        print('Done.')
        return self

    @db_session
    def create_metadata(self, db, full_dd):
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

        for i, d in full_dd.iterrows():
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
                'notes': d['Notes'] if not pd.isna(d['Notes']) else '',
                'order': d['Order'],
                'export': d['Export?'] == True,
            }
            action, instance = upsert(db.Metadata, {
                'field': d['Database field name'],
                'entity_name': d['Database entity'],
                'class_name': d['Type']
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
                'class_name': 'Policy'
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
                'field': 'loc',
                'entity_name': 'Place',
                'class_name': 'Plan'
            }, {
                'ingest_field': 'loc',
                'display_name': 'Country / Specific location',
                'colgroup': '',
                'definition': 'The location affected by the plan',
                'possible_values': 'Any text',
                'notes': '',
                'order': 0,
                'export': False,
            }), ({
                'field': 'source_id',
                'entity_name': 'Policy',
                'class_name': 'Policy',
            }, {
                'ingest_field': 'source_id',
                'display_name': 'Source ID',
                'colgroup': '',
                'definition': 'The unique ID of the record in the original dataset',
                'possible_values': 'Any text',
                'order': 0,
                'notes': '',
                'export': False,
            }), ({
                'field': 'source_id',
                'entity_name': 'Plan',
                'class_name': 'Plan',
            }, {
                'ingest_field': 'source_id',
                'display_name': 'Source ID',
                'colgroup': '',
                'definition': 'The unique ID of the record in the original dataset',
                'possible_values': 'Any text',
                'order': 0,
                'notes': '',
                'export': False,
            }), ({
                'field': 'source_id',
                'entity_name': 'Court_Challenge',
                'class_name': 'Court_Challenge',
            }, {
                'ingest_field': 'source_id',
                'display_name': 'Source ID',
                'colgroup': '',
                'definition': 'The unique ID of the record in the original dataset',
                'possible_values': 'Any text',
                'order': 0,
                'notes': '',
                'export': False,
            }), ({
                'field': 'date_end_actual_or_anticipated',
                'entity_name': 'Policy',
                'class_name': 'Policy',
            }, {
                'ingest_field': '',
                'display_name': 'Policy end date',
                'colgroup': '',
                'definition': 'The date on which the policy or law will (or did) end',
                'possible_values': 'Any date',
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
    def create_glossary(self, db):
        """Create glossary instances if they do not exist. If they do exist,
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
        print('\n\n[2b] Ingesting glossary...')
        colgroup = ''
        upserted = set()
        n_inserted = 0
        n_updated = 0
        for i, d in self.glossary.iterrows():
            if d['Key Term'] is None or d['Key Term'].strip() == '':
                continue
            attributes = {
                'definition': d['Definition'],
                'reference': d['Reference'],
                'entity_name': d['Database entity'],
                'field': d['Database field name'],
            }

            action, instance = upsert(db.Glossary, {
                'term': d['Key Term'],
                'subterm': d['Field Value'],
            }, attributes)
            if action == 'update':
                n_updated += 1
            elif action == 'insert':
                n_inserted += 1
            upserted.add(instance)

        # delete all records in table but not in ingest dataset
        n_deleted = db.Glossary.delete_2(upserted)
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

            # if unique ID is not an integer, skip
            # TODO handle on ingest
            try:
                int(d['id'])
            except:
                continue

            # if an attachment is available, skip
            attachment_available = d['attachment_for_policy'] is not None and \
                len(d['attachment_for_policy']) > 0

            if reject(d) or attachment_available:

                continue

            instance_data = {key.split('_', 1)[1]: d[key]
                             for key in policy_doc_keys}

            if not attachment_available and \
                (instance_data['filename'] is None or
                 instance_data['filename'].strip() == ''):
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

        # # delete any files that were not upserted from the database
        # to_delete = select(
        #     i for i in db.File
        #     if i not in upserted
        #     and not i.airtable_attachment
        # )
        # n_deleted = len(to_delete)
        # to_delete.delete()
        commit()

        print('Inserted: ' + str(n_inserted))
        print('Updated: ' + str(n_updated))
        # print('Deleted (still in S3): ' + str(n_deleted))

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
    def create_files_from_attachments(self, db, entity_class_name):
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
        print(
            f'''\n\n[4] Ingesting files from Airtable attachments for {entity_class_name}...''')

        # get entity class to use
        entity_class = getattr(db, entity_class_name)

        # TODO ensure correct set of keys used based on data being parsed
        # keys for policy document PDFs
        policy_doc_keys = {
            'attachment_for_policy': {
                'data_source': 'policy_data_source',
                'name': 'policy_name',
                'type': 'policy',
            },
        }

        # ...for plans
        plan_doc_keys = {
            'attachment_for_plan': {
                'data_source': 'plan_data_source',
                'name': 'name',
                'type': 'plan'
            },
            'attachment_for_plan_announcement': {
                'data_source': 'announcement_data_source',
                'name': 'name',
                'type': 'plan_announcement'
            },
        }

        docs_by_id = dict()

        # track missing PDF filenames and source URLs
        missing_filenames = list()

        # track upserted PDFs -- if there are any filenames in it that are not
        # in the S3 bucket, upload those files to S3
        upserted = set()
        n_inserted = 0
        n_updated = 0
        n_deleted = 0

        # get data to use
        data = self.data if entity_class == db.Policy else self.data_plans

        # get set of keys to use
        doc_keys = policy_doc_keys if entity_class == db.Policy \
            else plan_doc_keys

        # track types added to assist deletion
        types = set()

        # for each record in the raw data, potentially create file(s)
        # TODO replace `self.data` with an argument-specified dataset
        for i, d in data.iterrows():

            # if unique ID is not an integer, skip
            # TODO handle on ingest
            try:
                int(d['id'])
            except:
                continue

            if reject(d):
                continue

            for key in doc_keys:
                if d[key] is not None and len(d[key]) > 0:
                    # remove all non-airtable attachments of this type
                    type = doc_keys[key]['type']
                    types.add(type)
                    policy = entity_class[d['id']]
                    to_delete = select(
                        i for i in policy.file
                        if i.type == type
                        and not i.airtable_attachment
                    ).delete()
                    for dd in d[key]:
                        # create file key
                        file_key = dd['id'] + ' - ' + dd['filename'] + '.pdf'

                        # check if file exists already
                        # define get data
                        get_data = {
                            'filename': file_key
                        }

                        # define set data
                        set_data = {
                            'name': dd['filename'],
                            'type': type,
                            'data_source': d[doc_keys[key]['data_source']],
                            'permalink': dd['url'],
                            'airtable_attachment': True,
                        }

                        # perform upsert and link to relevant policy/plan
                        action, file = upsert(db.File, get_data, set_data)
                        if action == 'update':
                            n_updated += 1
                        elif action == 'insert':
                            n_inserted += 1
                        upserted.add(file)

                        # link file to policy
                        entity_class[d['id']].file.add(file)

        # delete any files that were not upserted from the database
        to_delete = select(
            i for i in db.File
            if i not in upserted
            and i.airtable_attachment
            and i.type in types
        )
        n_deleted = len(to_delete)
        to_delete.delete()
        commit()

        print('Inserted: ' + str(n_inserted))
        print('Updated: ' + str(n_updated))
        print('Deleted (still in S3): ' + str(n_deleted))

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
        n_checked = 0
        could_not_download = set()
        missing_filenames = set()
        for file in files:
            n_checked += 1
            print(f'''Checking file {n_checked} of {len(files)}...''')
            if file.filename is not None:
                file_key = file.filename
                if file_key in self.s3_bucket_keys:
                    # print('\nFile found')
                    n_valid += 1
                    pass
                elif (
                    file.data_source is None or file.data_source.strip() == ''
                ) and (
                    file.permalink is None or file.permalink.strip() == ''
                ):
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
                        if file is not None:
                            file.delete()
                        commit()
                        could_not_download.add(file_url)
                        n_failed += 1
            else:
                print("Skipping, no file associated")

        print('Valid: ' + str(n_valid))
        print('Added to S3: ' + str(n_added))
        print('Missing (no URL or filename): ' + str(n_missing))
        print('Failed to fetch from URL: ' + str(n_failed))
        if n_missing > 0:
            missing_filenames = list(missing_filenames)
            missing_filenames.sort()
            print(
                f'''\n{bcolors.BOLD}[Warning] URLs or filenames were not provided for {n_missing} files with the following names:{bcolors.ENDC}''')
            print(bcolors.BOLD + str(", ".join(missing_filenames)) + bcolors.ENDC)

        if n_failed > 0:
            could_not_download = list(could_not_download)
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
            print(data[dupes == True].loc[:, 'Unique ID'])
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
