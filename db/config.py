# standard packages
import argparse
import os
import boto3
import base64
import configparser
import json

# standard modules
from botocore.exceptions import ClientError

# 3rd party modules
from pony import orm


def get_secret(
    secret_name="talus_dev_rds_secret",
    region_name="us-west-1",
    profile='default'
):
    """Retrieve an AWS Secret value, given valid connection parameters and
    assuming the server has access to a valid configuration profile.

    Parameters
    ----------
    secret_name : str
        The name of the secret to be retrieved from AWS Secrets.
    region_name : str
        The name of the region that secret is housed in.
    profile : str
        The name of the profile that should be used to connect to AWS Secrets.

    Returns
    -------
    dict
        The secret, as a set of key/value pairs.

    """

    # Create a Secrets Manager client using boto3
    if os.environ.get('PROD') == 'true':
        session = boto3.session.Session()
    else:
        session = boto3.session.Session(profile_name=profile)

    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    # attempt to retrieve the secret, and throw a series of exceptions if the
    # attempt fails. See link below for more information.
    # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        print(e)
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the
            # provided KMS key.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the
            # current state
            # of the resource.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these
        # fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return secret
        else:
            decoded_binary_secret = base64.b64decode(
                get_secret_value_response['SecretBinary']
            )
            return decoded_binary_secret


# check for config.ini and use that configuration for the PostgreSQL
# database if it's there
config = configparser.ConfigParser(allow_no_value=True)
config.read('./db/config-local.ini')

# collate parameters from INI file or from AWS Secrets Manager if that is
# not provided
conn_params = {'database': 'covid-npi-policy-test'}
no_config = (len(config) == 1 and len(config['DEFAULT']) == 0)

if os.environ.get('PROD') != 'true' and not no_config:
    # if not no_config:
    for d in config['DEFAULT']:
        print(d)
        conn_params[d] = config['DEFAULT'].get(d)
else:
    secret = json.loads(get_secret())
    conn_params['username'] = secret['username']
    conn_params['host'] = secret['host']
    conn_params['password'] = secret['password']
print(conn_params)

# init PonyORM database instance
db = orm.Database()

db.bind(
    'postgres',
    user=conn_params['username'],
    password=conn_params['password'],
    host=conn_params['host'],
    database=conn_params['database']
)
