import json
import os
import boto3
import base64

# retrieve AWS Secret, used to define connection string for database in
# production mode
def get_secret(
    secret_name="talus-prod-1",
    region_name="us-west-1",
    profile="default",
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
    if os.environ.get("DEBUG") != "1":
        session = boto3.session.Session()
    else:
        session = boto3.session.Session(profile_name=profile)

    client = session.client(service_name="secretsmanager", region_name=region_name)

    # attempt to retrieve the secret, and throw a series of exceptions if the
    # attempt fails. See link below for more information.
    # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        print(e)
        if e.response["Error"]["Code"] == "DecryptionFailureException":
            # Secrets Manager can't decrypt the protected secret text using the
            # provided KMS key.
            raise e
        elif e.response["Error"]["Code"] == "InternalServiceErrorException":
            # An error occurred on the server side.
            raise e
        elif e.response["Error"]["Code"] == "InvalidParameterException":
            # You provided an invalid value for a parameter.
            raise e
        elif e.response["Error"]["Code"] == "InvalidRequestException":
            # You provided a parameter value that is not valid for the
            # current state
            # of the resource.
            raise e
        elif e.response["Error"]["Code"] == "ResourceNotFoundException":
            # We can't find the resource that you asked for.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these
        # fields will be populated.
        if "SecretString" in get_secret_value_response:
            secret = get_secret_value_response["SecretString"]
            return secret
        else:
            decoded_binary_secret = base64.b64decode(
                get_secret_value_response["SecretBinary"]
            )
            return decoded_binary_secret


class Config:
    def __init__(self):
        secret = json.loads(get_secret())

        self.db = {}
        keys = ("username", "host", "password", "database")
        if not all(k in secret for k in keys if k != "database"):
            raise ValueError("Secret did not contain all required config vars")
        for key in keys:
            self.db[key] = os.environ.get(key, secret.get(key))

        if os.getenv("DEBUG") == "1":
            print(self.db)

        # Debug mode is not used.
        self.debug = False

        # validate config
        if any(self.db[key] is None for key in keys):
            raise ValueError(
                f'Missing values in self.db, must define all of: {", ".join(keys)}'
            )

    # Instance methods
    # To string
    def __str__(self):
        return print(self.__dict__)

    # Get item from config file (basically, a key-value pair)
    def __getitem__(self, key):
        return self.__dict__[key]

    # Set item from config file
    def __setitem__(self, key, value):
        self.__dict__[key] = value
