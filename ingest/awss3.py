"""COVID AMP-specific Amazon Web Services S3 functions"""

from io import BytesIO
import logging
from typing import cast

import boto3
from requests import Response
from pony.orm import db_session, commit

from ingest.util import download_file

# define S3 client used for adding / checking for files in the S3
# storage bucket
s3 = boto3.client("s3")
S3_BUCKET_NAME = "covid-npi-policy-files"
# S3_BUCKET_NAME = "covid-npi-policy-storage"

logger = logging.getLogger(__name__)


def get_s3_bucket_keys():
    """Return all file keys, i.e., filenames, in `S3_BUCKET_NAME`"""
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
        if "NextContinuationToken" in response:
            nextContinuationToken = response["NextContinuationToken"]
        else:
            nextContinuationToken = None

        # for each response object, extract the key and add it to the
        # full list
        if response["KeyCount"] == 0:
            return list()
        for d in response["Contents"]:
            keys.append(d["Key"])

        # are there more keys to pull from the bucket?
        more_keys = nextContinuationToken is not None

    # return master list of all bucket keys
    return keys


@db_session
def add_file_to_s3_if_missing(file, s3_bucket_keys):
    file_key = file.filename
    if file_key in s3_bucket_keys:
        return "valid"
    elif (file.data_source is None or file.data_source.strip() == "") and (
        file.permalink in (None, "") or file.permalink.strip() == ""
    ):
        file.filename = None
        commit()
        return "missing"
    else:
        file_url = (
            file.permalink if file.permalink not in ("", None) else file.data_source
        )
        file_res_obj = cast(
            Response, download_file(file_url, file_key, None, as_object=True)
        )

        if file_res_obj and file_res_obj.content is not None:
            # handle text/html
            if file.permalink in ("", None):
                if file_res_obj.headers["content-type"].startswith("text/html"):
                    file.filename += file.filename + ".html"
                    file_key = file.filename

            # otherwise, assume PDF (do nothing, handled earlier)
            print(
                s3.put_object(
                    Body=file_res_obj.content,
                    Bucket=S3_BUCKET_NAME,
                    Key=file_key,
                    ACL="public-read",
                )
            )
            return "added"
        else:
            logger.info("Could not download file at URL " + str(file_url))
            if file is not None:
                file.delete()
            commit()
            return "failed"
