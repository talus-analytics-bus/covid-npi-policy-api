#!/bin/bash
##
# Update the COVID AMP case and death data in a local database from external
# data sources.
#
# TODO Add db-config params as optional args. to `ingest_caseload.py`; goal is
# to streamline process of defining db-config params.
##

programname=$0

function usage {
    echo "usage: $programname [username]"
    echo "  username    your local pgsql server username"
    echo "  dblocal     the name of the COVID AMP database on your local server with which you are updating"
    exit 1
}

username=${1?Provide your local pgsql server username in first argument};
dblocal=${2?Provide the name of the database on your local server with which you are updating in second argument};

# ingest latest NYT caseload data
python ingest_caseload.py -g -s -gd -c;

# NOTE: The code below is commented out because updates to relation `version`
# are now totally handled in Python scripts.
# # increment version table caseload dates
# psql \
# --host "localhost" \
# --port "5432" \
# --username $username \
# --dbname $dblocal < "sh/version-increment-caseload-dates.sql";
