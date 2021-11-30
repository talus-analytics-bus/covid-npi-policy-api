#!/bin/bash
##
# Update the COVID AMP database.
##

programname=$0

function usage {
    echo "usage: $programname [username]"
    echo "  username        your local pgsql server username"
    echo "  dblocal         the name of the COVID AMP database on your local server with which you are updating"
    echo "  dbmetriclocal   the name of the Metrics database on your local server with which you are updating"
    echo "  dbprodhost      the host name of the production server to which you are updating"
    echo "  airtablekey     your Airtable API key"
    exit 1
}

now=$(date);
username=${1?Provide your local pgsql server username in first argument};
dblocal=${2?Provide the name of the database on your local server with which you are updating in second argument};
dbmetriclocal=${3?Provide the name of the database on your local server with which you are updating in third argument};
dbprodhost=${4?Provide the name of the database on your local server with which you are updating in fourth argument};
airtablekey=${5?Provide your Airtable API key in fifth argument};

echo Ingesting data and updating test site...;
export AIRTABLE_API_KEY=$airtablekey;
# bash ingest/sql/ingest-caseload.sh $username $dblocal && \
# bash ingest/shell/update-metric-prod-from-local.sh $username $dbmetriclocal $dbprodhost && \  
bash ingest/shell/ingest.sh $username $dblocal && \
bash ingest/shell/update-test-from-local.sh $username $dblocal $dbprodhost;