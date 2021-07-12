#!/bin/bash
##
# Update the COVID AMP production database from a local version.
##

programname=$0

function usage {
    echo "usage: $programname [username]"
    echo "  username    your local pgsql server username"
    echo "  dblocal         the name of the COVID AMP database on your local server with which you are updating"
    echo "  dbprodhost      the host name of the production server to which you are updating"
    exit 1
}

now=$(date);
username=${1?Provide your local pgsql server username in first argument};
dblocal=${2?Provide the name of the database on your local server with which you are updating in second argument};
dbprodhost=${3?Provide the name of the host on AWS RDS to which you are updating in third argument};

# dump local
echo "Current date: $now";
cd ingest/backups/local;
pg_dump \
--host "localhost" \
--port "5432" \
--username $username \
--dbname $dblocal \
-F d -f "$now-local";
cd ../../..;

# dump prod'
cd ingest/backups/prod;
pg_dump \
--host $dbprodhost \
--port "5432" \
--username "talus" \
--dbname $dblocal \
-F d -f "$now-prod";
cd ../../..;

# drop prod
psql \
--host $dbprodhost \
--port "5432" \
--username "talus" \
--dbname $dblocal \
-c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;";

# restore prod from local dump
cd ingest/backups/local;
pg_restore \
--host $dbprodhost \
--port "5432" \
--username "talus" \
--dbname $dblocal \
--format=d --verbose "$now-local";
cd ../../..;

# restart API server
aws elasticbeanstalk restart-app-server --environment-name covid-npi-policy-api-prod --region us-west-2;
