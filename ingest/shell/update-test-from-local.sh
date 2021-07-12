#!/bin/bash
##
# Update the COVID AMP test database from a local version.
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
--no-acl \
--no-owner \
--host "localhost" \
--port "5432" \
--username $username \
--dbname $dblocal \
--verbose \
-F d -f "$now-local" && \
cd ../../..;

# # dump test
# cd ingest/backups/test;
# pg_dump \
# --host $dbprodhost \
# --port "5432" \
# --username "talus" \
# --dbname "covid-npi-policy-test" \
# -F d -f "$now-test";
# cd ../../..;

# drop test
psql \
--host $dbprodhost \
--port "5432" \
--username "talus" \
--dbname "covid-npi-policy-test" \
-c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;" && \

# restore test from local dump
cd ingest/backups/local;
pg_restore \
--no-acl \
--no-owner \
--host $dbprodhost \
--port "5432" \
--username "talus" \
--dbname "covid-npi-policy-test" \
--format=d --verbose "$now-local" && \
cd ../../..;

# restart API server
aws elasticbeanstalk restart-app-server \
--environment-name covid-npi-policy-api-dev \
--region us-west-2;
