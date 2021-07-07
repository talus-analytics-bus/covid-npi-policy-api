#!/bin/bash
##
# Update the Metrics RDS database from a local version.
##

programname=$0

function usage {
    echo "usage: $programname [username]"
    echo "  username    your local pgsql server username"
    echo "  dblocal     the name of the Metrics database on your local server with which you are updating"
    echo "  dbprodhost      the host name of the production server to which you are updating"
    exit 1
}

now=$(date);
username=${1?Provide your local pgsql server username in first argument};
dblocal=${2?Provide the name of the database on your local server with which you are updating in second argument};
dbprodhost=${3?Provide the name of the database on your local server with which you are updating in third argument};

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
-F d -f "$now-metric" && \
cd ../../.. && \

# # dump prod
# cd ingest/backups/prod;
# pg_dump \
# --host $dbprodhost \
# --port "5432" \
# --username "talus" \
# --dbname "metric" \
# -F d -f "$now-prod";
# cd ../../..;

# drop prod
psql \
--host $dbprodhost \
--port "5432" \
--username "talus" \
--dbname "metric-amp" \
-c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;" && \

# restore prod from local dump
cd ingest/backups/local;
pg_restore \
--no-acl \
--no-owner \
--host $dbprodhost \
--port "5432" \
--username "talus" \
--dbname "metric-amp" \
--format=d --verbose "$now-metric";
