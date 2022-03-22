#!/bin/bash
##
# Update the COVID AMP policy, plan, and court challenges data in a local db.
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

# delete existing data
psql \
--host "localhost" \
--port "5432" \
--username $username \
--dbname $dblocal \
-c "TRUNCATE policy, plan, court_challenge, policy_number CASCADE;" && \

if python ingest.py --all; then
    echo "Ingest ran successfully"
else
    exit 1
fi

# Post-process data # ------------------------------------------------------- #
# Remove future policies
psql \
--host "localhost" \
--port "5432" \
--username $username \
--dbname $dblocal < "ingest/sql/delete_future_policies.sql";

# Remove policies without places
psql \
--host "localhost" \
--port "5432" \
--username $username \
--dbname $dblocal < "ingest/sql/delete_policies_without_places_pg.sql";

# Update `name_and_desc` field based on name and desc of policy/plan
psql \
--host "localhost" \
--port "5432" \
--username $username \
--dbname $dblocal < "ingest/sql/update_name_and_desc.sql";

# refresh materialized views
echo "\nRefreshing materialized views...";
psql \
--host "localhost" \
--port "5432" \
--username $username \
--dbname $dblocal < "ingest/sql/refresh_mvs.sql";
echo "Refreshed.\n";