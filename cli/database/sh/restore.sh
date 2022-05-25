#!/bin/sh
# Restore local db to talus-prod one
# Arguments: [username] [aws_db_name] [local_db_name]

SCRIPTDIR=$(dirname "$0")
ORIGDIR=$(pwd)
cd "$SCRIPTDIR" || exit 1

# get cloud postgresql host
PGUSER_LOCAL=${1:?Provide local PG username in first argument}
PGDB_CLOUD=${2:?Provide cloud PG database name in second argument}
PGDB_LOCAL=${3:?Provide local PG database name in third argument}
PGHOST_CLOUD=${4:-"talus-prod.cvsrrvlopzxr.us-west-1.rds.amazonaws.com"}
PGUSER_CLOUD=${5:-"talus"}

bold=$(tput bold)
normal=$(tput sgr0)

now=$(date) &&
    echo "Current date: $now" &&
    # backup local
    mkdir backups
cd backups && mkdir backup-local
cd backup-local &&
    pg_dump \
        --no-acl \
        --no-owner \
        --host "localhost" \
        --port "5432" \
        --username "$PGUSER_LOCAL" \
        --dbname "$PGDB_LOCAL" \
        -F d -f "$PGDB_LOCAL-local-$now" --verbose &&
    cd ../.. &&

    # create AWS if not exists
    createdb --host "$PGHOST_CLOUD" \
        --port "5432" \
        --username "$PGUSER_CLOUD" \
        "$PGDB_CLOUD"

# backup aws
printf "${bold}\n\nDumping AWS database...\n${normal}" &&
    mkdir backups
cd backups && mkdir backup-cloud
cd backup-cloud &&
    pg_dump \
        --no-acl \
        --no-owner \
        --host "$PGHOST_CLOUD" \
        --port "5432" \
        --username "$PGUSER_CLOUD" \
        --dbname "$PGDB_CLOUD" \
        -F d -f "$PGDB_CLOUD-cloud-$now" --verbose
cd ../.. &&

    # drop aws
    printf "${bold}\n\nDropping AWS database data...\n${normal}" &&
    psql \
        --host "$PGHOST_CLOUD" \
        --port "5432" \
        --username "$PGUSER_CLOUD" \
        --dbname "$PGDB_CLOUD" \
        -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"

# restore aws from local dump
printf "${bold}\n\nRestoring AWS database from local database...\n${normal}" &&
    cd backups/backup-local &&
    pg_restore \
        --host "$PGHOST_CLOUD" \
        --port "5432" \
        --username "$PGUSER_CLOUD" \
        --dbname "$PGDB_CLOUD" \
        --format=d \
        --verbose \
        "$PGDB_LOCAL-local-$now"

# announce done
printf "${bold}\n\nOperation completed successfully.\n\n${normal}"
cd "$ORIGDIR" || exit 1
