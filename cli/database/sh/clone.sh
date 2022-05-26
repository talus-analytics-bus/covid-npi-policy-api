#!/bin/sh
# Dump cloud database and restore to local
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
PGDB_LOCAL_WORKMEM=${6:-"4GB"}

bold=$(tput bold)
normal=$(tput sgr0)

now=$(date) &&
    echo "Current date: $now" &&
    # backup local
    mkdir backups

printf "${bold}\n\nDumping cloud database...\n${normal}" &&
    cd backups && mkdir backup-cloud
cd backup-cloud &&
    pg_dump \
        --no-acl \
        --no-owner \
        --host "$PGHOST_CLOUD" \
        --port "5432" \
        --username "$PGUSER_CLOUD" \
        --dbname "$PGDB_CLOUD" \
        -F d -f "$PGDB_CLOUD-cloud-$now" --verbose &&

    # restore local from cloud backup
    printf "${bold}\n\nRestoring local database from cloud database...\n${normal}" &&
    createdb "$PGDB_LOCAL" && echo Created local database || echo Database already exists, continuing
pg_restore \
    --host localhost \
    --port "5432" \
    --username "$PGUSER_LOCAL" \
    --dbname "$PGDB_LOCAL" \
    --format=d \
    --verbose \
    "$PGDB_CLOUD-cloud-$now" &&
    psql -U "$PGUSER_LOCAL" "$PGDB_LOCAL" -c "set work_mem = '$PGDB_LOCAL_WORKMEM';"
cd "$ORIGDIR" || exit 1
