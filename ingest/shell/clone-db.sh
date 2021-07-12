#!/bin/bash
##
# Clone a AWS RDS database locally.
# Arguments: [username] [aws_db_name] [local_db_name] [aws_db_host]
##

programname=$0

function usage {
    echo "usage: $programname [username]"
    echo "  username    your local pgsql server username"
    echo "  dbprod      the name of the database on the AWS RDS server from which you are updating"
    echo "  dblocal     the name of the database on your local server to which you are updating"
    echo "  dbprodhost  the host name of the AWS RDS server from which you are updating"
    exit 1
}

username=${1?Provide your local pgsql server username in first argument};
dbprod=${2?Provide the name of the database on AWS RDS from which you are updating in second argument};
dblocal=${3?Provide the name of the database on your local server to which you are updating in third argument};
dbprodhost=${4?Provide the host name of the AWS RDS server from which you are updating in fourth argument};

bold=$(tput bold)
normal=$(tput sgr0)

printf "\n\n${bold}Cloning AWS RDS database locally...${normal}";
now=$(date) && \
printf "\nCurrent date: $now\n" &&

# create local
printf "\n\n${bold}Creating local database, if does not exist...\n${normal}";
createdb $dblocal;

# backup local
cd ingest/backups/local && \
pg_dump \
--no-acl \
--no-owner \
--host "localhost" \
--port "5432" \
--username "$username" \
--dbname "$dblocal" \
-F d -f "$dblocal-local-$now" --verbose && \
cd ../../.. && \

# backup aws
printf "${bold}\n\nDumping AWS database...\n${normal}" && \
cd ingest/backups/prod && \
pg_dump \
--no-acl \
--no-owner \
--host $dbprodhost \
--port "5432" \
--username "talus" \
--dbname "$dbprod" \
-F d -f "$dbprod-aws-$now" --verbose;
cd ../../.. && \

# drop local
printf "${bold}\n\nDropping local database data...\n${normal}" && \
psql \
--host "localhost" \
--port "5432" \
--username "$username" \
--dbname "$dblocal" \
-c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;" \

# restore local from AWS RDS dump
printf "${bold}\n\nRestoring AWS database to local database...\n${normal}" && \
cd ingest/backups/prod && \
pg_restore --host "localhost" \
--port "5432" \
--username "$username" \
--dbname "$dblocal" \
--format=d \
--verbose \
"$dbprod-aws-$now";

# announce done
printf "${bold}\n\nOperation completed successfully.\n\n${normal}";