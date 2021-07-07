# dump local
now=$(date);
echo "Current date: $now";
cd ingest/backups/local;
pg_dump \
--host "localhost" \
--port "5432" \
--username "mikevanmaele" \
--dbname "covid-npi-policy" \
-F d -f "$now-local";
cd ../../..;

# dump prod'
cd ingest/backups/prod;
pg_dump \
--host "talus-dev.cvsrrvlopzxr.us-west-1.rds.amazonaws.com" \
--port "5432" \
--username "talus" \
--dbname "covid-npi-policy" \
-F d -f "$now-prod";
cd ../../..;

# drop prod
psql \
--host "talus-dev.cvsrrvlopzxr.us-west-1.rds.amazonaws.com" \
--port "5432" \
--username "talus" \
--dbname "covid-npi-policy" \
-c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;";

# restore prod from local dump
cd ingest/backups/local;
pg_restore \
--host "talus-dev.cvsrrvlopzxr.us-west-1.rds.amazonaws.com" \
--port "5432" \
--username "talus" \
--dbname "covid-npi-policy" \
--format=d --verbose "$now-local";
cd ../../..;

# restart API server
aws elasticbeanstalk restart-app-server --environment-name covid-npi-policy-api-prod --region us-west-2;
