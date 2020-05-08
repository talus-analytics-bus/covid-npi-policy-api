# sync all files in the specified folder with AWS S3 bucket
aws s3 sync $1 s3://covid-npi-policy-storage
