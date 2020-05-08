# copy all files in the specified folder with AWS S3 bucket
# with recursion
aws s3 cp $1 s3://covid-npi-policy-storage --recursive
