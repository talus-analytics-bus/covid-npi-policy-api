# covid-npi-policy-api
API server for the COVID Policy Tracker website.
# Steps to start API server
1. Create a virtual environment using Python version 3.7.10, following the steps in [this](https://github.com/talus-analytics-bus/talus-intranet-react/wiki/Setting-up-a-Python-virtual-environment) checklist if unsure how.
1. Activate virtual environment from terminal (use guide in step 1 if unsure how).
1. Create file `db/config-local.ini` with the following contents, providing your own database connection information:
```
[DEFAULT]
username = mikevanmaele
host = localhost
password = 
database = covid-npi-policy
```
1. Start API server with terminal command `uvicorn main:app --reload --port 8002`.