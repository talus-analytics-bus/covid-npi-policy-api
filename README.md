# covid-npi-policy-api
API server for the COVID Policy Tracker website.
# Checklist to start API server
A list of what is needed and a checklist of steps to start the COVID AMP API server follows.
## What you need for this checklist
* An Airtable API key (if you plan to perform data updates) and access to the Airtable base [Covid Policy Tracker](https://airtable.com/tblLpBz6sEExhYVVC) (see Alaina). If you already have access to this base, your API key can be found on your [Airtable account page](https://airtable.com/account).
## Checklist
1. Create a virtual environment using Python version 3.7.13, following the steps in [this](https://github.com/talus-analytics-bus/talus-intranet-react/wiki/Setting-up-a-Python-virtual-environment) checklist if unsure how.
1. Activate virtual environment from terminal (use guide in step 1 if unsure how).
1. Create file `db/config-local.ini` with the following contents, providing your own database connection information:
    ```
    [DEFAULT]
    username = mikevanmaele
    host = localhost
    password = 
    database = covid-npi-policy
    ```
1. Export your Airtable API key as an environment variable by doing this terminal command:
    ```
    export AIRTABLE_API_KEY=REDACTED
    ```
1. Start API server with terminal command `uvicorn main:app --reload --port 8002`.