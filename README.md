# COVID AMP API server (https://api.covidamp.org/docs)
Application programming interface (API) server for the COVID Policy Tracker website.

A list of all relevant web resources for this project follows.
- https://covidamp.org. Distribution URL for main COVID AMP site.
- https://test.covidamp.org
- https://api.covidamp.org/docs
- https://api-test.covidamp.org/docs
- https://airtable.com/tblLpBz6sEExhYVVC
- https://github.com/CSSEGISandData/COVID-19
- https://github.com/nytimes/covid-19-data


# Code organization
A description of the most important modules and packages in `covid-npi-policy-api` follows.
## Key modules and packages
- [`main.py`](./main.py). The main entrypoint module of the application (see checklist below for how to start it).
- [`db`](./db) and [`db_metric`](./db_metric). Packages that handle getting a connection to the main COVID AMP database (containing policy data) and the COVID AMP metrics database (containing COVID-19 caseload/death data). Each contains a `models.py` module that defines the entities and data fields in the databases.
- [`api`](./api). Package containing main API functionality, including defining the routing, API documentation, and functions that retrieve data from the database and return it as API responses.

## Common tasks for extending and maintaining code
See [Common tasks for extending and maintaining code](<./Common tasks for extending and maintaining code.md>)

# Checklist to start API server
A list of what is needed and a checklist of steps to start the COVID AMP API server follows.
## What you need for this checklist
* An Airtable API key (if you plan to perform data updates) and access to the Airtable base [Covid Policy Tracker](https://airtable.com/tblLpBz6sEExhYVVC) (see Hailey). If you already have access to this base, your API key can be found on your [Airtable account page](https://airtable.com/account).
## Checklist
1. Install `pipenv` if you have not already (see https://pipenv.pypa.io/en/latest/install/)
1. Install Python v3.7.13 if you have not already; `pyenv` is a tool for managing multiple Python versions on a single host (see https://github.com/pyenv/pyenv)
1. Install dev packages by doing
    ```
    pipenv install --dev --python=3.7.13
    ```
1. If you'd like to connect to a local version of the AMP database, clone the production databases with the following commands, replacing `[YOUR_POSTGRESQL_USERNAME]` with your local PostgreSQL server username. Otherwise, continue (not recommended).
    ```
    pipenv run python -m amp database clone-from-cloud -u [YOUR_LOCAL_POSTGRESQL_USERNAME] -d covid-npi-policy-local -dc covid-npi-policy
    ```
    ```
    pipenv run python -m amp database clone-from-cloud -u [YOUR_LOCAL_POSTGRESQL_USERNAME] -d metric-amp-local -dc metric-amp
    ```
1. Set up environment configuration as described in the next section
1. Start API server with terminal command `pipenv run uvicorn main:app --reload --port 8002`. You can then view interactive documentation for the API server at `http://localhost:8002/docs`.The frontend will make requests to `http://localhost:8002`, and the API server will restart if you save any Python files it uses.

## Environment configuration
You must at a minimum define environment variable `database` with the database name for the API server to connect to. Use the name `covid-npi-policy-test` for the cloud development database or `covid-npi-policy` for the cloud production database. However, it is recommended you clone one of these to work with locally to avoid making unintended changes to the cloud databases.

By default the API server will use your AWS credentials to access the production database and serve those data. If you'd like to change which host/database/etc. the server connects to, set the following environment variables as needed. If you're using `pipenv` you can put them in a file `.env` in the repository root (which git won't track).

    host=localhost
    port=5432
    database=covid-npi-policy-local
    database_metric=metric-amp-local
    username=[YOUR_LOCAL_POSTGRESQL_USERNAME]
    password=[YOUR_LOCAL_POSTGRESQL_PASSWORD]
    
Note that you must also set environment variable `AIRTABLE_API_KEY` if you're doing data ingest.
# Checklist to perform data updates
See [COVID AMP data update brief checklist](<./COVID AMP data update brief checklist.md>)