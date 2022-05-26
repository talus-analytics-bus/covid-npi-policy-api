# COVID MAP data update brief checklist
1. Install virtual environment as described in the main [`README.md`](./README.md) of this repository

1. If not done once already, clone COVID AMP main database by doing
    ```
    pipenv run python -m amp database clone-from-cloud -u [YOUR_POSTGRESQL_USERNAME] -d covid-amp-local -dc covid-npi-policy
    ```
    replacing `[YOUR_POSTGRESQL_USERNAME]` with your local PostgreSQL server username.

1. If not done once already, clone COVID AMP metrics database by doing
    ```
    pipenv run python -m amp database clone-from-cloud -u [YOUR_POSTGRESQL_USERNAME] -d metric-amp-local -dc metric-amp
    ```
    replacing `[YOUR_POSTGRESQL_USERNAME]` with your local PostgreSQL server username.

1. To update caseload data from WHO and JHU, do
    ```
    pipenv run python -m amp ingest caseload -u [YOUR_POSTGRESQL_USERNAME] -d metric-amp-local -dc metric-amp
    ```

1. To update main data from COVID AMP Airtable base, do 
    ```
    pipenv run python -m amp ingest airtable -d covid-amp-local -dc covid-npi-policy-test -e amp-dev2
    ```

1. Review [https://test.covidamp.org](https://test.covidamp.org) to ensure the site looks correct. If you need the username and password, contact a member of the development team.

1. If there are no issues, update the production site's database by doing
    ```
    pipenv run python -m amp ingest airtable -d covid-amp-local -dc covid-npi-policy -e amp-prod2
    ```
