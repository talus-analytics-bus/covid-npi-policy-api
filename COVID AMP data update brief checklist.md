# COVID AMP data update brief checklist
Note: It is safe to perform data updates on different hosts because this checklist will completely and comprehensively replace the data in the cloud databases each time.

1. Install virtual environment as described in the main [`README.md`](./README.md) of this repository

1. Start a shell in `pipenv` with your virtual environment with command
    ```
    pipenv shell
    ```

1. If not done once already, clone COVID AMP main database locally by doing
    ```
    python -m amp database clone-from-cloud -u [YOUR_POSTGRESQL_USERNAME] -d covid-npi-policy-local -dc covid-npi-policy
    ```
    replacing `[YOUR_POSTGRESQL_USERNAME]` with your local PostgreSQL server username.

1. If not done once already, clone COVID AMP metrics database locally by doing
    ```
    python -m amp database clone-from-cloud -u [YOUR_POSTGRESQL_USERNAME] -d metric-amp-local -dc metric-amp
    ```

1. To update caseload data from NYT and JHU, do
    ```
    python -m amp ingest caseload -u [YOUR_POSTGRESQL_USERNAME] -d metric-amp-local -dc metric-amp
    ```

1. Concurrent with or after the previous step, update main data from COVID AMP Airtable base, do 
    ```
    python -m amp ingest airtable -u [YOUR_POSTGRESQL_USERNAME] -d covid-npi-policy-local -dc covid-npi-policy-test -e amp-dev2
    ```

1. Review [https://test.covidamp.org](https://test.covidamp.org) to ensure the site looks correct. If you need the username and password, contact a member of the development team.

1. Ensure you are in the `dev` branch of this repository by doing command
    ```
    git checkout dev
    ```

1. Re-generate the static Excel download files (full and summary versions) by doing command
    ```
    python -m amp export -s && python -m amp export -sb
    ```

1. Commit and push the updated Excel files with command
    ```
    git add api/export/static/staticfull.xlsx api/export/static/staticsummary.xlsx &&
        git commit -m "Update static unfiltered Excel files" &&
        git push;
    ```
    **Note: This will push any unpushed commits you may have made on branch `dev`.**
    The `push` will trigger a CircleCI job to update the API server deployment which you can monitor here: https://app.circleci.com/pipelines/github/talus-analytics-bus/covid-npi-policy-api. When the job turns green, continue.
1. If there are no issues, update the production site's database by doing
    ```
    python -m amp database restore-to-cloud -u [YOUR_POSTGRESQL_USERNAME] -d covid-npi-policy-local -dc covid-npi-policy -e amp-prod2
    ```

1. Merge the branch `dev` into branch `master` and push the latest to GitHub by doing
    ```
    git checkout master && git merge dev && git push;
    ```
    The `push` will trigger a CircleCI job to update the API server deployment which you can monitor here: https://app.circleci.com/pipelines/github/talus-analytics-bus/covid-npi-policy-api.
