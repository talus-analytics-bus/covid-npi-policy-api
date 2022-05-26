# `api` package
Package containing main API functionality, including defining the routing, API documentation, and functions that retrieve data from the database and return it as API responses.
## Key modules and packages
- [`models.py`](./models.py). Module defining Pydantic types (classes) for marshalling API responses, i.e., getting data into the right format and raising exceptions if its format is unexpected.

## Troubleshooting
**I get a CORS error when consuming data from the API.**
Ensure the domain you're making the API request from is on the list of allowed domains in module [app.py](./app.py) in variable `allow_origin_regex`.