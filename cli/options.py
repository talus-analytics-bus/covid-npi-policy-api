"""Reused Click options"""
import os
import click

# import logging
# import functools


username_local_op = click.option(
    "--username-local",
    "-u",
    default=os.getenv("PG_LOCAL_USERNAME"),
    help="Local PostgreSQL database server username",
)

dbname_local_op = click.option(
    "--dbname-local",
    "-d",
    help="Local PostgreSQL database name (to copy from)",
)

DBMIGRATION_LOCAL_OPS = [
    username_local_op,
    dbname_local_op,
]


AWSEB_ENV_RESTART_OPS = [
    click.option(
        "--awseb-environment-name",
        "-e",
        default=None,
        type=str,
        help="Environment name of AWS Elastic Beanstalk application server which uses the"
        " database named in `--dbname-cloud` and which should be restarted after the"
        " database is updated. If blank, no server will be restarted.",
    ),
    click.option(
        "--awseb-environment-region",
        "-r",
        default="us-west-1",
        show_default=True,
        type=str,
        help="Region of AWS Elastic Beanstalk application server which uses the"
        " database named in `--dbname-cloud` and which should be restarted after the"
        " database is updated. Only required if `--awseb-environment-name` is defined.",
    ),
]


def awseb_restart(func):
    for op in AWSEB_ENV_RESTART_OPS:
        func = op(func)
    return func


def validate_awseb_restart_ops(awseb_environment_name, awseb_environment_region):
    if awseb_environment_name is not None and awseb_environment_region is None:
        raise ValueError("Must define both env name and region if name is defined")


def dbmigration_local(func):
    for op in DBMIGRATION_LOCAL_OPS:
        func = op(func)
    return func


dbmigration_cloud = click.option(
    "--dbname-cloud",
    "-dc",
    type=str,
    help="Cloud PostgreSQL database name (to copy to)",
)


def dbmigration_all(func):

    ops = [
        dbmigration_cloud,
        *DBMIGRATION_LOCAL_OPS,
    ]
    for op in ops:
        func = op(func)
    return func


@click.option(
    "--skip-restore",
    "-s",
    is_flag=True,
    show_default=True,
    default=False,
    help="If flag is set, skips restoring the local database to the cloud"
    " database. Used for debugging purposes.",
)
def skip_restore(func):
    op = click.option(
        "--skip-restore",
        "-s",
        is_flag=True,
        show_default=True,
        default=False,
        help="If flag is set, skips restoring the local database to the cloud"
        " database. Used for debugging purposes.",
    )
    func = op(func)
    return func


def yes(func):
    """Adds option to respond "yes" automatically"""
    op = click.option(
        "--yes",
        "-y",
        default=False,
        is_flag=True,
        show_default=True,
        help="Automatically respond 'yes' to all prompts without stopping",
    )

    func = op(func)
    return func


# WIP potential future decorators below

# def quiet(func):
#     op = click.option(
#         "--quiet",
#         "-q",
#         default=False,
#         is_flag=True,
#         show_default=True,
#         help="Silences console logging and only logs to files",
#     )

#     func = op(func)
#     return func


# def handle_quiet(func):
#     @functools.wraps(func)
#     def wrapper(*args, **kwargs):
#         if kwargs.get("quiet") is True:
#             logging.basicConfig(handlers=[])
#         return func(*args, **kwargs)

#     return wrapper
