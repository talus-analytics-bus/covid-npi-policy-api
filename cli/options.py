"""Reused Click options"""
import os
import click

# import logging
# import functools


DBMIGRATION_LOCAL_OPS = [
    click.option(
        "--username-local",
        "-u",
        default=os.getenv("PG_LOCAL_USERNAME"),
        help="Local PostgreSQL database server username",
    ),
    click.option(
        "--dbname-local",
        "-d",
        help="Local PostgreSQL database name (to copy from)",
    ),
]


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
