import subprocess
from re import S

import click
from pony.orm import db_session


@click.group(help="Advanced commands not normally used except for debugging")
def advanced():
    pass


@advanced.command(
    help="Add S3 files from policy Airtable PDF URLs if they aren't already in S3"
)
def add_s3_files():
    do_add_s3_files()


@db_session
def do_add_s3_files():
    from db import db, models
    from ingest import awss3
    from ingest.util import bcolors
    from pony.orm import commit
    from tqdm import tqdm

    db.generate_mapping(create_tables=False)
    s3_bucket_keys = awss3.get_s3_bucket_keys()
    files = models.File.select()
    print(f"""Validating {len(files)} files...""")
    # confirm file exists in S3 bucket for file, if not, either add it
    # or remove the PDF text

    # track what was done
    n_valid = 0
    n_missing = 0
    n_added = 0
    n_failed = 0
    n_checked = 0
    could_not_download = set()
    missing_filenames = set()
    for file in tqdm(files, desc="Checking S3 files"):
        n_checked += 1
        file_filename = file.filename
        if file_filename is not None:
            status: str = awss3.add_file_to_s3_if_missing(file, s3_bucket_keys)
            if status == "missing":
                n_missing += 1
                missing_filenames.add(file_filename)
            elif status == "failed":
                n_failed += 1
                could_not_download.add(file_filename)
            elif status == "valid":
                n_valid += 1
            elif status == "added":
                n_added += 1
            else:
                raise ValueError("Unexpected status: " + str(status))

            if status in ("valid", "added"):
                s3_permalink = file.get_s3_permalink()
                if s3_permalink != "":
                    file.permalink = s3_permalink
        else:
            print("Skipping, no file associated")
    commit()

    print("\n\nS3 file validation results:")
    print("Valid: " + str(n_valid))
    print("Added to S3: " + str(n_added))
    print("Missing (no URL or filename): " + str(n_missing))
    print("Failed to fetch from URL: " + str(n_failed))
    if n_missing > 0:
        missing_filenames = list(missing_filenames)
        missing_filenames.sort()
        print(
            f"""\n{bcolors.BOLD}[Warning] URLs or filenames were not """
            f"""provided for {n_missing} files with the following """
            f"""names:{bcolors.ENDC}"""
        )
        print(bcolors.BOLD + str(", ".join(missing_filenames)) + bcolors.ENDC)

    if n_failed > 0:
        could_not_download = list(could_not_download)
        could_not_download.sort()
        print(
            f"""\n{bcolors.BOLD}[Warning] Files could not be """
            f"""downloaded from the following {n_failed} """
            f"""sources:{bcolors.ENDC}"""
        )
        print(bcolors.BOLD + str(", ".join(could_not_download)) + bcolors.ENDC)
