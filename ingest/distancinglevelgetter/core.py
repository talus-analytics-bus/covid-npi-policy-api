import boto3
import botocore
import pandas as pd
import sys
from datetime import datetime
from dateutil.tz import tzutc
from typing import Any, List

from alive_progress.core.progress import alive_bar
from pony.orm.core import Database, db_session, select, delete, commit

from ingest.plugins import get_name_from_iso3
from ingest.util import upsert


class DistancingLevelGetter(object):
    def __init__(self, S3_BUCKET_NAME: str, path: str, fn_prefix: str) -> None:
        """Creates a dataframe of the most recent distancing levels CSV in S3.

        Args:
            S3_BUCKET_NAME (str): The S3 Bucket where the distancing levels CSV
            is stored.

            path (str): The path to the director that holds the CSV

            fn_prefix (str): The prefix used for the CSV filenames
        """
        self.s3_client: Any = boto3.client("s3")
        self.distancing_levels_df: pd.DataFrame = pd.DataFrame([])
        self.newest_csv_obj_key: str = None
        s3_obj_meta: dict = None
        s3_obj_metas: List[dict] = self.s3_client.list_objects_v2(
            Bucket=S3_BUCKET_NAME, Prefix=(path + "/" + fn_prefix)
        ).get("Contents", list())
        newest_last_modified_dt: datetime = datetime(1900, 1, 1, tzinfo=tzutc())
        for s3_obj_meta in s3_obj_metas:
            s3_obj_key: str = s3_obj_meta.get("Key", None)
            last_modified_dt: datetime = s3_obj_meta.get("LastModified", None)
            if s3_obj_key is not None:
                if (
                    last_modified_dt is not None
                    and newest_last_modified_dt < last_modified_dt
                ):
                    newest_last_modified_dt = last_modified_dt
                    self.newest_csv_obj_key = s3_obj_key
        if self.newest_csv_obj_key is not None:
            csv: botocore.response.StreamingBody = self.s3_client.get_object(
                Bucket=S3_BUCKET_NAME, Key=self.newest_csv_obj_key
            )["Body"]
            self.distancing_levels_df: pd.DataFrame = pd.read_csv(csv)
        else:
            print("No distancing levels CSV was found.")
        super().__init__()

    @db_session
    def import_levels(self, db: Database) -> None:
        # delete existing observations
        print("Deleting existing observations...")
        delete(i for i in db.Observation if i.metric == 0)
        print("Existing observations deleted.")

        # add new observations
        skipped: int = 0
        source_id: int = 0
        with alive_bar(
            len(self.distancing_levels_df.index),
            title="Importing distancing levels",
        ) as bar:
            record: dict = None
            for record in self.distancing_levels_df.to_dict(orient="records"):
                bar()
                if "Name" not in record:
                    skipped += 1
                    continue
                if not (
                    record["Date"].startswith("2020")
                    or record["Date"].startswith("2021")
                ):
                    skipped += 1
                    continue

                place = None
                if record["Location type"] == "State":
                    place = select(
                        i
                        for i in db.Place
                        if i.iso3 == "USA"
                        and i.area1 == record["Name"]
                        and (i.area2 == "Unspecified" or i.area2 == "")
                        and i.level == "State / Province"
                    ).first()

                    if place is None:
                        # TODO generalize to all countries
                        action, place = upsert(
                            db.Place,
                            {
                                "iso3": "USA",
                                "country_name": "United States of America" " (USA)",
                                "area1": record["Name"],
                                "area2": "Unspecified",
                                "level": "State / Province",
                            },
                            {"loc": f"""{record['Name']}, USA"""},
                        )

                else:
                    # TODO
                    place = select(
                        i
                        for i in db.Place
                        if i.iso3 == record["Name"] and i.level == "Country"
                    ).first()

                    if place is None:
                        # TODO generalize to all countries
                        action, place = upsert(
                            db.Place,
                            {
                                "iso3": record["Name"],
                                "country_name": get_name_from_iso3(record["Name"])
                                + f""" ({record['Name']})""",
                                "area1": "Unspecified",
                                "area2": "Unspecified",
                                "level": "Country",
                            },
                            {
                                "loc": get_name_from_iso3(record["Name"])
                                + f""" ({record['Name']})"""
                            },
                        )

                if place is None:
                    print("[FATAL ERROR] Missing place")
                    sys.exit(0)

                action, record = upsert(
                    db.Observation,
                    {"source_id": str(source_id)},
                    {
                        "date": record["Date"],
                        "metric": 0,
                        "value": "Mixed distancing levels"
                        if record["Status"] == "Mixed"
                        else record["Status"],
                        "place": place,
                    },
                    do_commit=False,
                )
                source_id += 1
            print("\nCommitting records to database...")
            commit()
            print("Committed.")
