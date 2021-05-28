from db_metric.models import Place
from pony.orm.core import Database, db_session, select


@db_session
def get_place_from_name(db: Database, name: str) -> Place:
    return select(
        i for i in db.Place if i.name == name or name in i.other_names
    ).first()
