from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from orm import CategoryORM, LocationORM

VALID_STATUSES = {"available", "loaned", "retired"}
VALID_SORTS = {"asset_tag", "name", "updated_at"}
VALID_ORDERS = {"asc", "desc"}


def blank_to_none(value: Optional[str]) -> Optional[str]:
    if value == "":
        return None
    return value


def normalize_status(status: Optional[str]) -> Optional[str]:
    if status in VALID_STATUSES:
        return status
    return None


def normalize_sort(sort: str) -> str:
    if sort in VALID_SORTS:
        return sort
    return "asset_tag"


def normalize_order(order: str) -> str:
    if order in VALID_ORDERS:
        return order
    return "asc"


def normalize_limit(limit: int, *, min_value: int = 1, max_value: int = 500) -> int:
    if limit < min_value:
        return min_value
    if limit > max_value:
        return max_value
    return limit


def normalize_offset(offset: int) -> int:
    if offset < 0:
        return 0
    return offset


def resolve_category_id(db: Session, name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    row = db.execute(select(CategoryORM.id).where(CategoryORM.name == name)).first()
    return row[0] if row else None


def resolve_location_id(db: Session, name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    row = db.execute(select(LocationORM.id).where(LocationORM.name == name)).first()
    return row[0] if row else None
