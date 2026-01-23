from __future__ import annotations

from datetime import datetime, timezone

from typing import Optional
from uuid import uuid4

from sqlalchemy import select, delete, func, or_, update
from sqlalchemy.orm import Session

from models import Asset, AssetIn, AssetUpdate, Loan
from orm import AssetORM, LoanORM, CategoryORM, LocationORM

ALLOWED_SORTS = {
    "asset_tag": AssetORM.asset_tag,
    "name": AssetORM.name,
    "updated_at": AssetORM.updated_at,
}

def _asset_to_schema(a: AssetORM) -> Asset:
    return Asset(
        id=a.id,
        name=a.name,
        asset_tag=a.asset_tag,
        category=a.category,
        location=a.location,
        note=a.note,
        status=a.status,  # type: ignore
        created_at=a.created_at,
        updated_at=a.updated_at,
    )

def _loan_to_schema(l: LoanORM) -> Loan:
    return Loan(
        id=l.id,
        asset_id=l.asset_id,
        borrower=l.borrower,
        loaned_at=l.loaned_at,
        due_at=l.due_at,
        returned_at=l.returned_at,
        note=l.note,
    )


# ---------- Asset ----------
def asset_tag_exists(db: Session, asset_tag: str, exclude_asset_id: Optional[str] = None) -> bool:
    stmt = select(AssetORM).where(AssetORM.asset_tag == asset_tag)
    if exclude_asset_id:
        stmt = stmt.where(AssetORM.id != exclude_asset_id)
    return db.execute(stmt).first() is not None


def get_asset(db: Session, asset_id: str) -> Optional[Asset]:
    row = db.get(AssetORM, asset_id)
    return _asset_to_schema(row) if row else None


def create_asset(db: Session, body: AssetIn) -> Asset:
    now = datetime.now(timezone.utc)
    a = AssetORM(
        id=str(uuid4()),
        name=body.name,
        asset_tag=body.asset_tag,
        category=body.category,
        location=body.location,
        note=body.note,
        status="available",
        created_at=now,
        updated_at=now,
    )
    db.add(a)
    db.commit()
    db.refresh(a)
    return _asset_to_schema(a)


def update_asset(db: Session, asset_id: str, body: AssetUpdate) -> Optional[Asset]:
    a = db.get(AssetORM, asset_id)
    if not a:
        return None

    data = body.model_dump(exclude_unset=True)
    for k, v in data.items():
        setattr(a, k, v)
    a.updated_at = datetime.now(timezone.utc)

    db.commit()
    db.refresh(a)
    return _asset_to_schema(a)


def delete_asset(db: Session, asset_id: str) -> bool:
    result = db.execute(delete(AssetORM).where(AssetORM.id == asset_id))
    db.commit()
    return result.rowcount > 0


def build_assets_query(q: str | None, status: str | None, category: str | None, location: str | None):
    stmt = select(AssetORM)

    if q:
        like = f"%{q}%"
        stmt = stmt.where(
            or_(
                AssetORM.name.ilike(like),
                AssetORM.asset_tag.ilike(like),
                AssetORM.note.ilike(like),
            )
        )
    if status:
        stmt = stmt.where(AssetORM.status == status)
    if category:
        stmt = stmt.where(AssetORM.category == category)
    if location:
        stmt = stmt.where(AssetORM.location == location)

    return stmt

def assets_meta(
    db: Session,
    *,
    q: str | None,
    status: str | None,
    category: str | None,
    location: str | None,
    limit: int,
    offset: int,
) -> dict:
    if limit < 1:
        limit = 1
    if limit > 500:
        limit = 500
    if offset < 0:
        offset = 0

    total = count_assets_filtered(db, q=q, status=status, category=category, location=location)
    total_pages = max(1, (total + limit - 1) // limit)

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "total_pages": total_pages,
    }

def count_assets_filtered(db: Session, *, q: str | None, status: str | None, category: str | None, location: str | None) -> int:
    stmt = build_assets_query(q, status, category, location)
    count_stmt = select(func.count()).select_from(stmt.subquery())
    return int(db.execute(count_stmt).scalar_one())

def list_assets_filtered(
    db: Session,
    *,
    q: str | None,
    status: str | None,
    category: str | None,
    location: str | None,
    sort: str,
    order: str,
    limit: int,
    offset: int,
) -> list[Asset]:
    stmt = build_assets_query(q, status, category, location)

    col = ALLOWED_SORTS.get(sort, AssetORM.asset_tag)
    desc = (order or "").lower() == "desc"
    stmt = stmt.order_by(col.desc() if desc else col.asc())

    stmt = stmt.limit(limit).offset(offset)
    rows = db.execute(stmt).scalars().all()
    return [_asset_to_schema(a) for a in rows]

def list_distinct_values(db: Session, column_name: str) -> list[str]:
    # category/location 用の候補リスト
    col = getattr(AssetORM, column_name)
    stmt = select(col).where(col.is_not(None)).distinct().order_by(col.asc())
    return [r[0] for r in db.execute(stmt).all() if r[0]]

# ---------- Loan ----------
def get_active_loan(db: Session, asset_id: str) -> Optional[Loan]:
    stmt = (
        select(LoanORM)
        .where(LoanORM.asset_id == asset_id, LoanORM.returned_at.is_(None))
        .order_by(LoanORM.loaned_at.desc())
        .limit(1)
    )
    row = db.execute(stmt).scalars().first()
    return _loan_to_schema(row) if row else None


def loan_asset(db: Session, asset_id: str, borrower: str, due_at: Optional[datetime], note: Optional[str]) -> bool:
    a = db.get(AssetORM, asset_id)
    if not a or a.status != "available":
        return False

    now = datetime.now(timezone.utc)
    loan = LoanORM(
        id=str(uuid4()),
        asset_id=asset_id,
        borrower=borrower,
        loaned_at=now,
        due_at=due_at,
        returned_at=None,
        note=note,
    )
    db.add(loan)

    a.status = "loaned"
    a.updated_at = now

    db.commit()
    return True


def return_asset(db: Session, asset_id: str) -> bool:
    a = db.get(AssetORM, asset_id)
    if not a:
        return False

    now = datetime.now(timezone.utc)

    # アクティブな貸出を返却済みにする（あれば）
    stmt = (
        select(LoanORM)
        .where(LoanORM.asset_id == asset_id, LoanORM.returned_at.is_(None))
        .order_by(LoanORM.loaned_at.desc())
        .limit(1)
    )
    loan = db.execute(stmt).scalars().first()
    if loan:
        loan.returned_at = now

    a.status = "available"
    a.updated_at = now

    db.commit()
    return True

def bulk_import_assets(db: Session, rows: list[dict[str, str]]) -> dict:
    """
    rows: [{"name": "...", "asset_tag": "...", "category": "...", "location": "...", "note": "..."}]
    """
    created = 0
    skipped = 0
    errors: list[str] = []

    for idx, r in enumerate(rows, start=1):
        name = (r.get("name") or "").strip()
        asset_tag = (r.get("asset_tag") or "").strip()
        category = (r.get("category") or "").strip() or None
        location = (r.get("location") or "").strip() or None
        note = (r.get("note") or "").strip() or None

        if not name or not asset_tag:
            errors.append(f"row {idx}: name/asset_tag is empty")
            continue

        if asset_tag_exists(db, asset_tag):
            skipped += 1
            continue

        body = AssetIn(
            name=name,
            asset_tag=asset_tag,
            category=category,
            location=location,
            note=note,
        )
        create_asset(db, body)
        created += 1

    return {"created": created, "skipped": skipped, "errors": errors}


def list_categories(db: Session) -> list[tuple[str, str]]:
    """(id, name) のリスト。sort_order, name の順で返す"""
    rows = db.execute(
        select(CategoryORM.id, CategoryORM.name)
        .order_by(CategoryORM.sort_order.asc(), CategoryORM.name.asc())
    ).all()
    return [(r[0], r[1]) for r in rows]

def list_locations(db: Session) -> list[tuple[str, str]]:
    rows = db.execute(
        select(LocationORM.id, LocationORM.name)
        .order_by(LocationORM.sort_order.asc(), LocationORM.name.asc())
    ).all()
    return [(r[0], r[1]) for r in rows]


def create_category(db: Session, *, name: str, sort_order: int = 0) -> bool:
    name = (name or "").strip()
    if not name:
        return False
    exists = db.execute(select(CategoryORM).where(CategoryORM.name == name)).first()
    if exists:
        return False

    now = datetime.now(datetime.UTC)
    c = CategoryORM(id=str(uuid4()), name=name, sort_order=sort_order, created_at=now, updated_at=now)
    db.add(c)
    db.commit()
    return True

def create_location(db: Session, *, name: str, sort_order: int = 0) -> bool:
    name = (name or "").strip()
    if not name:
        return False
    exists = db.execute(select(LocationORM).where(LocationORM.name == name)).first()
    if exists:
        return False

    now = datetime.now(datetime.UTC)
    l = LocationORM(id=str(uuid4()), name=name, sort_order=sort_order, created_at=now, updated_at=now)
    db.add(l)
    db.commit()
    return True


def rename_category(db: Session, *, category_id: str, new_name: str, cascade_assets: bool = True) -> bool:
    new_name = (new_name or "").strip()
    if not new_name:
        return False

    c = db.get(CategoryORM, category_id)
    if not c:
        return False

    # 同名チェック（自分以外）
    dup = db.execute(
        select(CategoryORM).where(CategoryORM.name == new_name, CategoryORM.id != category_id)
    ).first()
    if dup:
        return False

    old_name = c.name
    now = datetime.now(datetime.UTC)
    c.name = new_name
    c.updated_at = now

    if cascade_assets and old_name != new_name:
        db.execute(
            update(AssetORM)
            .where(AssetORM.category == old_name)
            .values(category=new_name, updated_at=now)
        )

    db.commit()
    return True


def rename_location(db: Session, *, location_id: str, new_name: str, cascade_assets: bool = True) -> bool:
    new_name = (new_name or "").strip()
    if not new_name:
        return False

    l = db.get(LocationORM, location_id)
    if not l:
        return False

    dup = db.execute(
        select(LocationORM).where(LocationORM.name == new_name, LocationORM.id != location_id)
    ).first()
    if dup:
        return False

    old_name = l.name
    now = datetime.now(datetime.UTC)
    l.name = new_name
    l.updated_at = now

    if cascade_assets and old_name != new_name:
        db.execute(
            update(AssetORM)
            .where(AssetORM.location == old_name)
            .values(location=new_name, updated_at=now)
        )

    db.commit()
    return True


def delete_category(db: Session, *, category_id: str) -> bool:
    c = db.get(CategoryORM, category_id)
    if not c:
        return False

    # 使用中なら削除不可（assetsは文字列なので name で見る）
    used = db.execute(
        select(func.count()).select_from(AssetORM).where(AssetORM.category == c.name)
    ).scalar_one()
    if int(used) > 0:
        return False

    db.execute(delete(CategoryORM).where(CategoryORM.id == category_id))
    db.commit()
    return True


def delete_location(db: Session, *, location_id: str) -> bool:
    l = db.get(LocationORM, location_id)
    if not l:
        return False

    used = db.execute(
        select(func.count()).select_from(AssetORM).where(AssetORM.location == l.name)
    ).scalar_one()
    if int(used) > 0:
        return False

    db.execute(delete(LocationORM).where(LocationORM.id == location_id))
    db.commit()
    return True
