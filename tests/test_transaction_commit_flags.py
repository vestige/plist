from sqlalchemy import select

import crud
from models import AssetIn
from orm import CategoryORM, LocationORM


def test_create_asset_commit_false_requires_manual_commit(db_session):
    body = AssetIn(name="Tablet", asset_tag="T-001", category="Device", location="Shelf A")
    created = crud.create_asset(db_session, body, commit=False)

    db_session.commit()
    db_session.expire_all()

    loaded = crud.get_asset(db_session, created.id)
    assert loaded is not None
    assert loaded.asset_tag == "T-001"


def test_create_asset_commit_false_rollback_discards_change(db_session):
    body = AssetIn(name="Tablet", asset_tag="T-002", category="Device", location="Shelf A")
    created = crud.create_asset(db_session, body, commit=False)

    db_session.rollback()
    db_session.expire_all()

    loaded = crud.get_asset(db_session, created.id)
    assert loaded is None


def test_rename_category_commit_false_rollback_discards_change(db_session):
    created = crud.create_category(db_session, name="Cable")
    assert created is True

    category_id = db_session.execute(
        select(CategoryORM.id).where(CategoryORM.name == "Cable")
    ).scalar_one()

    renamed = crud.rename_category(
        db_session,
        category_id=category_id,
        new_name="Adapter",
        commit=False,
    )
    assert renamed is True

    db_session.rollback()
    db_session.expire_all()

    category = db_session.get(CategoryORM, category_id)
    assert category is not None
    assert category.name == "Cable"


def test_delete_category_commit_false_rollback_discards_delete(db_session):
    created = crud.create_category(db_session, name="Tripod")
    assert created is True

    category_id = db_session.execute(
        select(CategoryORM.id).where(CategoryORM.name == "Tripod")
    ).scalar_one()

    deleted = crud.delete_category(db_session, category_id=category_id, commit=False)
    assert deleted is True

    db_session.rollback()
    db_session.expire_all()

    category = db_session.get(CategoryORM, category_id)
    assert category is not None
    assert category.name == "Tripod"


def test_delete_location_commit_false_requires_manual_commit(db_session):
    created = crud.create_location(db_session, name="Rack A")
    assert created is True

    location_id = db_session.execute(
        select(LocationORM.id).where(LocationORM.name == "Rack A")
    ).scalar_one()

    deleted = crud.delete_location(db_session, location_id=location_id, commit=False)
    assert deleted is True

    db_session.commit()
    db_session.expire_all()

    location = db_session.get(LocationORM, location_id)
    assert location is None


def test_loan_asset_commit_false_requires_manual_commit(db_session):
    asset = crud.create_asset(db_session, AssetIn(name="Camera", asset_tag="C-001"))

    loaned = crud.loan_asset(
        db_session,
        asset.id,
        borrower="Alice",
        due_at=None,
        note=None,
        commit=False,
    )
    assert loaned is True

    db_session.commit()
    db_session.expire_all()

    loaded = crud.get_asset(db_session, asset.id)
    assert loaded is not None
    assert loaded.status == "loaned"

    active = crud.get_active_loan(db_session, asset.id)
    assert active is not None
    assert active.borrower == "Alice"


def test_return_asset_commit_false_rollback_discards_return(db_session):
    asset = crud.create_asset(db_session, AssetIn(name="Camera", asset_tag="C-002"))
    loaned = crud.loan_asset(
        db_session,
        asset.id,
        borrower="Bob",
        due_at=None,
        note=None,
        commit=True,
    )
    assert loaned is True

    returned = crud.return_asset(db_session, asset.id, commit=False)
    assert returned is True

    db_session.rollback()
    db_session.expire_all()

    loaded = crud.get_asset(db_session, asset.id)
    assert loaded is not None
    assert loaded.status == "loaned"

    active = crud.get_active_loan(db_session, asset.id)
    assert active is not None
    assert active.borrower == "Bob"
    assert active.returned_at is None
