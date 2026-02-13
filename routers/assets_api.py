from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

import crud
from dependencies import get_db
from filter_helpers import (
    blank_to_none,
    normalize_limit,
    normalize_offset,
    normalize_order,
    normalize_sort,
    normalize_status,
    resolve_category_id,
    resolve_location_id,
)
from models import Asset, AssetIn, AssetUpdate, AssetsMeta

router = APIRouter()


@router.get("/assets", response_model=list[Asset])
def list_assets_api(
    q: Optional[str] = None,
    category: Optional[str] = None,
    status: Optional[str] = None,
    location: Optional[str] = None,
    sort: str = "asset_tag",
    order: str = "asc",
    limit: int = 50,
    offset: int = 0,
    db: Session = Depends(get_db),
):
    sort = normalize_sort(sort)
    order = normalize_order(order)
    status = normalize_status(status)
    category = blank_to_none(category)
    location = blank_to_none(location)
    limit = normalize_limit(limit)
    offset = normalize_offset(offset)

    category_id = resolve_category_id(db, category)
    location_id = resolve_location_id(db, location)

    return crud.list_assets_filtered(
        db,
        q=q,
        status=status,
        category_id=category_id,
        location_id=location_id,
        sort=sort,
        order=order,
        limit=limit,
        offset=offset,
    )


@router.get("/assets/meta", response_model=AssetsMeta)
def assets_meta_api(
    q: Optional[str] = None,
    category: Optional[str] = None,
    status: Optional[str] = None,
    location: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    db: Session = Depends(get_db),
):
    status = normalize_status(status)
    category = blank_to_none(category)
    location = blank_to_none(location)
    limit = normalize_limit(limit)
    offset = normalize_offset(offset)

    category_id = resolve_category_id(db, category)
    location_id = resolve_location_id(db, location)

    meta = crud.assets_meta(
        db,
        q=q,
        status=status,
        category_id=category_id,
        location_id=location_id,
        limit=limit,
        offset=offset,
    )
    return AssetsMeta(**meta)


@router.post("/assets", response_model=Asset, status_code=201)
def create_asset_api(
    body: AssetIn,
    db: Session = Depends(get_db),
):
    if crud.asset_tag_exists(db, body.asset_tag):
        raise HTTPException(status_code=409, detail="asset_tag already exists")
    return crud.create_asset(db, body)


@router.get("/assets/{asset_id}", response_model=Asset)
def get_asset_api(
    asset_id: str,
    db: Session = Depends(get_db),
):
    asset = crud.get_asset(db, asset_id)
    if not asset:
        raise HTTPException(status_code=404, detail="asset not found")
    return asset


@router.patch("/assets/{asset_id}", response_model=Asset)
def update_asset_api(
    asset_id: str,
    body: AssetUpdate,
    db: Session = Depends(get_db),
):
    asset = crud.get_asset(db, asset_id)
    if not asset:
        raise HTTPException(status_code=404, detail="asset not found")

    if body.asset_tag and crud.asset_tag_exists(db, body.asset_tag, exclude_asset_id=asset_id):
        raise HTTPException(status_code=409, detail="asset_tag already exists")

    updated = crud.update_asset(db, asset_id, body)
    if not updated:
        raise HTTPException(status_code=404, detail="asset not found")
    return updated


@router.delete("/assets/{asset_id}", status_code=204)
def delete_asset_api(
    asset_id: str,
    db: Session = Depends(get_db),
):
    ok = crud.delete_asset(db, asset_id)
    if not ok:
        raise HTTPException(status_code=404, detail="asset not found")
    return None
