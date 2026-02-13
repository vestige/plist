from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session

import crud
from csv_utils import assets_to_csv_response, csv_bytes_to_rows
from dependencies import get_db
from filter_helpers import (
    blank_to_none,
    normalize_order,
    normalize_sort,
    normalize_status,
)
from models import AssetIn, AssetUpdate

router = APIRouter()
PAGE_SIZE = 50
EXPORT_LIMIT = 20000


@router.get("/ui/assets", response_class=HTMLResponse)
def assets_ui(
    request: Request,
    q: Optional[str] = None,
    status: Optional[str] = None,
    category_id: Optional[str] = None,
    location_id: Optional[str] = None,
    sort: str = "asset_tag",
    order: str = "asc",
    page: int = 1,
    db: Session = Depends(get_db),
):
    if page < 1:
        page = 1

    status = normalize_status(status)
    category_id = blank_to_none(category_id)
    location_id = blank_to_none(location_id)
    sort = normalize_sort(sort)
    order = normalize_order(order)

    meta = crud.assets_meta(
        db,
        q=q,
        status=status,
        category_id=category_id,
        location_id=location_id,
        limit=PAGE_SIZE,
        offset=(page - 1) * PAGE_SIZE,
    )
    total = meta["total"]
    total_pages = meta["total_pages"]
    if page > total_pages:
        page = total_pages

    offset = (page - 1) * PAGE_SIZE
    assets = crud.list_assets_filtered(
        db,
        q=q,
        status=status,
        category_id=category_id,
        location_id=location_id,
        sort=sort,
        order=order,
        limit=PAGE_SIZE,
        offset=offset,
    )

    active_loans = {}
    for asset in assets:
        active = crud.get_active_loan(db, asset.id)
        if active:
            active_loans[asset.id] = active

    categories = crud.list_categories(db)
    locations = crud.list_locations(db)
    category_map = {cid: cname for cid, cname in categories}
    location_map = {lid: lname for lid, lname in locations}

    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "assets.html",
        {
            "assets": assets,
            "active_loans": active_loans,
            "q": q or "",
            "status": status or "",
            "category_id": category_id or "",
            "location_id": location_id or "",
            "sort": sort,
            "order": order,
            "page": page,
            "total_pages": total_pages,
            "total": total,
            "page_size": PAGE_SIZE,
            "categories": categories,
            "locations": locations,
            "category_map": category_map,
            "location_map": location_map,
        },
    )


@router.post("/ui/assets")
def create_asset_ui(
    name: str = Form(...),
    asset_tag: str = Form(...),
    category: Optional[str] = Form(None),
    location: Optional[str] = Form(None),
    note: Optional[str] = Form(None),
    db: Session = Depends(get_db),
):
    if crud.asset_tag_exists(db, asset_tag):
        return RedirectResponse(url="/ui/assets", status_code=303)

    body = AssetIn(
        name=name,
        asset_tag=asset_tag,
        category=category,
        location=location,
        note=note,
    )
    crud.create_asset(db, body)
    return RedirectResponse(url="/ui/assets", status_code=303)


@router.get("/ui/assets/{asset_id}/edit", response_class=HTMLResponse)
def edit_asset_ui(request: Request, asset_id: str, db: Session = Depends(get_db)):
    asset = crud.get_asset(db, asset_id)
    if not asset:
        raise HTTPException(status_code=404, detail="asset not found")

    categories = crud.list_categories(db)
    locations = crud.list_locations(db)

    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "asset_edit.html",
        {
            "asset": asset,
            "categories": categories,
            "locations": locations,
        },
    )


@router.post("/ui/assets/{asset_id}/edit")
def update_asset_ui(
    asset_id: str,
    name: str = Form(...),
    asset_tag: str = Form(...),
    category: Optional[str] = Form(None),
    location: Optional[str] = Form(None),
    note: Optional[str] = Form(None),
    status: str = Form("available"),
    db: Session = Depends(get_db),
):
    if crud.asset_tag_exists(db, asset_tag, exclude_asset_id=asset_id):
        return RedirectResponse(url=f"/ui/assets/{asset_id}/edit", status_code=303)

    body = AssetUpdate(
        name=name,
        asset_tag=asset_tag,
        category=category,
        location=location,
        note=note,
        status=status,  # type: ignore[arg-type]
    )
    crud.update_asset(db, asset_id, body)
    return RedirectResponse(url="/ui/assets", status_code=303)


@router.post("/ui/assets/{asset_id}/delete")
def delete_asset_ui(
    asset_id: str,
    db: Session = Depends(get_db),
):
    crud.delete_asset(db, asset_id)
    return RedirectResponse(url="/ui/assets", status_code=303)


@router.post("/ui/assets/{asset_id}/loan")
def loan_asset_ui(
    asset_id: str,
    borrower: str = Form(...),
    due_date: Optional[str] = Form(None),
    note: Optional[str] = Form(None),
    db: Session = Depends(get_db),
):
    due_at = datetime.fromisoformat(due_date) if due_date else None
    crud.loan_asset(db, asset_id, borrower=borrower, due_at=due_at, note=note)
    return RedirectResponse(url="/ui/assets", status_code=303)


@router.post("/ui/assets/{asset_id}/return")
def return_asset_ui(
    asset_id: str,
    db: Session = Depends(get_db),
):
    crud.return_asset(db, asset_id)
    return RedirectResponse(url="/ui/assets", status_code=303)


@router.get("/ui/import", response_class=HTMLResponse)
def import_ui(request: Request):
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "import.html",
        {"result": None},
    )


@router.post("/ui/import", response_class=HTMLResponse)
async def import_ui_post(
    request: Request,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    data = await file.read()
    rows, err = csv_bytes_to_rows(data)

    templates = request.app.state.templates
    if err:
        return templates.TemplateResponse(
            request,
            "import.html",
            {"result": {"error": err}},
        )

    result = crud.bulk_import_assets(db, rows)
    return templates.TemplateResponse(
        request,
        "import.html",
        {"result": result},
    )


@router.get("/ui/assets/export")
def export_assets(
    q: Optional[str] = None,
    status: Optional[str] = None,
    category_id: Optional[str] = None,
    location_id: Optional[str] = None,
    sort: str = "asset_tag",
    order: str = "asc",
    db: Session = Depends(get_db),
):
    status = normalize_status(status)
    category_id = blank_to_none(category_id)
    location_id = blank_to_none(location_id)
    sort = normalize_sort(sort)
    order = normalize_order(order)

    assets = crud.list_assets_filtered(
        db,
        q=q,
        status=status,
        category_id=category_id,
        location_id=location_id,
        sort=sort,
        order=order,
        limit=EXPORT_LIMIT,
        offset=0,
    )
    return assets_to_csv_response(assets, filename="assets_export.csv")
