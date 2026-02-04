from fastapi import FastAPI, HTTPException, Request, Form, UploadFile, File, Depends
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from sqlalchemy import select

from typing import Optional
from datetime import datetime, timezone
import logging
import time

from models import Asset, AssetIn, AssetUpdate, Status, AssetsMeta
from orm import CategoryORM, LocationORM
from db import Base, SessionLocal, engine
from csv_utils import decode_csv_bytes, normalize_header, assets_to_csv_response, csv_bytes_to_rows

import orm
import crud

app = FastAPI(title="備品管理API")
templates = Jinja2Templates(directory="templates")

Base.metadata.create_all(bind=engine)

# -----------------------
# Logging
# -----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("app")

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.time()
    response = await call_next(request)
    elapsed_ms = int((time.time() - start) * 1000)
    logger.info(
        "method=%s path=%s status=%s elapsed_ms=%s",
        request.method,
        request.url.path,
        response.status_code,
        elapsed_ms,
    )
    return response

def resolve_category_id(db: Session, name: str | None) -> str | None:
    if not name:
        return None
    row = db.execute(select(CategoryORM.id).where(CategoryORM.name == name)).first()
    return row[0] if row else None

def resolve_location_id(db: Session, name: str | None) -> str | None:
    if not name:
        return None
    row = db.execute(select(LocationORM.id).where(LocationORM.name == name)).first()
    return row[0] if row else None

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/")
def root():
    return {"message": "備品管理API", "docs": "/docs", "ui": "/ui/assets"}

# -----------------------
# API: /assets
# -----------------------
@app.get("/assets", response_model=list[Asset])
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
    if sort not in ("asset_tag", "name", "updated_at"):
        sort = "asset_tag"
    if order not in ("asc", "desc"):
        order = "asc"

    if status not in ("available", "loaned", "retired"):
        status = None
    if category == "":
        category = None
    if location == "":
        location = None

    if limit < 1:
        limit = 1
    if limit > 500:
        limit = 500
    if offset < 0:
        offset = 0

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

@app.get("/assets/meta", response_model=AssetsMeta)
def assets_meta_api(
    q: Optional[str] = None,
    category: Optional[str] = None,
    status: Optional[str] = None,
    location: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    db: Session = Depends(get_db),
):
    if status not in ("available", "loaned", "retired"):
        status = None
    if category == "":
        category = None
    if location == "":
        location = None

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

@app.post("/assets", response_model=Asset, status_code=201)
def create_asset_api(
    body: AssetIn,
    db: Session = Depends(get_db),
):
    if crud.asset_tag_exists(db, body.asset_tag):
        raise HTTPException(status_code=409, detail="asset_tag already exists")
    return crud.create_asset(db, body)

@app.get("/assets/{asset_id}", response_model=Asset)
def get_asset_api(
    asset_id: str,
    db: Session = Depends(get_db),
):
    asset = crud.get_asset(db, asset_id)
    if not asset:
        raise HTTPException(status_code=404, detail="asset not found")
    return asset

@app.patch("/assets/{asset_id}", response_model=Asset)
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

@app.delete("/assets/{asset_id}", status_code=204)
def delete_asset_api(
    asset_id: str,
    db: Session = Depends(get_db),
):
    ok = crud.delete_asset(db, asset_id)
    if not ok:
        raise HTTPException(status_code=404, detail="asset not found")
    return None

# -----------------------
# UI: /ui/assets
# -----------------------
@app.get("/ui/assets", response_class=HTMLResponse)
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
    PAGE_SIZE = 50
    if page < 1:
        page = 1

    if status not in ("available", "loaned", "retired"):
        status = None
    if category_id == "":
        category_id = None
    if location_id == "":
        location_id = None

    if sort not in ("asset_tag", "name", "updated_at"):
        sort = "asset_tag"
    if order not in ("asc", "desc"):
        order = "asc"

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
    for a in assets:
        active = crud.get_active_loan(db, a.id)
        if active:
            active_loans[a.id] = active

    categories = crud.list_categories(db)
    locations  = crud.list_locations(db)

    category_map = {cid: cname for cid, cname in categories}
    location_map = {lid: lname for lid, lname in locations}

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
        }
    )

@app.post("/ui/assets")
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

@app.get("/ui/assets/{asset_id}/edit", response_class=HTMLResponse)
def edit_asset_ui(request: Request, asset_id: str, db: Session = Depends(get_db)):
    asset = crud.get_asset(db, asset_id)
    if not asset:
        raise HTTPException(status_code=404, detail="asset not found")

    categories = crud.list_categories(db)
    locations  = crud.list_locations(db)

    return templates.TemplateResponse(
        "asset_edit.html",
        {
            "request": request,
            "asset": asset,
            "categories": categories,
            "locations": locations,
        },
    )

@app.post("/ui/assets/{asset_id}/edit")
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
        status=status,  # type: ignore
    )
    crud.update_asset(db, asset_id, body)
    return RedirectResponse(url="/ui/assets", status_code=303)

@app.post("/ui/assets/{asset_id}/delete")
def delete_asset_ui(
    asset_id: str,
    db: Session = Depends(get_db),
):
    crud.delete_asset(db, asset_id)
    return RedirectResponse(url="/ui/assets", status_code=303)

@app.post("/ui/assets/{asset_id}/loan")
def loan_asset_ui(
    asset_id: str,
    borrower: str = Form(...),
    due_date: Optional[str] = Form(None),  # YYYY-MM-DD
    note: Optional[str] = Form(None),
    db: Session = Depends(get_db),
):
    due_at = datetime.fromisoformat(due_date) if due_date else None
    crud.loan_asset(db, asset_id, borrower=borrower, due_at=due_at, note=note)
    return RedirectResponse(url="/ui/assets", status_code=303)

@app.post("/ui/assets/{asset_id}/return")
def return_asset_ui(
    asset_id: str,
    db: Session = Depends(get_db),
):
    crud.return_asset(db, asset_id)
    return RedirectResponse(url="/ui/assets", status_code=303)

@app.get("/ui/categories", response_class=HTMLResponse)
def categories_ui(request: Request, db: Session = Depends(get_db)):
    categories = crud.list_categories(db)  # [(id, name), ...]
    return templates.TemplateResponse(
        request,
        "categories.html",
        {"categories": categories},
    )

@app.post("/ui/categories")
def create_category_ui(
    name: str = Form(...),
    sort_order: int = Form(0),
    db: Session = Depends(get_db),
):
    crud.create_category(db, name=name, sort_order=sort_order)
    return RedirectResponse(url="/ui/categories", status_code=303)

@app.post("/ui/categories/{category_id}/rename")
def rename_category_ui(
    category_id: str,
    new_name: str = Form(...),
    cascade_assets: str = Form("on"),
    db: Session = Depends(get_db),
):
    cascade = cascade_assets == "on"
    crud.rename_category(db, category_id=category_id, new_name=new_name, cascade_assets=cascade)
    return RedirectResponse(url="/ui/categories", status_code=303)

@app.post("/ui/categories/{category_id}/delete")
def delete_category_ui(
    category_id: str,
    db: Session = Depends(get_db),
):
    crud.delete_category(db, category_id=category_id)
    return RedirectResponse(url="/ui/categories", status_code=303)

@app.get("/ui/locations", response_class=HTMLResponse)
def locations_ui(request: Request, db: Session = Depends(get_db)):
    locations = crud.list_locations(db)  # [(id, name), ...]
    return templates.TemplateResponse(
        request,
        "locations.html",
        {"locations": locations},
    )

@app.post("/ui/locations")
def create_location_ui(
    name: str = Form(...),
    sort_order: int = Form(0),
    db: Session = Depends(get_db),
):
    crud.create_location(db, name=name, sort_order=sort_order)
    return RedirectResponse(url="/ui/locations", status_code=303)

@app.post("/ui/locations/{location_id}/rename")
def rename_location_ui(
    location_id: str,
    new_name: str = Form(...),
    cascade_assets: str = Form("on"),
    db: Session = Depends(get_db),
):
    cascade = cascade_assets == "on"
    crud.rename_location(db, location_id=location_id, new_name=new_name, cascade_assets=cascade)
    return RedirectResponse(url="/ui/locations", status_code=303)

@app.post("/ui/locations/{location_id}/delete")
def delete_location_ui(
    location_id: str,
    db: Session = Depends(get_db),
):
    crud.delete_location(db, location_id=location_id)
    return RedirectResponse(url="/ui/locations", status_code=303)

# ---------------------------------------------------------
@app.get("/ui/import", response_class=HTMLResponse)
def import_ui(request: Request):
    return templates.TemplateResponse(
        "import.html",
        {"request": request, "result": None}
    )

@app.post("/ui/import", response_class=HTMLResponse)
async def import_ui_post(
    request: Request,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    data = await file.read()
    rows, err = csv_bytes_to_rows(data)

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

@app.get("/ui/assets/export")
def export_assets(
    q: Optional[str] = None,
    status: Optional[str] = None,
    category: Optional[str] = None,
    location: Optional[str] = None,
    sort: str = "asset_tag",
    order: str = "asc",
    db: Session = Depends(get_db),
):
    if status not in ("available", "loaned", "retired"):
        status = None
    if category == "":
        category = None
    if location == "":
        location = None    

    if sort not in ("asset_tag", "name", "updated_at"):
        sort = "asset_tag"
    if order not in ("asc", "desc"):
        order = "asc"

    LIMIT = 20000

    category_id = resolve_category_id(db, category)
    location_id = resolve_location_id(db, location)

    assets = crud.list_assets_filtered(
        db,
        q=q,
        status=status,
        category_id=category_id,
        location_id=location_id,
        sort=sort,
        order=order,
        limit=LIMIT,
        offset=0,
    )

    return assets_to_csv_response(assets, filename="assets_export.csv")
