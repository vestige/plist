from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session

import crud
from dependencies import get_db

router = APIRouter()


@router.get("/ui/categories", response_class=HTMLResponse)
def categories_ui(request: Request, db: Session = Depends(get_db)):
    categories = crud.list_categories(db)
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "categories.html",
        {"categories": categories},
    )


@router.post("/ui/categories")
def create_category_ui(
    name: str = Form(...),
    sort_order: int = Form(0),
    db: Session = Depends(get_db),
):
    crud.create_category(db, name=name, sort_order=sort_order)
    return RedirectResponse(url="/ui/categories", status_code=303)


@router.post("/ui/categories/{category_id}/rename")
def rename_category_ui(
    category_id: str,
    new_name: str = Form(...),
    cascade_assets: str = Form("on"),
    db: Session = Depends(get_db),
):
    cascade = cascade_assets == "on"
    crud.rename_category(db, category_id=category_id, new_name=new_name, cascade_assets=cascade)
    return RedirectResponse(url="/ui/categories", status_code=303)


@router.post("/ui/categories/{category_id}/delete")
def delete_category_ui(
    category_id: str,
    db: Session = Depends(get_db),
):
    crud.delete_category(db, category_id=category_id)
    return RedirectResponse(url="/ui/categories", status_code=303)


@router.get("/ui/locations", response_class=HTMLResponse)
def locations_ui(request: Request, db: Session = Depends(get_db)):
    locations = crud.list_locations(db)
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "locations.html",
        {"locations": locations},
    )


@router.post("/ui/locations")
def create_location_ui(
    name: str = Form(...),
    sort_order: int = Form(0),
    db: Session = Depends(get_db),
):
    crud.create_location(db, name=name, sort_order=sort_order)
    return RedirectResponse(url="/ui/locations", status_code=303)


@router.post("/ui/locations/{location_id}/rename")
def rename_location_ui(
    location_id: str,
    new_name: str = Form(...),
    cascade_assets: str = Form("on"),
    db: Session = Depends(get_db),
):
    cascade = cascade_assets == "on"
    crud.rename_location(db, location_id=location_id, new_name=new_name, cascade_assets=cascade)
    return RedirectResponse(url="/ui/locations", status_code=303)


@router.post("/ui/locations/{location_id}/delete")
def delete_location_ui(
    location_id: str,
    db: Session = Depends(get_db),
):
    crud.delete_location(db, location_id=location_id)
    return RedirectResponse(url="/ui/locations", status_code=303)
