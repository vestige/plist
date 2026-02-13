from .assets_api import router as assets_api_router
from .assets_ui import router as assets_ui_router
from .masters_ui import router as masters_ui_router

ALL_ROUTERS = (
    assets_api_router,
    assets_ui_router,
    masters_ui_router,
)
