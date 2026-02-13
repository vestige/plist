import logging
import sys
import time
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates

from db import Base, SessionLocal, engine
from dependencies import get_db
from routers import ALL_ROUTERS


def resource_dir() -> Path:
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return Path(sys._MEIPASS)
    return Path(__file__).resolve().parent


BASE_DIR = resource_dir()
app = FastAPI(title="備品管理API")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
app.state.templates = templates

# テーブルを自動作成（最初だけ）
Base.metadata.create_all(bind=engine)


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


@app.get("/")
def root():
    return {"message": "備品管理API", "docs": "/docs", "ui": "/ui/assets"}


for router in ALL_ROUTERS:
    app.include_router(router)


if __name__ == "__main__":
    import os

    import uvicorn

    port = int(os.getenv("PORT", "1234"))
    uvicorn.run(app, host="0.0.0.0", port=port)
