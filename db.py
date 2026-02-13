from pathlib import Path
import os
import sys
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase

def app_root_dir() -> Path:
    if getattr(sys, "frozen", False):
        exe_dir = Path(sys.executable).resolve().parent  
        return exe_dir.parent
    return Path(__file__).resolve().parent

def resolve_db_path(root_dir: Path) -> Path:
    custom_path = os.getenv("APP_DB_PATH")
    if not custom_path:
        data_dir = root_dir / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        return data_dir / "equip.db"

    db_path = Path(custom_path).expanduser()
    if not db_path.is_absolute():
        db_path = (root_dir / db_path).resolve()

    db_path.parent.mkdir(parents=True, exist_ok=True)
    return db_path

ROOT_DIR = app_root_dir()
DB_PATH = resolve_db_path(ROOT_DIR)
DATABASE_URL = f"sqlite:///{DB_PATH.as_posix()}"

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

class Base(DeclarativeBase):
    pass
