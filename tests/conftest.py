import os
import importlib

import pytest
from fastapi.testclient import TestClient
from fastapi.templating import Jinja2Templates


@pytest.fixture(scope="session")
def app_module(tmp_path_factory):
    # ---- テスト用DBパス ----
    tmp_dir = tmp_path_factory.mktemp("equip_app")
    db_path = tmp_dir / "test_equip.db"
    os.environ["APP_DB_PATH"] = str(db_path)

    # ---- 最小テンプレ（UIルートが落ちないため）----
    tmpl_dir = tmp_dir / "templates"
    tmpl_dir.mkdir(parents=True, exist_ok=True)

    (tmpl_dir / "assets.html").write_text(
        "<html><body>assets ok ({{ assets|length }})</body></html>",
        encoding="utf-8",
    )
    (tmpl_dir / "import.html").write_text(
        "<html><body>import ok</body></html>",
        encoding="utf-8",
    )
    (tmpl_dir / "asset_edit.html").write_text(
        "<html><body>edit ok {{ asset.name }} {% for c in categories %}{{ c }}{% endfor %}</body></html>",
        encoding="utf-8",
    )
    (tmpl_dir / "categories.html").write_text(
    "<html><body>categories ok {% for (cid, name) in categories %}{{ name }}{% endfor %}</body></html>",
    encoding="utf-8",
    )

    (tmpl_dir / "locations.html").write_text(
        "<html><body>locations ok {% for (lid, name) in locations %}{{ name }}{% endfor %}</body></html>",
        encoding="utf-8",
    )

    # ---- reloadして、テスト用APP_DB_PATHを反映させる ----
    import db
    import orm
    import crud
    import main

    importlib.reload(db)
    importlib.reload(orm)
    importlib.reload(crud)
    importlib.reload(main)

    # templatesディレクトリをテスト用に差し替え
    main.templates = Jinja2Templates(directory=str(tmpl_dir))
    main.app.state.templates = main.templates

    return main


@pytest.fixture()
def client(app_module):
    # get_db を override（テスト用SessionLocalを使う）
    def _get_db_override():
        db = app_module.SessionLocal()
        try:
            yield db
        finally:
            db.close()

    app_module.app.dependency_overrides[app_module.get_db] = _get_db_override
    with TestClient(app_module.app) as c:
        yield c
    app_module.app.dependency_overrides.clear()


@pytest.fixture()
def db_session(app_module):
    db = app_module.SessionLocal()
    try:
        yield db
    finally:
        db.close()


@pytest.fixture(autouse=True)
def clean_db(app_module, db_session):
    # 各テスト前にテーブルを全消し（順序注意：loans -> assets -> masters）
    from sqlalchemy import delete
    from orm import LoanORM, AssetORM, CategoryORM, LocationORM

    db_session.execute(delete(LoanORM))
    db_session.execute(delete(AssetORM))
    db_session.execute(delete(CategoryORM))
    db_session.execute(delete(LocationORM))
    db_session.commit()
    yield
