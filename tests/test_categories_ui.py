# tests/test_categories_ui.py

from sqlalchemy import select

from orm import CategoryORM

def test_category_crud_smoke(client, db_session):
    # 作成
    r = client.post("/ui/categories", data={"name": "Cable"})
    assert r.status_code in (200, 303)

    # 一覧に出る
    r = client.get("/ui/categories")
    assert "Cable" in r.text

    # IDで名前変更
    category_id = db_session.execute(
        select(CategoryORM.id).where(CategoryORM.name == "Cable")
    ).scalar_one()

    r = client.post(
        f"/ui/categories/{category_id}/rename",
        data={"new_name": "Cable2"},
    )
    assert r.status_code in (200, 303)

    db_session.expire_all()
    updated = db_session.get(CategoryORM, category_id)
    assert updated is not None
    assert updated.name == "Cable2"

    r = client.get("/ui/categories")
    assert "Cable2" in r.text
