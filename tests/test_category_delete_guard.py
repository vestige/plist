# tests/test_category_delete_guard.py

from sqlalchemy import select

from orm import CategoryORM

def test_category_delete_guard(client, db_session):
    # カテゴリ作成
    client.post("/ui/categories", data={"name": "Monitor"})

    # 備品作成（カテゴリを使う）
    client.post(
        "/ui/assets",
        data={
            "name": "Dell Monitor",
            "asset_tag": "M-001",
            "category": "Monitor",
        },
    )

    category_id = db_session.execute(
        select(CategoryORM.id).where(CategoryORM.name == "Monitor")
    ).scalar_one()

    # カテゴリ削除を試みる
    r = client.post(f"/ui/categories/{category_id}/delete")
    assert r.status_code in (200, 303)

    # まだ残っている
    db_session.expire_all()
    remaining = db_session.get(CategoryORM, category_id)
    assert remaining is not None
    assert remaining.name == "Monitor"
