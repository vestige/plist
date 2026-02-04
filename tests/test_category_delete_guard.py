# tests/test_category_delete_guard.py

def test_category_delete_guard(client):
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

    # カテゴリ削除を試みる
    r = client.post("/ui/categories/Monitor/delete")
    assert r.status_code in (200, 303)

    # まだ残っている
    r = client.get("/ui/categories")
    assert "Monitor" in r.text
