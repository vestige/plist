# tests/test_categories_ui.py

def test_category_crud_smoke(client):
    # 作成
    r = client.post("/ui/categories", data={"name": "Cable"})
    assert r.status_code in (200, 303)

    # 一覧に出る
    r = client.get("/ui/categories")
    assert "Cable" in r.text

    # 名前変更
    r = client.post(
        "/ui/categories",
        data={"name": "Cable2"},
    )
    assert r.status_code in (200, 303)

    r = client.get("/ui/categories")
    assert "Cable2" in r.text
