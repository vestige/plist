def test_location_crud_smoke(client):
    # 作成
    r = client.post("/ui/locations", data={"name": "Shelf A", "sort_order": 0})
    assert r.status_code in (200, 303)

    # 一覧に出る
    r = client.get("/ui/locations")
    assert r.status_code == 200
    assert "Shelf A" in r.text
