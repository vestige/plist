def _create_asset(client, name, tag, category=None, location=None, note=None):
    body = {
        "name": name,
        "asset_tag": tag,
        "category": category,
        "location": location,
        "note": note,
    }
    r = client.post("/assets", json=body)
    assert r.status_code == 201, r.text
    return r.json()


def test_ui_assets_smoke(client):
    _create_asset(client, "HDMI Cable", "A-001", category="Cable", location="Shelf A")
    r = client.get("/ui/assets")
    assert r.status_code == 200
    assert "assets ok" in r.text


def test_export_csv_smoke(client):
    _create_asset(client, "HDMI Cable", "A-001", category="Cable", location="Shelf A")
    _create_asset(client, "USB Hub", "A-002", category="Adapter", location="Shelf A")

    # 空文字フィルタでも落ちない
    r = client.get("/ui/assets/export?status=&category=&location=")
    assert r.status_code == 200, r.text
    ct = r.headers.get("content-type", "")
    assert "text/csv" in ct

    text = r.text.strip().splitlines()
    assert text[0].startswith("id,name,asset_tag,category,location,status,updated_at,note")
    assert len(text) == 1 + 2  # header + 2 rows
