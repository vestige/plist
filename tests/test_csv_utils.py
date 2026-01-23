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


def test_assets_list_filter_sort_paging(client):
    _create_asset(client, "HDMI Cable", "A-001", category="Cable", location="Shelf A")
    _create_asset(client, "USB Hub", "A-002", category="Adapter", location="Shelf A")
    _create_asset(client, "Monitor", "A-003", category="Display", location="Shelf B")

    # limit/offset paging
    r = client.get("/assets?limit=2&offset=0&sort=asset_tag&order=asc")
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 2
    assert data[0]["asset_tag"] == "A-001"
    assert data[1]["asset_tag"] == "A-002"

    # filter by category
    r = client.get("/assets?category=Cable&limit=50&offset=0")
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 1
    assert data[0]["name"] == "HDMI Cable"

    # search q
    r = client.get("/assets?q=usb&limit=50&offset=0")
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 1
    assert data[0]["asset_tag"] == "A-002"

    # empty-string filters should not 422
    r = client.get("/assets?status=&category=&location=")
    assert r.status_code == 200


def test_assets_meta_matches_total(client):
    _create_asset(client, "HDMI Cable", "A-001", category="Cable", location="Shelf A")
    _create_asset(client, "HDMI Adapter", "A-002", category="Adapter", location="Shelf A")
    _create_asset(client, "Monitor", "A-003", category="Display", location="Shelf B")

    # q=HDMI should match 2
    meta = client.get("/assets/meta?q=hdmi&limit=50&offset=0")
    assert meta.status_code == 200, meta.text
    m = meta.json()
    assert m["total"] == 2
    assert m["total_pages"] >= 1
