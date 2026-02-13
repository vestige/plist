import re

def _extract_asset_id_from_redirect(resp):
    # 例: Location: /ui/assets/<uuid>/edit などを想定
    loc = resp.headers.get("location", "")
    m = re.search(r"/ui/assets/([^/]+)", loc)
    return m.group(1) if m else None


def test_ui_flow_create_edit_loan_return_export(client):
    # 1) 事前にカテゴリ/場所を作る（UI）
    r = client.post("/ui/categories", data={"name": "Cable"})
    assert r.status_code in (200, 303)

    r = client.post("/ui/locations", data={"name": "Shelf A"})
    assert r.status_code in (200, 303)

    # 2) 追加（UI）
    r = client.post(
        "/ui/assets",
        data={
            "name": "HDMI Cable",
            "asset_tag": "A-100",
            "category": "Cable",
            "location": "Shelf A",
            "note": "new",
        },
        follow_redirects=False,
    )
    assert r.status_code in (302, 303)

    # 一覧から追加できたことを確認（UI）
    r = client.get("/ui/assets?q=A-100")
    assert r.status_code == 200

    # 3) APIからasset_idを取る（ここだけAPI使うと安定）
    r = client.get("/assets?q=A-100&limit=10&offset=0")
    assert r.status_code == 200
    items = r.json()
    assert len(items) == 1
    assert items[0]["asset_tag"] == "A-100"
    assert items[0]["name"] == "HDMI Cable"
    asset_id = items[0]["id"]
    
    # 4) 編集（UI）カテゴリ/場所を変えて反映されること
    r = client.post("/ui/categories", data={"name": "Adapter"})
    assert r.status_code in (200, 303)
    r = client.post("/ui/locations", data={"name": "Shelf B"})
    assert r.status_code in (200, 303)

    r = client.post(
        f"/ui/assets/{asset_id}/edit",
        data={
            "name": "HDMI Cable v2",
            "asset_tag": "A-100",
            "category": "Adapter",
            "location": "Shelf B",
            "note": "edited",
            "status": "available",
        },
        follow_redirects=False,
    )
    assert r.status_code in (302, 303)

    # 反映確認（API）
    r = client.get(f"/assets/{asset_id}")
    assert r.status_code == 200
    a = r.json()
    assert a["name"] == "HDMI Cable v2"
    assert a["category"] == "Adapter"
    assert a["location"] == "Shelf B"
    assert a["status"] == "available"

    # 5) 貸出（UI）
    r = client.post(
        f"/ui/assets/{asset_id}/loan",
        data={"borrower": "Alice", "due_date": "", "note": "loaned"},
        follow_redirects=False,
    )
    assert r.status_code in (302, 303)

    r = client.get(f"/assets/{asset_id}")
    assert r.status_code == 200
    a = r.json()
    assert a["status"] == "loaned"

    # 6) 返却（UI）
    r = client.post(
        f"/ui/assets/{asset_id}/return",
        data={"note": "returned"},
        follow_redirects=False,
    )
    assert r.status_code in (302, 303)

    r = client.get(f"/assets/{asset_id}")
    assert r.status_code == 200
    a = r.json()
    assert a["status"] == "available"
    assert a.get("borrower") in (None, "")

    # 7) CSV出力（UI export）に対象が含まれること
    r = client.get("/ui/assets/export?q=A-100&status=&category=&location=&sort=asset_tag&order=asc")
    assert r.status_code == 200
    assert "text/csv" in r.headers.get("content-type", "")
    text = r.text
    assert "A-100" in text
    assert "HDMI Cable v2" in text
