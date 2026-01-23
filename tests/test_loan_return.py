def _create_asset(client, name, tag):
    r = client.post("/assets", json={"name": name, "asset_tag": tag})
    assert r.status_code == 201, r.text
    return r.json()


def test_loan_and_return_flow(client, db_session):
    a = _create_asset(client, "Projector", "P-001")
    asset_id = a["id"]

    # loan (UI form endpoint)
    r = client.post(f"/ui/assets/{asset_id}/loan", data={"borrower": "Alice", "due_date": "", "note": ""})
    assert r.status_code in (200, 303)  # RedirectResponse(303) が通常

    # status becomes loaned
    r = client.get(f"/assets/{asset_id}")
    assert r.status_code == 200
    assert r.json()["status"] == "loaned"

    # active loan exists (crud level)
    import crud
    active = crud.get_active_loan(db_session, asset_id)
    assert active is not None
    assert active.borrower == "Alice"
    assert active.returned_at is None

    # return
    r = client.post(f"/ui/assets/{asset_id}/return")
    assert r.status_code in (200, 303)

    r = client.get(f"/assets/{asset_id}")
    assert r.status_code == 200
    assert r.json()["status"] == "available"

    active2 = crud.get_active_loan(db_session, asset_id)
    assert active2 is None
