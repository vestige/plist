from csv_utils import csv_bytes_to_rows, decode_csv_bytes, normalize_header


def test_normalize_header_basic():
    assert normalize_header("name") == "name"
    assert normalize_header("assetTag") == "asset_tag"
    assert normalize_header("管理番号") == "asset_tag"
    assert normalize_header("カテゴリ") == "category"


def test_csv_bytes_to_rows_utf8_basic():
    data = b"name,asset_tag,category,location,note\nHDMI Cable,A-001,Cable,Shelf A,ok\n"
    rows, err = csv_bytes_to_rows(data)
    assert err is None
    assert rows == [
        {
            "name": "HDMI Cable",
            "asset_tag": "A-001",
            "category": "Cable",
            "location": "Shelf A",
            "note": "ok",
        }
    ]


def test_csv_bytes_to_rows_utf8_bom():
    # utf-8-sig で読めること（BOM付き）
    text = "name,asset_tag\nUSB Hub,A-002\n"
    data = text.encode("utf-8-sig")
    rows, err = csv_bytes_to_rows(data)
    assert err is None
    assert rows[0]["name"] == "USB Hub"
    assert rows[0]["asset_tag"] == "A-002"


def test_csv_bytes_to_rows_cp932_decoding():
    # cp932 で読めること（日本語ヘッダ）
    text = "名前,管理番号,場所\nプロジェクター,P-001,棚A\n"
    data = text.encode("cp932")
    rows, err = csv_bytes_to_rows(data)
    assert err is None
    assert rows == [{"name": "プロジェクター", "asset_tag": "P-001", "location": "棚A"}]


def test_csv_bytes_to_rows_header_not_found():
    # fieldnames が取れないケース
    rows, err = csv_bytes_to_rows(b"")
    assert rows == []
    assert err == "CSV header not found"


def test_csv_bytes_to_rows_none_values_become_empty_string():
    # DictReader は列が足りないと None を入れることがある
    data = b"name,asset_tag,category\nHDMI Cable,A-001\n"
    rows, err = csv_bytes_to_rows(data)
    assert err is None
    assert rows[0]["category"] == ""
