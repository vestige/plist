import csv
import io
from typing import Iterable, Any, Sequence, Callable, Optional
from fastapi.responses import StreamingResponse

def decode_csv_bytes(data: bytes) -> str:
    # Windowsでありがちな順に試す：UTF-8(BOM) → UTF-8 → CP932
    for enc in ("utf-8-sig", "utf-8", "cp932"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    # 最後の手段
    return data.decode("utf-8", errors="replace")


def normalize_header(h: str) -> str:
    h = (h or "").strip()
    mapping = {
        # 英語
        "name": "name",
        "asset_tag": "asset_tag",
        "assettag": "asset_tag",
        "tag": "asset_tag",
        "category": "category",
        "location": "location",
        "note": "note",
        # 日本語
        "名前": "name",
        "備品名": "name",
        "管理番号": "asset_tag",
        "資産番号": "asset_tag",
        "カテゴリ": "category",
        "分類": "category",
        "場所": "location",
        "保管場所": "location",
        "メモ": "note",
        "備考": "note",
    }
    key = h.lower()
    return mapping.get(h, mapping.get(key, h))

def assets_to_csv_response(
    assets: Iterable[Any],
    *,
    filename: str = "assets_export.csv",
    columns: Optional[Sequence[tuple[str, Callable[[Any], str]]]] = None,
) -> StreamingResponse:
    """
    assets(iterable) を CSV にしてダウンロードさせる StreamingResponse を返す。
    ORM/PydanticどちらでもOK（属性アクセスできればOK）
    """

    if columns is None:
        columns = [
            ("id", lambda a: str(getattr(a, "id", ""))),
            ("name", lambda a: str(getattr(a, "name", ""))),
            ("asset_tag", lambda a: str(getattr(a, "asset_tag", ""))),
            ("category", lambda a: str(getattr(a, "category", "") or "")),
            ("location", lambda a: str(getattr(a, "location", "") or "")),
            ("status", lambda a: str(getattr(a, "status", ""))),
            ("updated_at", lambda a: (
                getattr(a, "updated_at").isoformat()
                if getattr(a, "updated_at", None) is not None and hasattr(getattr(a, "updated_at"), "isoformat")
                else str(getattr(a, "updated_at", "") or "")
            )),
            ("note", lambda a: str(getattr(a, "note", "") or "")),
        ]

    def generate():
        buf = io.StringIO()
        w = csv.writer(buf)

        # header
        w.writerow([h for h, _ in columns])
        yield buf.getvalue()
        buf.seek(0)
        buf.truncate(0)

        # rows
        for a in assets:
            w.writerow([getter(a) for _, getter in columns])
            yield buf.getvalue()
            buf.seek(0)
            buf.truncate(0)

    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(generate(), media_type="text/csv; charset=utf-8", headers=headers)

def csv_bytes_to_rows(data: bytes) -> tuple[list[dict[str, str]], str | None]:
    """
    CSVのバイト列を rows(list[dict]) に変換する。
    戻り値: (rows, error_message)
      - 成功: (rows, None)
      - 失敗: ([], "CSV header not found") など
    """
    text = decode_csv_bytes(data)
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        return [], "CSV header not found"

    field_map = {fn: normalize_header(fn) for fn in reader.fieldnames}

    rows: list[dict[str, str]] = []
    for raw in reader:
        row: dict[str, str] = {}
        for k, v in raw.items():
            nk = field_map.get(k, k)
            row[nk] = v if v is not None else ""
        rows.append(row)

    return rows, None