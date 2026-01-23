#!/usr/bin/env python3
# bulk_load_sqlite.py
import argparse
import csv
import io
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from uuid import uuid4


def decode_csv_bytes(data: bytes) -> str:
    # Windowsでありがちな順に試す
    for enc in ("utf-8-sig", "utf-8", "cp932"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def normalize_header(h: str) -> str:
    h = (h or "").strip()
    # 日本語ヘッダも軽く対応
    mapping = {
        "name": "name",
        "asset_tag": "asset_tag",
        "category": "category",
        "location": "location",
        "note": "note",
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


def ensure_schema(conn: sqlite3.Connection) -> None:
    # いまのアプリのスキーマ相当（最小）
    conn.execute("""
    CREATE TABLE IF NOT EXISTS assets (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      asset_tag TEXT NOT NULL UNIQUE,
      category TEXT,
      location TEXT,
      note TEXT,
      status TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS ix_assets_asset_tag ON assets(asset_tag)")
    conn.execute("""
    CREATE TABLE IF NOT EXISTS loans (
      id TEXT PRIMARY KEY,
      asset_id TEXT NOT NULL,
      borrower TEXT NOT NULL,
      loaned_at TEXT NOT NULL,
      due_at TEXT,
      returned_at TEXT,
      note TEXT,
      FOREIGN KEY(asset_id) REFERENCES assets(id)
    )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS ix_loans_asset_id ON loans(asset_id)")
    conn.commit()


def set_fast_pragmas(conn: sqlite3.Connection) -> None:
    # 高速化（ローカル一括投入向け）
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=OFF;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA cache_size=-200000;")  # 約200MB（負ならKB指定）
    conn.execute("PRAGMA foreign_keys=ON;")      # loans削除順の安全
    # conn.execute("PRAGMA mmap_size=268435456;") # 環境によっては効く


def wipe_all(conn: sqlite3.Connection) -> None:
    # FKがあるので loans -> assets の順
    conn.execute("DELETE FROM loans;")
    conn.execute("DELETE FROM assets;")
    conn.commit()
    # ファイルサイズを詰めたい場合（時間はかかる）
    # conn.execute("VACUUM;")
    # conn.commit()


def parse_csv_rows(csv_path: Path) -> list[dict[str, str]]:
    data = csv_path.read_bytes()
    text = decode_csv_bytes(data)

    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise ValueError("CSV header not found")

    field_map = {fn: normalize_header(fn) for fn in reader.fieldnames}

    rows: list[dict[str, str]] = []
    for raw in reader:
        row: dict[str, str] = {}
        for k, v in raw.items():
            nk = field_map.get(k, k)
            row[nk] = (v or "").strip()
        rows.append(row)
    return rows


def bulk_insert_assets(
    conn: sqlite3.Connection,
    rows: list[dict[str, str]],
    chunk_size: int = 5000,
) -> dict:
    """
    INSERT OR IGNORE で asset_tag 重複は自動スキップ。
    """
    now = datetime.utcnow().isoformat(timespec="seconds")

    created = 0
    skipped = 0
    errors: list[str] = []

    sql = """
    INSERT OR IGNORE INTO assets
      (id, name, asset_tag, category, location, note, status, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    # 1トランザクションでまとめて投入
    conn.execute("BEGIN;")
    try:
        batch: list[tuple] = []
        for idx, r in enumerate(rows, start=1):
            name = (r.get("name") or "").strip()
            asset_tag = (r.get("asset_tag") or "").strip()
            if not name or not asset_tag:
                errors.append(f"row {idx}: name/asset_tag is empty")
                continue

            category = (r.get("category") or "").strip() or None
            location = (r.get("location") or "").strip() or None
            note = (r.get("note") or "").strip() or None

            batch.append((
                str(uuid4()),
                name,
                asset_tag,
                category,
                location,
                note,
                "available",
                now,
                now,
            ))

            if len(batch) >= chunk_size:
                cur = conn.executemany(sql, batch)
                # sqlite3はrowcountが信用できないことがあるので、changes()で取得
                created += conn.execute("SELECT changes();").fetchone()[0]
                batch.clear()

        if batch:
            conn.executemany(sql, batch)
            created += conn.execute("SELECT changes();").fetchone()[0]

        conn.execute("COMMIT;")
    except Exception:
        conn.execute("ROLLBACK;")
        raise

    # スキップ数（重複）は、(投入対象 - created - エラー) で概算
    attempted = len(rows) - len(errors)
    skipped = max(0, attempted - created)

    return {"created": created, "skipped": skipped, "errors": errors}


def main():
    ap = argparse.ArgumentParser(description="SQLite wipe + fast CSV import for equip.db")
    ap.add_argument("--db", default="equip.db", help="Path to SQLite DB (default: equip.db)")
    ap.add_argument("--wipe", action="store_true", help="Delete all rows from assets/loans")
    ap.add_argument("--csv", help="CSV file path to import into assets")
    ap.add_argument("--chunk", type=int, default=5000, help="Chunk size for executemany (default: 5000)")
    args = ap.parse_args()

    db_path = Path(args.db)
    conn = sqlite3.connect(db_path.as_posix())
    try:
        set_fast_pragmas(conn)
        ensure_schema(conn)

        if args.wipe:
            wipe_all(conn)
            print("Wipe: OK (assets/loans deleted)")

        if args.csv:
            csv_path = Path(args.csv)
            if not csv_path.exists():
                raise FileNotFoundError(csv_path)

            rows = parse_csv_rows(csv_path)
            result = bulk_insert_assets(conn, rows, chunk_size=args.chunk)
            print(f"Import: created={result['created']} skipped={result['skipped']} errors={len(result['errors'])}")
            if result["errors"]:
                # 最初の10件だけ表示
                print("Errors (first 10):")
                for e in result["errors"][:10]:
                    print("  -", e)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
