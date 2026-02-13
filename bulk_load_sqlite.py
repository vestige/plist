#!/usr/bin/env python3
# bulk_load_sqlite.py
import argparse
import csv
import io
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


def decode_csv_bytes(data: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "cp932"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def normalize_header(h: str) -> str:
    h = (h or "").strip()
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
    # assets
    conn.execute("""
    CREATE TABLE IF NOT EXISTS assets (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      asset_tag TEXT NOT NULL UNIQUE,
      category TEXT,
      location TEXT,
      category_id TEXT,
      location_id TEXT,                    
      note TEXT,
      status TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS ix_assets_asset_tag ON assets(asset_tag)")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_assets_category ON assets(category)")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_assets_location ON assets(location)")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_assets_category_id ON assets(category_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_assets_location_id ON assets(location_id)")

    # loans
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

    # masters: categories / locations
    conn.execute("""
    CREATE TABLE IF NOT EXISTS categories (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL UNIQUE,
      sort_order INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS ix_categories_name ON categories(name)")

    conn.execute("""
    CREATE TABLE IF NOT EXISTS locations (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL UNIQUE,
      sort_order INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS ix_locations_name ON locations(name)")

    conn.commit()


def set_fast_pragmas(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=OFF;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA cache_size=-200000;")
    conn.execute("PRAGMA foreign_keys=ON;")


def wipe_all(conn: sqlite3.Connection) -> None:
    conn.execute("DELETE FROM loans;")
    conn.execute("DELETE FROM assets;")
    conn.execute("DELETE FROM categories;")
    conn.execute("DELETE FROM locations;")
    conn.commit()


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


def bulk_upsert_masters_from_rows(
    conn: sqlite3.Connection,
    rows: list[dict[str, str]],
    chunk_size: int = 5000,
) -> dict:
    """
    CSV行から category/location のユニーク集合を作って、
    categories/locations に INSERT OR IGNORE。
    """
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    categories = set()
    locations = set()
    for r in rows:
        c = (r.get("category") or "").strip()
        l = (r.get("location") or "").strip()
        if c:
            categories.add(c)
        if l:
            locations.add(l)

    cat_created = 0
    loc_created = 0

    sql_cat = """
    INSERT OR IGNORE INTO categories
      (id, name, sort_order, created_at, updated_at)
    VALUES (?, ?, 0, ?, ?)
    """
    sql_loc = """
    INSERT OR IGNORE INTO locations
      (id, name, sort_order, created_at, updated_at)
    VALUES (?, ?, 0, ?, ?)
    """

    conn.execute("BEGIN;")
    try:
        # categories
        batch: list[tuple] = []
        for name in sorted(categories):
            batch.append((str(uuid4()), name, now, now))
            if len(batch) >= chunk_size:
                conn.executemany(sql_cat, batch)
                cat_created += conn.execute("SELECT changes();").fetchone()[0]
                batch.clear()
        if batch:
            conn.executemany(sql_cat, batch)
            cat_created += conn.execute("SELECT changes();").fetchone()[0]

        # locations
        batch = []
        for name in sorted(locations):
            batch.append((str(uuid4()), name, now, now))
            if len(batch) >= chunk_size:
                conn.executemany(sql_loc, batch)
                loc_created += conn.execute("SELECT changes();").fetchone()[0]
                batch.clear()
        if batch:
            conn.executemany(sql_loc, batch)
            loc_created += conn.execute("SELECT changes();").fetchone()[0]

        conn.execute("COMMIT;")
    except Exception:
        conn.execute("ROLLBACK;")
        raise

    return {
        "categories_total": len(categories),
        "locations_total": len(locations),
        "categories_created": cat_created,
        "locations_created": loc_created,
    }


def bulk_insert_assets(
    conn: sqlite3.Connection,
    rows: list[dict[str, str]],
    chunk_size: int = 5000,
) -> dict:
    """
    INSERT OR IGNORE で asset_tag 重複は自動スキップ。
    """
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    created = 0
    errors: list[str] = []

    sql = """
    INSERT OR IGNORE INTO assets
    (id, name, asset_tag, category, location, category_id, location_id, note, status, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
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

            category_id = None
            location_id = None

            if category:
                row = conn.execute("SELECT id FROM categories WHERE name = ?", (category,)).fetchone()
                category_id = row[0] if row else None
            if location:
                row = conn.execute("SELECT id FROM locations WHERE name = ?", (location,)).fetchone()
                location_id = row[0] if row else None


            batch.append((
                str(uuid4()),
                name,
                asset_tag,
                category,
                location,
                category_id,
                location_id,
                note,
                "available",
                now,
                now,
            ))

            if len(batch) >= chunk_size:
                conn.executemany(sql, batch)
                created += conn.execute("SELECT changes();").fetchone()[0]
                batch.clear()

        if batch:
            conn.executemany(sql, batch)
            created += conn.execute("SELECT changes();").fetchone()[0]

        conn.execute("COMMIT;")
    except Exception:
        conn.execute("ROLLBACK;")
        raise

    attempted = len(rows) - len(errors)
    skipped = max(0, attempted - created)

    return {"created": created, "skipped": skipped, "errors": errors}


def main():
    ap = argparse.ArgumentParser(description="SQLite wipe + fast CSV import for equip.db")
    ap.add_argument("--db", default="equip.db", help="Path to SQLite DB (default: equip.db)")
    ap.add_argument("--wipe", action="store_true", help="Delete all rows from assets/loans/categories/locations")
    ap.add_argument("--csv", help="CSV file path to import into assets (also sync master tables)")
    ap.add_argument("--chunk", type=int, default=5000, help="Chunk size for executemany (default: 5000)")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    logger = logging.getLogger("bulk_load_sqlite")

    db_path = Path(args.db)
    conn = sqlite3.connect(db_path.as_posix())
    try:
        set_fast_pragmas(conn)
        ensure_schema(conn)

        if args.wipe:
            wipe_all(conn)
            logger.info("Wipe OK (assets/loans/categories/locations deleted)")

        if args.csv:
            csv_path = Path(args.csv)
            if not csv_path.exists():
                raise FileNotFoundError(csv_path)

            rows = parse_csv_rows(csv_path)

            master_result = bulk_upsert_masters_from_rows(conn, rows, chunk_size=args.chunk)
            logger.info(
                "Masters categories_created=%s/%s locations_created=%s/%s",
                master_result["categories_created"],
                master_result["categories_total"],
                master_result["locations_created"],
                master_result["locations_total"],
            )

            result = bulk_insert_assets(conn, rows, chunk_size=args.chunk)
            logger.info(
                "Assets created=%s skipped=%s errors=%s",
                result["created"],
                result["skipped"],
                len(result["errors"]),
            )
            if result["errors"]:
                logger.warning("Errors (first 10):")
                for e in result["errors"][:10]:
                    logger.warning("  - %s", e)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
