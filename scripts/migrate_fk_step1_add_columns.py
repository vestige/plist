#!/usr/bin/env python3
# scripts/migrate_fk_step1_add_columns.py
import argparse
import sqlite3
from pathlib import Path


def column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table});").fetchall()
    # PRAGMA table_info: (cid, name, type, notnull, dflt_value, pk)
    return any(r[1] == column for r in rows)


def main() -> None:
    ap = argparse.ArgumentParser(description="Add category_id/location_id columns to assets (idempotent).")
    ap.add_argument("--db", default="equip.db", help="Path to SQLite DB (default: equip.db)")
    args = ap.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise FileNotFoundError(db_path)

    conn = sqlite3.connect(db_path.as_posix())
    try:
        conn.execute("PRAGMA foreign_keys=ON;")

        # assetsに列追加（存在しない場合だけ）
        if not column_exists(conn, "assets", "category_id"):
            conn.execute("ALTER TABLE assets ADD COLUMN category_id TEXT;")
            print("Added column: assets.category_id")
        else:
            print("Already exists: assets.category_id")

        if not column_exists(conn, "assets", "location_id"):
            conn.execute("ALTER TABLE assets ADD COLUMN location_id TEXT;")
            print("Added column: assets.location_id")
        else:
            print("Already exists: assets.location_id")

        # インデックス（存在しない場合だけ）
        conn.execute("CREATE INDEX IF NOT EXISTS ix_assets_category_id ON assets(category_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS ix_assets_location_id ON assets(location_id);")
        print("Ensured indexes: ix_assets_category_id, ix_assets_location_id")

        conn.commit()
        print("OK")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
