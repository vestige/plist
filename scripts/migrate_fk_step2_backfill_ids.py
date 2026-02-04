#!/usr/bin/env python3
# scripts/migrate_fk_step2_backfill_ids.py
import argparse
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def ensure_column(conn: sqlite3.Connection, table: str, column: str) -> None:
    cols = conn.execute(f"PRAGMA table_info({table});").fetchall()
    if not any(c[1] == column for c in cols):
        raise RuntimeError(f"Column not found: {table}.{column}. Run step1 first.")


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill assets.category_id/location_id from text columns.")
    ap.add_argument("--db", default="equip.db", help="Path to SQLite DB (default: equip.db)")
    ap.add_argument("--dry-run", action="store_true", help="Show counts only, do not write")
    args = ap.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise FileNotFoundError(db_path)

    conn = sqlite3.connect(db_path.as_posix())
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA foreign_keys=ON;")

        ensure_column(conn, "assets", "category_id")
        ensure_column(conn, "assets", "location_id")

        # 事前カウント
        missing_cat = conn.execute("""
            SELECT COUNT(*) AS n
            FROM assets
            WHERE category IS NOT NULL AND TRIM(category) <> ''
              AND (category_id IS NULL OR TRIM(category_id) = '')
        """).fetchone()["n"]

        missing_loc = conn.execute("""
            SELECT COUNT(*) AS n
            FROM assets
            WHERE location IS NOT NULL AND TRIM(location) <> ''
              AND (location_id IS NULL OR TRIM(location_id) = '')
        """).fetchone()["n"]

        uniq_cat = conn.execute("""
            SELECT COUNT(DISTINCT TRIM(category)) AS n
            FROM assets
            WHERE category IS NOT NULL AND TRIM(category) <> ''
        """).fetchone()["n"]

        uniq_loc = conn.execute("""
            SELECT COUNT(DISTINCT TRIM(location)) AS n
            FROM assets
            WHERE location IS NOT NULL AND TRIM(location) <> ''
        """).fetchone()["n"]

        print(f"Assets missing category_id: {missing_cat}")
        print(f"Assets missing location_id: {missing_loc}")
        print(f"Distinct category strings in assets: {uniq_cat}")
        print(f"Distinct location strings in assets: {uniq_loc}")

        if args.dry_run:
            print("Dry-run: no changes.")
            return

        now = now_utc_iso()

        conn.execute("BEGIN;")
        try:
            # 1) assetsからユニークなcategory/locationを集めて、マスタへINSERT OR IGNORE
            #    ※ id は新規挿入時のみ必要。既存は無視される。
            #    SQLiteの executemany で高速投入
            cat_names = [
                r["name"] for r in conn.execute("""
                    SELECT DISTINCT TRIM(category) AS name
                    FROM assets
                    WHERE category IS NOT NULL AND TRIM(category) <> ''
                """).fetchall()
            ]
            loc_names = [
                r["name"] for r in conn.execute("""
                    SELECT DISTINCT TRIM(location) AS name
                    FROM assets
                    WHERE location IS NOT NULL AND TRIM(location) <> ''
                """).fetchall()
            ]

            conn.executemany(
                "INSERT OR IGNORE INTO categories (id, name, sort_order, created_at, updated_at) VALUES (?, ?, 0, ?, ?)",
                [(str(uuid4()), name, now, now) for name in cat_names],
            )
            cat_inserted = conn.execute("SELECT changes();").fetchone()[0]

            conn.executemany(
                "INSERT OR IGNORE INTO locations (id, name, sort_order, created_at, updated_at) VALUES (?, ?, 0, ?, ?)",
                [(str(uuid4()), name, now, now) for name in loc_names],
            )
            loc_inserted = conn.execute("SELECT changes();").fetchone()[0]

            print(f"Inserted into categories: {cat_inserted} (ignored existing: {len(cat_names) - cat_inserted})")
            print(f"Inserted into locations: {loc_inserted} (ignored existing: {len(loc_names) - loc_inserted})")

            # 2) assets.category_id を categories.id で埋める（まだ空のものだけ）
            conn.execute("""
                UPDATE assets
                SET category_id = (
                    SELECT c.id
                    FROM categories c
                    WHERE c.name = TRIM(assets.category)
                )
                WHERE category IS NOT NULL AND TRIM(category) <> ''
                  AND (category_id IS NULL OR TRIM(category_id) = '')
            """)
            cat_updated = conn.execute("SELECT changes();").fetchone()[0]

            # 3) assets.location_id を locations.id で埋める（まだ空のものだけ）
            conn.execute("""
                UPDATE assets
                SET location_id = (
                    SELECT l.id
                    FROM locations l
                    WHERE l.name = TRIM(assets.location)
                )
                WHERE location IS NOT NULL AND TRIM(location) <> ''
                  AND (location_id IS NULL OR TRIM(location_id) = '')
            """)
            loc_updated = conn.execute("SELECT changes();").fetchone()[0]

            conn.execute("COMMIT;")
            print(f"Updated assets.category_id: {cat_updated}")
            print(f"Updated assets.location_id: {loc_updated}")

        except Exception:
            conn.execute("ROLLBACK;")
            raise

        # 事後チェック
        remaining_cat = conn.execute("""
            SELECT COUNT(*) AS n
            FROM assets
            WHERE category IS NOT NULL AND TRIM(category) <> ''
              AND (category_id IS NULL OR TRIM(category_id) = '')
        """).fetchone()["n"]

        remaining_loc = conn.execute("""
            SELECT COUNT(*) AS n
            FROM assets
            WHERE location IS NOT NULL AND TRIM(location) <> ''
              AND (location_id IS NULL OR TRIM(location_id) = '')
        """).fetchone()["n"]

        print(f"Remaining missing category_id: {remaining_cat}")
        print(f"Remaining missing location_id: {remaining_loc}")
        print("OK")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
