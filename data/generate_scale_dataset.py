#!/usr/bin/env python3
"""Generate a large SQLite dataset (default 1,000,000 posts) for performance testing.

Rows are derived from seed CSV rows (cycled). Each row gets unique author/id suffix,
spread created_at dates, and optional minor content mutation so LIKE filters still match
seed keywords. Sentiment fields are filled synthetically (no NLP call) so dashboards
measure read/aggregation cost without a multi-hour backfill.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import sqlite3
import sys
from datetime import date, datetime, timedelta
from pathlib import Path


def column_exists(conn: sqlite3.Connection, table: str, name: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table})")
    for cid, cname, ctype, notnull, dflt, pk in cur.fetchall():
        if cname == name:
            return True
    return False


def ensure_migrated_posts(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            platform TEXT NOT NULL,
            author TEXT NOT NULL,
            content TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    migrations = [
        ("sentiment_label", "ALTER TABLE posts ADD COLUMN sentiment_label TEXT DEFAULT 'neutral'"),
        ("sentiment_score", "ALTER TABLE posts ADD COLUMN sentiment_score INTEGER DEFAULT 0"),
        ("nlp_version", "ALTER TABLE posts ADD COLUMN nlp_version INTEGER DEFAULT 0"),
    ]
    for name, ddl in migrations:
        if not column_exists(conn, "posts", name):
            conn.execute(ddl)


def load_seed_rows(csv_path: Path) -> list[tuple[str, str, str]]:
    rows: list[tuple[str, str, str]] = []
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        expected = {"name", "date", "content"}
        if set(reader.fieldnames or []) != expected:
            raise ValueError(f"Unexpected CSV headers: {reader.fieldnames}")
        for row in reader:
            name = (row.get("name") or "").strip()
            d = (row.get("date") or "").strip()
            content = (row.get("content") or "").strip()
            if name and d and content:
                rows.append((name, d, content))
    if not rows:
        raise RuntimeError("No rows loaded from seed CSV")
    return rows


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate large posts dataset in SQLite")
    p.add_argument("--db", default="./data/listenai_scale.db", help="Output SQLite path")
    p.add_argument("--csv", default="./data/posts.csv", help="Seed CSV (name,date,content)")
    p.add_argument("--target", type=int, default=1_000_000, help="Number of posts to insert")
    p.add_argument("--platform", default="x", help="platform column value")
    p.add_argument("--batch-size", type=int, default=5000, help="Rows per executemany batch")
    p.add_argument("--seed", type=int, default=42, help="RNG seed for sentiment")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    db_path = Path(args.db)
    if not db_path.is_absolute():
        db_path = (root / db_path).resolve()
    csv_path = Path(args.csv)
    if not csv_path.is_absolute():
        csv_path = (root / csv_path).resolve()

    rng = random.Random(args.seed)
    labels = ("positive", "neutral", "negative")

    try:
        seed_rows = load_seed_rows(csv_path)
    except Exception as exc:  # noqa: BLE001
        print(f"Error loading seed: {exc}", file=sys.stderr)
        return 1

    conn = sqlite3.connect(str(db_path))
    ensure_migrated_posts(conn)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    cur = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='post_tokens' LIMIT 1"
    )
    if cur.fetchone():
        conn.execute("DELETE FROM post_tokens")
    # Clear existing posts for a clean scale DB when re-running on same file
    conn.execute("DELETE FROM posts")
    conn.commit()

    start_calendar = date(2018, 1, 1)
    span_days = 365 * 8

    sql = """
        INSERT INTO posts(platform, author, content, created_at, sentiment_label, sentiment_score, nlp_version)
        VALUES(?, ?, ?, ?, ?, ?, ?)
    """

    inserted = 0
    batch: list[tuple] = []
    batch_size = max(100, int(args.batch_size))

    for i in range(int(args.target)):
        name, _date_str, content = seed_rows[i % len(seed_rows)]
        day_offset = rng.randint(0, span_days)
        d = start_calendar + timedelta(days=day_offset)
        created_at = datetime(d.year, d.month, d.day, rng.randint(0, 23), rng.randint(0, 59), 0).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        author = f"{name}_s{i}"
        label = rng.choice(labels)
        score = rng.randint(-3, 3)
        # Keep seed text; tiny suffix preserves keyword hits for dashboard defaults.
        c = f"{content} #{i}"
        batch.append((args.platform, author, c, created_at, label, score, 1))
        if len(batch) >= batch_size:
            conn.executemany(sql, batch)
            conn.commit()
            inserted += len(batch)
            batch.clear()
            if inserted % 100_000 == 0:
                print(json.dumps({"inserted": inserted, "target": args.target}))

    if batch:
        conn.executemany(sql, batch)
        conn.commit()
        inserted += len(batch)

    total = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
    conn.close()
    print(json.dumps({"db": str(db_path), "inserted": inserted, "total_posts": total}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
