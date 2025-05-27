"""Utility helpers for node DB creation and CSV bootstrap.

Usage (inside Node.__init__):
    from db_utils import ensure_schema
    ensure_schema(db_path=f"node_{self.id_node}.db", data_dir="/path/where/csvs/live")

The function is *idempotent*: if the .db and tables already exist it
simply returns without touching data.
"""

from __future__ import annotations

import csv
import sqlite3
from pathlib import Path
from typing import Final
from datetime import datetime

SCHEMA_SQL: Final[str] = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS items (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT    NOT NULL,
    category    TEXT    NOT NULL,
    price       REAL    NOT NULL,
    stock   INTEGER NOT NULL,
    tax_rate    REAL    NOT NULL DEFAULT 0.16,
    last_update TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS branch_stock (
    branch_id   INTEGER,
    item_id     INTEGER,
    quantity    INTEGER,
    last_update TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(branch_id, item_id),
    FOREIGN KEY(item_id) REFERENCES items(id)
);

CREATE TABLE IF NOT EXISTS clients (
    id          INTEGER PRIMARY KEY,
    name        TEXT NOT NULL,
    phone       TEXT,
    email       TEXT,
    last_update TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

def ensure_schema(db_path: str | Path, *, seeds_dir: str | Path):
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.executescript(SCHEMA_SQL)

    def table_empty(name: str) -> bool:
        return conn.execute(f"SELECT 1 FROM {name} LIMIT 1").fetchone() is None

    # Importa CSV si la tabla está vacía, independientemente de que el .db exista
    if table_empty("items"):
        _import_csv(conn, seeds_dir / "oxxo_products.csv", "items")
    if table_empty("branch_stock"):
        _import_csv(conn, seeds_dir / "branch_stock.csv", "branch_stock")
    if table_empty("clients"):
        _import_csv(conn, seeds_dir / "clients.csv", "clients")

    conn.commit()
    conn.close()
    print(f"[db_utils] → {db_path} listo.")

def _import_csv(conn: sqlite3.Connection, csv_path: Path, table: str) -> None:
    """Insert rows from *csv_path* into *table* if the table is empty."""
    cur = conn.cursor()
    cur.execute(f"SELECT 1 FROM {table} LIMIT 1;")
    if cur.fetchone():
        print(f"  ↳ {table} already populated; skipping {csv_path.name}.")
        return

    if not csv_path.exists():
        print(f"  ↳ Seed file {csv_path} not found; continuing without import.")
        return

    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        placeholders = ",".join(["?"] * len(header))
        cols = ",".join(header)
        sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
        cur.executemany(sql, reader)
    print(f"  ↳ Inserted {conn.total_changes} rows into {table} from {csv_path.name}.")


if __name__ == "__main__":
    # Quick manual test: python db_utils.py node_0.db ./data
    import argparse, sys

    parser = argparse.ArgumentParser(description="Bootstrap node DB and seed data from CSVs.")
    parser.add_argument("db", help="Path to SQLite file (e.g., node_1.db)")
    parser.add_argument("--data-dir", default=".", help="Directory with CSV seeds")
    args = parser.parse_args()

    ensure_schema(args.db, data_dir=args.data_dir)
    print("Terminado.")
