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

SCHEMA_SQL: Final[str] = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS messages (
    id          INTEGER   PRIMARY KEY AUTOINCREMENT,
    origin      INTEGER   NOT NULL,
    destination INTEGER   NOT NULL,
    content     TEXT      NOT NULL,
    timestamp   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS items (
    id              INTEGER   PRIMARY KEY AUTOINCREMENT,
    name            TEXT      NOT NULL,
    category        TEXT      NOT NULL,
    price           REAL      NOT NULL,
    stock           INTEGER   NOT NULL,
    tax_rate        REAL      NOT NULL DEFAULT 0.16,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS branch_stock (
    branch_id       INTEGER   NOT NULL,
    item_id         INTEGER   NOT NULL,
    quantity        INTEGER   NOT NULL,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(branch_id, item_id),
    FOREIGN KEY(item_id) REFERENCES items(id)
);

CREATE TABLE IF NOT EXISTS clients (
    id              INTEGER   PRIMARY KEY AUTOINCREMENT,
    name            TEXT      NOT NULL,
    phone           TEXT,
    email           TEXT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS purchases (
    purchase_id     INTEGER   PRIMARY KEY AUTOINCREMENT,
    item_id         INTEGER   NOT NULL,
    quantity        INTEGER   NOT NULL,
    branch_id       INTEGER   NOT NULL,
    client_id       TEXT      NOT NULL,
    delivery_guide  TEXT      NOT NULL,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(item_id) REFERENCES items(id),
    FOREIGN KEY(client_id) REFERENCES clients(id)
);
"""

def ensure_schema(db_path: str | Path):
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.executescript(SCHEMA_SQL)
    conn.close()

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

import utils
def migrate_all(node):
    # Implementación de la distribución de clientes, items, etc.
    if not node.master:
        return

    conn = sqlite3.connect(node.db_path, check_same_thread=False)

    def table_empty(name: str) -> bool:
        return conn.execute(f"SELECT 1 FROM {name} LIMIT 1").fetchone() is None

    # Importa CSV si la tabla está vacía, independientemente de que el .db exista
    if table_empty("clients"):
        print(f"[Node {node.id_node}] Ingresando clientes...")
        _import_csv(conn, node.db_seeds_dir / "clients.csv", "clients")
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT id, name FROM clients")
            rows = cursor.fetchall()

            num_neighbours = len(node.neighbours.keys())
            num_clients = len(rows)
            print('Clients: ', num_clients)
            print('Vecinos: ', num_neighbours)
            print('Rows: ', rows)
            
            print(f"\n[Node {node.id_node}] Enviando datos a Nodo {999}...")
            clients_divided = utils.divide_list(rows, num_neighbours)
            print(clients_divided)
        except Exception as e:
            print(f"\n[Node {node.id_node}] Error durante migracion de clientes: {e}")

    if table_empty("branch_stock"):
        _import_csv(conn, node.db_seeds_dir / "branch_stock.csv", "branch_stock")
    if table_empty("items"):
        _import_csv(conn, node.db_seeds_dir / "oxxo_products.csv", "items")

    conn.commit()
    conn.close()
    node.sync_data_ready_event.set()
if __name__ == "__main__":
    # Quick manual test: python db_utils.py node_0.db ./data
    import argparse, sys

    parser = argparse.ArgumentParser(description="Bootstrap node DB and seed data from CSVs.")
    parser.add_argument("db", help="Path to SQLite file (e.g., node_1.db)")
    parser.add_argument("--data-dir", default=".", help="Directory with CSV seeds")
    args = parser.parse_args()

    ensure_schema(args.db, data_dir=args.data_dir)
    print("Terminado.")
