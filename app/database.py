from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

DB_PATH = Path(__file__).resolve().parent.parent / "data.sqlite3"

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS connections (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  url TEXT NOT NULL,
  username TEXT NOT NULL DEFAULT '',
  password TEXT NOT NULL DEFAULT '',
  security_mode INTEGER NOT NULL DEFAULT 1,
  security_policy TEXT NOT NULL DEFAULT 'None',
  timeout INTEGER NOT NULL DEFAULT 10,
  description TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS favorites (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  conn_id INTEGER NOT NULL,
  node_id TEXT NOT NULL,
  display_name TEXT NOT NULL DEFAULT '',
  browse_name TEXT NOT NULL DEFAULT '',
  node_class TEXT NOT NULL DEFAULT '',
  data_type TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  UNIQUE(conn_id, node_id),
  FOREIGN KEY (conn_id) REFERENCES connections(id) ON DELETE CASCADE
);
"""


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    conn = get_conn()
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()


def utcnow() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")
