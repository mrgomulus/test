from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

DB_PATH = Path(__file__).resolve().parent.parent / "data.sqlite3"

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS lines (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE,
  active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS subplants (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE,
  active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS line_subplant (
  line_id INTEGER NOT NULL,
  subplant_id INTEGER NOT NULL,
  PRIMARY KEY (line_id, subplant_id),
  FOREIGN KEY (line_id) REFERENCES lines (id) ON DELETE CASCADE,
  FOREIGN KEY (subplant_id) REFERENCES subplants (id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  username TEXT NOT NULL UNIQUE,
  password_hash TEXT,
  role TEXT NOT NULL DEFAULT 'user',
  auth_type TEXT NOT NULL DEFAULT 'local',
  must_change_password INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS disturbances (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  stoerung_text TEXT,
  anlage TEXT,
  teilanlage_id INTEGER,
  ursache TEXT,
  behebung TEXT,
  dauer_minuten INTEGER,
  kategorie TEXT,
  bereitschaft TEXT,
  berechtigter_einsatz TEXT,
  verantwortlicher TEXT,
  event_time TEXT,
  created_at TEXT NOT NULL,
  FOREIGN KEY (teilanlage_id) REFERENCES subplants (id)
);

CREATE VIRTUAL TABLE IF NOT EXISTS disturbances_fts USING fts5(
  stoerung_text,
  ursache,
  behebung,
  content='disturbances',
  content_rowid='id'
);

CREATE TRIGGER IF NOT EXISTS disturbances_ai AFTER INSERT ON disturbances BEGIN
  INSERT INTO disturbances_fts(rowid, stoerung_text, ursache, behebung)
  VALUES (new.id, new.stoerung_text, new.ursache, new.behebung);
END;

CREATE TRIGGER IF NOT EXISTS disturbances_ad AFTER DELETE ON disturbances BEGIN
  INSERT INTO disturbances_fts(disturbances_fts, rowid, stoerung_text, ursache, behebung)
  VALUES('delete', old.id, old.stoerung_text, old.ursache, old.behebung);
END;

CREATE TRIGGER IF NOT EXISTS disturbances_au AFTER UPDATE ON disturbances BEGIN
  INSERT INTO disturbances_fts(disturbances_fts, rowid, stoerung_text, ursache, behebung)
  VALUES('delete', old.id, old.stoerung_text, old.ursache, old.behebung);
  INSERT INTO disturbances_fts(rowid, stoerung_text, ursache, behebung)
  VALUES (new.id, new.stoerung_text, new.ursache, new.behebung);
END;

CREATE TABLE IF NOT EXISTS predictions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  type TEXT NOT NULL,
  prediction_json TEXT NOT NULL,
  confidence REAL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_disturbances_event_time ON disturbances(event_time);
CREATE INDEX IF NOT EXISTS idx_disturbances_kategorie ON disturbances(kategorie);
CREATE INDEX IF NOT EXISTS idx_disturbances_verantwortlicher ON disturbances(verantwortlicher);
"""


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = get_conn()
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()


def set_setting(key: str, value: Any) -> None:
    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, json.dumps(value)),
        )
        conn.commit()
    finally:
        conn.close()


def get_setting(key: str, default: Any = None) -> Any:
    conn = get_conn()
    try:
        row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        return default if not row else json.loads(row["value"])
    finally:
        conn.close()


def utcnow() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")
