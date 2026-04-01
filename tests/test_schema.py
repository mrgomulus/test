"""Basic tests for the OPC UA Browser database schema and API."""
from __future__ import annotations

import os
import sqlite3
import tempfile
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Patch DB path for testing
import app.database as db_module

_tmp = tempfile.NamedTemporaryFile(suffix='.sqlite3', delete=False)
_tmp.close()
db_module.DB_PATH = db_module.Path(_tmp.name)


def setup_module():
    db_module.init_db()


def teardown_module():
    os.unlink(_tmp.name)


def test_connections_crud():
    conn = db_module.get_conn()
    now = db_module.utcnow()
    cur = conn.execute(
        "INSERT INTO connections(name, url, created_at, updated_at) VALUES(?,?,?,?)",
        ("Test", "opc.tcp://localhost:4840", now, now),
    )
    conn.commit()
    row_id = cur.lastrowid
    row = conn.execute("SELECT name FROM connections WHERE id=?", (row_id,)).fetchone()
    assert row["name"] == "Test"
    conn.execute("UPDATE connections SET name=? WHERE id=?", ("Updated", row_id))
    conn.commit()
    row = conn.execute("SELECT name FROM connections WHERE id=?", (row_id,)).fetchone()
    assert row["name"] == "Updated"
    conn.close()


def test_favorites_crud():
    conn = db_module.get_conn()
    now = db_module.utcnow()
    cur = conn.execute(
        "INSERT INTO connections(name, url, created_at, updated_at) VALUES(?,?,?,?)",
        ("FavTest", "opc.tcp://server:4840", now, now),
    )
    conn.commit()
    conn_id = cur.lastrowid
    conn.execute(
        "INSERT INTO favorites(conn_id, node_id, display_name, created_at) VALUES(?,?,?,?)",
        (conn_id, "ns=2;i=1001", "MyVariable", now),
    )
    conn.commit()
    fav = conn.execute("SELECT * FROM favorites WHERE conn_id=?", (conn_id,)).fetchone()
    assert fav["node_id"] == "ns=2;i=1001"
    assert fav["display_name"] == "MyVariable"
    conn.execute("DELETE FROM connections WHERE id=?", (conn_id,))
    conn.commit()
    # Cascade delete should remove favorites
    fav_after = conn.execute("SELECT * FROM favorites WHERE conn_id=?", (conn_id,)).fetchone()
    assert fav_after is None
    conn.close()


def test_utcnow_format():
    ts = db_module.utcnow()
    assert "T" in ts
    assert len(ts) >= 19
