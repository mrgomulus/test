from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.database import get_conn, init_db, utcnow
from app.opcua_client import opcua_manager

logger = logging.getLogger(__name__)

app = FastAPI(title="OPC UA Browser")
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")


def _safe_err(exc: Exception) -> str:
    """Return a safe error message that does not expose internal paths or stack traces."""
    return type(exc).__name__


# ── Startup ───────────────────────────────────────────────────────────────────


@app.on_event("startup")
def startup() -> None:
    init_db()


# ── Frontend shell ────────────────────────────────────────────────────────────


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


# ── Connections CRUD ──────────────────────────────────────────────────────────


@app.get("/api/connections")
def list_connections():
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT id, name, url, username, security_mode, security_policy, "
            "timeout, description, created_at, updated_at FROM connections ORDER BY name"
        ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["connected"] = opcua_manager.is_connected(d["id"])
            result.append(d)
        return result
    finally:
        conn.close()


@app.post("/api/connections", status_code=201)
def create_connection(payload: dict):
    now = utcnow()
    conn = get_conn()
    try:
        cur = conn.execute(
            "INSERT INTO connections(name, url, username, password, security_mode, "
            "security_policy, timeout, description, created_at, updated_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                payload.get("name", "New Connection"),
                payload.get("url", "opc.tcp://localhost:4840"),
                payload.get("username", ""),
                payload.get("password", ""),
                payload.get("security_mode", 1),
                payload.get("security_policy", "None"),
                payload.get("timeout", 10),
                payload.get("description", ""),
                now,
                now,
            ),
        )
        conn.commit()
        row = conn.execute(
            "SELECT id, name, url, username, security_mode, security_policy, "
            "timeout, description, created_at, updated_at FROM connections WHERE id=?",
            (cur.lastrowid,),
        ).fetchone()
        d = dict(row)
        d["connected"] = False
        return JSONResponse(d, status_code=201)
    finally:
        conn.close()


@app.put("/api/connections/{conn_id}")
def update_connection(conn_id: int, payload: dict):
    now = utcnow()
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT id FROM connections WHERE id=?", (conn_id,)
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Connection not found")
        conn.execute(
            "UPDATE connections SET name=?, url=?, username=?, password=?, "
            "security_mode=?, security_policy=?, timeout=?, description=?, updated_at=? "
            "WHERE id=?",
            (
                payload.get("name"),
                payload.get("url"),
                payload.get("username", ""),
                payload.get("password", ""),
                payload.get("security_mode", 1),
                payload.get("security_policy", "None"),
                payload.get("timeout", 10),
                payload.get("description", ""),
                now,
                conn_id,
            ),
        )
        conn.commit()
        row = conn.execute(
            "SELECT id, name, url, username, security_mode, security_policy, "
            "timeout, description, created_at, updated_at FROM connections WHERE id=?",
            (conn_id,),
        ).fetchone()
        d = dict(row)
        d["connected"] = opcua_manager.is_connected(conn_id)
        return d
    finally:
        conn.close()


@app.delete("/api/connections/{conn_id}", status_code=204)
async def delete_connection(conn_id: int):
    await opcua_manager.disconnect(conn_id)
    conn = get_conn()
    try:
        conn.execute("DELETE FROM connections WHERE id=?", (conn_id,))
        conn.commit()
    finally:
        conn.close()


# ── OPC UA connect / disconnect ───────────────────────────────────────────────


@app.post("/api/connections/{conn_id}/connect")
async def connect_server(conn_id: int):
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT url, username, password, security_mode, security_policy, timeout "
            "FROM connections WHERE id=?",
            (conn_id,),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Connection not found")
        r = dict(row)
    finally:
        conn.close()

    try:
        info = await opcua_manager.connect(
            conn_id=conn_id,
            url=r["url"],
            username=r["username"] or "",
            password=r["password"] or "",
            timeout=r["timeout"] or 10,
            security_mode=r["security_mode"] or 1,
            security_policy=r["security_policy"] or "None",
        )
        return info
    except Exception as exc:
        logger.warning("OPC UA error: %s", exc)
        raise HTTPException(status_code=502, detail=_safe_err(exc))


@app.post("/api/connections/{conn_id}/disconnect")
async def disconnect_server(conn_id: int):
    await opcua_manager.disconnect(conn_id)
    return {"status": "disconnected"}


@app.get("/api/connections/{conn_id}/status")
async def connection_status(conn_id: int):
    if not opcua_manager.is_connected(conn_id):
        return {"connected": False}
    try:
        info = await opcua_manager.get_server_info(conn_id)
        info["connected"] = True
        return info
    except Exception as exc:
        logger.warning("Server info error: %s", exc)
        return {"connected": False, "error": _safe_err(exc)}


# ── Browse ────────────────────────────────────────────────────────────────────


@app.get("/api/browse")
async def browse(conn_id: int, node_id: Optional[str] = None):
    _require_connection(conn_id)
    try:
        return await opcua_manager.browse(conn_id, node_id)
    except Exception as exc:
        logger.warning("OPC UA error: %s", exc)
        raise HTTPException(status_code=502, detail=_safe_err(exc))


# ── Attributes ────────────────────────────────────────────────────────────────


@app.get("/api/attributes")
async def get_attributes(conn_id: int, node_id: str):
    _require_connection(conn_id)
    try:
        return await opcua_manager.get_attributes(conn_id, node_id)
    except Exception as exc:
        logger.warning("OPC UA error: %s", exc)
        raise HTTPException(status_code=502, detail=_safe_err(exc))


# ── Read ──────────────────────────────────────────────────────────────────────


@app.get("/api/read")
async def read_value(conn_id: int, node_id: str):
    _require_connection(conn_id)
    try:
        return await opcua_manager.read_value(conn_id, node_id)
    except Exception as exc:
        logger.warning("OPC UA error: %s", exc)
        raise HTTPException(status_code=502, detail=_safe_err(exc))


# ── Write ─────────────────────────────────────────────────────────────────────


@app.post("/api/write")
async def write_value(payload: dict):
    conn_id: int = payload.get("conn_id")
    node_id: str = payload.get("node_id", "")
    value: Any = payload.get("value")
    dt_hint: Optional[str] = payload.get("data_type")
    _require_connection(conn_id)
    try:
        return await opcua_manager.write_value(conn_id, node_id, value, dt_hint)
    except Exception as exc:
        logger.warning("OPC UA error: %s", exc)
        raise HTTPException(status_code=502, detail=_safe_err(exc))


# ── Method call ───────────────────────────────────────────────────────────────


@app.post("/api/call-method")
async def call_method(payload: dict):
    conn_id: int = payload.get("conn_id")
    parent_id: str = payload.get("parent_id", "")
    method_id: str = payload.get("method_id", "")
    args: list = payload.get("args", [])
    _require_connection(conn_id)
    try:
        result = await opcua_manager.call_method(conn_id, parent_id, method_id, args)
        return {"result": result}
    except Exception as exc:
        logger.warning("OPC UA error: %s", exc)
        raise HTTPException(status_code=502, detail=_safe_err(exc))


# ── Favorites ─────────────────────────────────────────────────────────────────


@app.get("/api/favorites")
def list_favorites(conn_id: int):
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM favorites WHERE conn_id=? ORDER BY display_name",
            (conn_id,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@app.post("/api/favorites", status_code=201)
def add_favorite(payload: dict):
    conn_id = payload.get("conn_id")
    node_id = payload.get("node_id", "")
    if not conn_id or not node_id:
        raise HTTPException(status_code=422, detail="conn_id and node_id required")
    now = utcnow()
    conn = get_conn()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO favorites(conn_id, node_id, display_name, "
            "browse_name, node_class, data_type, created_at) VALUES(?,?,?,?,?,?,?)",
            (
                conn_id,
                node_id,
                payload.get("display_name", ""),
                payload.get("browse_name", ""),
                payload.get("node_class", ""),
                payload.get("data_type", ""),
                now,
            ),
        )
        conn.commit()
        row = conn.execute(
            "SELECT * FROM favorites WHERE conn_id=? AND node_id=?",
            (conn_id, node_id),
        ).fetchone()
        return JSONResponse(dict(row), status_code=201)
    finally:
        conn.close()


@app.delete("/api/favorites/{fav_id}", status_code=204)
def delete_favorite(fav_id: int):
    conn = get_conn()
    try:
        conn.execute("DELETE FROM favorites WHERE id=?", (fav_id,))
        conn.commit()
    finally:
        conn.close()


# ── WebSocket subscriptions ───────────────────────────────────────────────────


@app.websocket("/ws/{conn_id}")
async def websocket_endpoint(websocket: WebSocket, conn_id: int):
    await websocket.accept()
    if not opcua_manager.is_connected(conn_id):
        await websocket.send_json({"error": "Not connected"})
        await websocket.close()
        return

    queue: asyncio.Queue = asyncio.Queue()

    async def sender():
        while True:
            msg = await queue.get()
            try:
                await websocket.send_json(msg)
            except Exception:
                return

    sender_task = asyncio.create_task(sender())
    try:
        while True:
            raw = await websocket.receive_text()
            msg = json.loads(raw)
            action = msg.get("action")
            node_id = msg.get("node_id", "")
            if action == "subscribe" and node_id:
                try:
                    await opcua_manager.subscribe(conn_id, node_id, queue)
                    await websocket.send_json(
                        {"type": "subscribed", "node_id": node_id}
                    )
                except Exception as exc:
                    await websocket.send_json(
                        {"type": "error", "node_id": node_id, "message": _safe_err(exc)}
                    )
            elif action == "unsubscribe" and node_id:
                await opcua_manager.unsubscribe(conn_id, node_id)
                await websocket.send_json(
                    {"type": "unsubscribed", "node_id": node_id}
                )
    except WebSocketDisconnect:
        pass
    finally:
        sender_task.cancel()


# ── Helpers ───────────────────────────────────────────────────────────────────


def _require_connection(conn_id: Any) -> None:
    if not conn_id or not opcua_manager.is_connected(int(conn_id)):
        raise HTTPException(
            status_code=400, detail="Not connected to this OPC UA server"
        )
